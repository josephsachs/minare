package com.minare.core.frames.worker

import com.google.inject.Inject
import com.google.inject.Singleton
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.Assert
import com.minare.core.entity.annotations.FunctionCall
import com.minare.core.entity.annotations.Trigger
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.models.Entity
import com.minare.core.operation.models.OperationSet.FailurePolicy
import com.minare.core.storage.interfaces.StateStore
import com.minare.worker.coordinator.models.OperationCompletion
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import org.slf4j.LoggerFactory
import java.lang.reflect.Method
import kotlin.reflect.full.callSuspend
import kotlin.reflect.jvm.kotlinFunction

/**
 * Executes an OperationSet's members sequentially within the worker's frame thread.
 *
 * Ephemeral per-execution: the singleton holds injected services, but all execution
 * state (pipe, trace, snapshot) lives as locals inside execute(). The set occupies
 * the worker thread until it yields — no timeout, no interleaving.
 *
 * Member dispatch:
 * - MUTATE/CREATE/DELETE → event bus to existing operation handlers (same delta path)
 * - FUNCTION_CALL/ASSERT → inline hydration + reflection invocation (needs pipe context)
 * - TRIGGER → event bus publish, fire-and-forget (runs in Task-like context)
 */
@Singleton
class OperationSetExecutor @Inject constructor(
    private val vertx: Vertx,
    private val stateStore: StateStore,
    private val entityFactory: EntityFactory,
    private val reflectionCache: ReflectionCache,
    private val hazelcastInstance: HazelcastInstance
) {
    private val log = LoggerFactory.getLogger(OperationSetExecutor::class.java)

    companion object {
        const val ADDRESS_TRIGGER = "worker.trigger"
        const val ADDRESS_SET_EVENT = "minare.operation-set.event"
    }

    /**
     * Run every member of the set in setIndex order. Returns the count of
     * completed steps. Handles FailurePolicy on step failure.
     */
    suspend fun execute(members: List<JsonObject>, frameNumber: Long, workerId: String): Int {
        if (members.isEmpty()) return 0

        val sorted = members.sortedBy { it.getInteger("setIndex", 0) }
        val setId = sorted.first().getString("operationSetId")
        val policy = parsePolicy(sorted.first())
        val completionMap: IMap<String, OperationCompletion> =
            hazelcastInstance.getMap("operation-completions")

        // Pre-read entity states for rollback (one batch call)
        val entityIds = sorted.mapNotNull { it.getString("entityId") }.distinct()
        val snapshot = if (policy == FailurePolicy.ROLLBACK && entityIds.isNotEmpty()) {
            stateStore.findJson(entityIds)
        } else emptyMap()

        val trace = ExecutionTrace(setId)
        var pipe: Any? = null
        val completed = mutableListOf<JsonObject>()

        for (member in sorted) {
            val action = member.getString("action") ?: continue
            val stepId = member.getString("id") ?: continue

            try {
                when (action) {
                    "MUTATE", "CREATE", "DELETE" -> {
                        val result = dispatchOperation(member, frameNumber)
                        if (result.getBoolean("success", false)) {
                            completed.add(member)
                            record(completionMap, stepId, workerId, frameNumber)
                            trace.step(stepId, action, true)
                            // Track created entityId for rollback
                            result.getString("entityId")?.let {
                                member.put("_createdEntityId", it)
                            }
                        } else {
                            trace.step(stepId, action, false, result.getString("error"))
                            return onFailure(
                                policy, trace, completed, snapshot,
                                frameNumber, workerId, completionMap
                            )
                        }
                    }

                    "FUNCTION_CALL" -> {
                        val args = pipe ?: member.getJsonObject("values")
                        val result = callFunction(member, args)
                        pipe = result
                        completed.add(member)
                        record(completionMap, stepId, workerId, frameNumber)
                        trace.step(stepId, action, true)
                    }

                    "ASSERT" -> {
                        val args = pipe ?: member.getJsonObject("values")
                        val pass = callAssert(member, args)
                        if (!pass) {
                            trace.step(stepId, action, false, "Assertion failed")
                            return onFailure(
                                policy, trace, completed, snapshot,
                                frameNumber, workerId, completionMap
                            )
                        }
                        completed.add(member)
                        record(completionMap, stepId, workerId, frameNumber)
                        trace.step(stepId, action, true)
                    }

                    "TRIGGER" -> {
                        fireTrigger(member, pipe)
                        trace.step(stepId, action, true)
                        // Fire-and-forget — not tracked in completed
                    }

                    else -> log.warn("Unknown action in OperationSet: {}", action)
                }
            } catch (e: Exception) {
                log.error("OperationSet step {} failed", stepId, e)
                trace.step(stepId, action, false, e.message)
                return onFailure(
                    policy, trace, completed, snapshot,
                    frameNumber, workerId, completionMap
                )
            }
        }

        publishCompletion(setId, completed.size)
        return completed.size
    }

    // ── Operation dispatch ──────────────────────────────────────────────────

    /**
     * Route a MUTATE/CREATE/DELETE member through the normal handler path.
     * Same event bus addresses as solo operations — hooks, delta storage, and
     * tandem-maintenance all occur as expected.
     */
    private suspend fun dispatchOperation(member: JsonObject, frameNumber: Long): JsonObject {
        val action = member.getString("action")
        val context = JsonObject()
            .put("operation", member)
            .put("frameNumber", frameNumber)

        return try {
            vertx.eventBus()
                .request<JsonObject>("worker.process.$action", context)
                .await()
                .body()
        } catch (e: Exception) {
            log.error("Handler dispatch failed for {}", action, e)
            JsonObject().put("success", false).put("error", e.message)
        }
    }

    // ── Predicate invocation ────────────────────────────────────────────────

    /**
     * Hydrate the target entity, find the @FunctionCall method by name,
     * invoke with the current args, return the result for piping.
     */
    private suspend fun callFunction(member: JsonObject, args: Any?): Any? {
        val entity = hydrate(member)
        val target = member.getString("target")
        val method = findAnnotated(entity, target, FunctionCall::class.java)
            ?: throw IllegalStateException(
                "No @FunctionCall method '$target' on ${member.getString("entityType")}"
            )

        return invokeMethod(entity, method, args)
    }

    /**
     * Hydrate the target entity, find the @Assert method by name,
     * invoke and expect a Boolean return.
     */
    private suspend fun callAssert(member: JsonObject, args: Any?): Boolean {
        val entity = hydrate(member)
        val target = member.getString("target")
        val method = findAnnotated(entity, target, Assert::class.java)
            ?: throw IllegalStateException(
                "No @Assert method '$target' on ${member.getString("entityType")}"
            )

        val result = invokeMethod(entity, method, args)
        return result as? Boolean
            ?: throw IllegalStateException("@Assert method '$target' must return Boolean, got: $result")
    }

    /**
     * Publish to the trigger address — a registered consumer hydrates and invokes
     * asynchronously. The executor does not wait.
     */
    private fun fireTrigger(member: JsonObject, args: Any?) {
        val payload = member.copy()
        if (args is JsonObject) {
            payload.put("_pipe", args)
        }
        vertx.eventBus().send(ADDRESS_TRIGGER, payload)
    }

    // ── Entity hydration ────────────────────────────────────────────────────

    /**
     * Hydrate an entity synchronously — unlike EntityObjectHydrator, this awaits
     * setEntityState/setEntityProperties so the entity is fully populated before
     * we invoke methods on it.
     */
    private suspend fun hydrate(member: JsonObject): Entity {
        val entityId = member.getString("entityId")
            ?: throw IllegalStateException("entityId required for ${member.getString("action")}")
        val entityType = member.getString("entityType")
            ?: throw IllegalStateException("entityType required")

        val entityJson = stateStore.findOneJson(entityId)
            ?: throw IllegalStateException("Entity $entityId not found")

        val entity = entityFactory.getNew(entityType).apply {
            _id = entityId
            version = entityJson.getLong("version")
            type = entityType
        }

        stateStore.setEntityState(entity, entityType, entityJson.getJsonObject("state", JsonObject()))
        stateStore.setEntityProperties(entity, entityType, entityJson.getJsonObject("properties", JsonObject()))

        return entity
    }

    // ── Reflection ──────────────────────────────────────────────────────────

    /**
     * Find a method on the entity class that matches the target name and
     * carries the expected annotation. Logs and returns null on miss.
     */
    private fun findAnnotated(
        entity: Entity,
        target: String,
        annotation: Class<out Annotation>
    ): Method? {
        val methods = reflectionCache.getFunctions(entity.javaClass)
        return methods.firstOrNull { method ->
            method.name == target && method.isAnnotationPresent(annotation)
        }.also { method ->
            if (method == null) {
                log.warn(
                    "No method named '{}' with @{} on {}",
                    target, annotation.simpleName, entity.javaClass.simpleName
                )
            }
        }
    }

    /**
     * Invoke a method on a hydrated entity, matching args to its parameters.
     *
     * Arg matching:
     * - 0 params → invoke bare
     * - 1 param, args is JsonObject → extract by param name, or pass the JsonObject
     * - 1 param, args is non-null → pass directly
     * - N params, args is JsonObject → kwargs: extract each param by name
     * - Supports suspend functions via callSuspend
     */
    private suspend fun invokeMethod(entity: Entity, method: Method, args: Any?): Any? {
        method.isAccessible = true
        val kFunc = method.kotlinFunction
            ?: throw IllegalStateException("Cannot get KFunction for ${method.name}")

        val params = kFunc.parameters.drop(1) // skip receiver

        val callArgs: Array<Any?> = when {
            params.isEmpty() -> emptyArray()

            params.size == 1 && args is JsonObject -> {
                // Single param — try extracting by name, fall back to passing the whole object
                val paramName = params[0].name
                val extracted = if (paramName != null) args.getValue(paramName) else null
                if (extracted != null) arrayOf(extracted) else arrayOf(args)
            }

            params.size == 1 && args != null -> arrayOf(args)

            args is JsonObject -> {
                // kwargs: match each parameter by name from the JsonObject
                params.map { param ->
                    param.name?.let { args.getValue(it) }
                }.toTypedArray()
            }

            args == null -> Array(params.size) { null }

            else -> {
                log.warn(
                    "Cannot match args type {} to {} params on {}, passing as first arg",
                    args.javaClass.simpleName, params.size, method.name
                )
                Array(params.size) { i -> if (i == 0) args else null }
            }
        }

        return if (kFunc.isSuspend) {
            kFunc.callSuspend(entity, *callArgs)
        } else {
            kFunc.call(entity, *callArgs)
        }
    }

    // ── Failure handling ────────────────────────────────────────────────────

    private suspend fun onFailure(
        policy: FailurePolicy,
        trace: ExecutionTrace,
        completed: List<JsonObject>,
        snapshot: Map<String, JsonObject>,
        frameNumber: Long,
        workerId: String,
        completionMap: IMap<String, OperationCompletion>
    ): Int {
        when (policy) {
            FailurePolicy.CONTINUE -> {
                // Proceed — caller loop handles this by not returning early.
                // We only reach here from an explicit failure; log and continue is
                // handled by returning completed.size and letting the loop proceed.
                // In practice, onFailure with CONTINUE shouldn't short-circuit.
            }

            FailurePolicy.ABORT -> {
                log.warn("OperationSet {} aborted at step — {} completed",
                    trace.setId, completed.size)
                publishDiagnostic(trace)
            }

            FailurePolicy.ROLLBACK -> {
                log.warn("OperationSet {} rolling back {} completed steps",
                    trace.setId, completed.size)
                rollback(completed, snapshot, frameNumber, workerId, completionMap, trace)
                publishDiagnostic(trace)
            }
        }

        return completed.size
    }

    /**
     * Reverse completed operations through the normal handler path.
     * Iterates in reverse order to undo the most recent changes first.
     * Each reverse operation creates an auditable delta trail.
     */
    private suspend fun rollback(
        completed: List<JsonObject>,
        snapshot: Map<String, JsonObject>,
        frameNumber: Long,
        workerId: String,
        completionMap: IMap<String, OperationCompletion>,
        trace: ExecutionTrace
    ) {
        for (member in completed.reversed()) {
            val action = member.getString("action") ?: continue
            val entityId = member.getString("entityId")

            try {
                val reverseOp = when (action) {
                    "MUTATE" -> {
                        // Restore the before-state values for the fields this mutation touched
                        val beforeState = snapshot[entityId]
                            ?.getJsonObject("state", JsonObject()) ?: JsonObject()
                        val originalDelta = member.getJsonObject("delta") ?: continue
                        val reverseDelta = JsonObject()
                        originalDelta.fieldNames().forEach { field ->
                            reverseDelta.put(field, beforeState.getValue(field))
                        }
                        member.copy()
                            .put("id", java.util.UUID.randomUUID().toString())
                            .put("delta", reverseDelta)
                            .put("_rollback", true)
                    }

                    "CREATE" -> {
                        // Delete the entity that was created
                        val createdId = member.getString("_createdEntityId") ?: continue
                        JsonObject()
                            .put("id", java.util.UUID.randomUUID().toString())
                            .put("entityId", createdId)
                            .put("entityType", member.getString("entityType"))
                            .put("action", "DELETE")
                            .put("timestamp", System.currentTimeMillis())
                            .put("_rollback", true)
                    }

                    "DELETE" -> {
                        // Re-create from snapshot — limitation: new entity gets a new ID
                        val beforeEntity = snapshot[entityId] ?: continue
                        val beforeState = beforeEntity.getJsonObject("state", JsonObject())
                        log.warn(
                            "DELETE rollback for {} will create a new entity (original ID lost)",
                            entityId
                        )
                        JsonObject()
                            .put("id", java.util.UUID.randomUUID().toString())
                            .put("entityType", member.getString("entityType"))
                            .put("action", "CREATE")
                            .put("delta", beforeState)
                            .put("timestamp", System.currentTimeMillis())
                            .put("_rollback", true)
                    }

                    else -> continue
                }

                val result = dispatchOperation(reverseOp, frameNumber)
                if (result.getBoolean("success", false)) {
                    trace.step(reverseOp.getString("id"), "ROLLBACK_$action", true)
                } else {
                    log.error("Rollback step failed for {}: {}",
                        action, result.getString("error"))
                    trace.step(reverseOp.getString("id"), "ROLLBACK_$action", false,
                        result.getString("error"))
                }
            } catch (e: Exception) {
                log.error("Rollback failed for {} on entity {}", action, entityId, e)
                trace.step(entityId ?: "unknown", "ROLLBACK_$action", false, e.message)
            }
        }
    }

    // ── Completion tracking ─────────────────────────────────────────────────

    private fun record(
        map: IMap<String, OperationCompletion>,
        stepId: String,
        workerId: String,
        frameNumber: Long
    ) {
        val key = "frame-$frameNumber:op-$stepId"
        map[key] = OperationCompletion(operationId = stepId, workerId = workerId)
    }

    private fun publishCompletion(setId: String, stepCount: Int) {
        vertx.eventBus().publish(
            ADDRESS_SET_EVENT,
            JsonObject()
                .put("type", "SET_COMPLETED")
                .put("operationSetId", setId)
                .put("stepCount", stepCount)
        )
    }

    private fun publishDiagnostic(trace: ExecutionTrace) {
        vertx.eventBus().publish(ADDRESS_SET_EVENT, trace.toJson())
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    private fun parsePolicy(member: JsonObject): FailurePolicy {
        return try {
            FailurePolicy.valueOf(member.getString("failurePolicy", "CONTINUE"))
        } catch (_: IllegalArgumentException) {
            FailurePolicy.CONTINUE
        }
    }

    // ── Execution trace ─────────────────────────────────────────────────────

    /**
     * Collects per-step records during set execution.
     * Emitted on failure for diagnostics; on success the data is discarded.
     */
    class ExecutionTrace(val setId: String) {
        private val steps = mutableListOf<StepRecord>()

        fun step(id: String, action: String, success: Boolean, error: String? = null) {
            steps.add(StepRecord(id, action, success, error, System.currentTimeMillis()))
        }

        fun toJson(): JsonObject {
            val stepsArray = JsonArray()
            steps.forEach { s ->
                stepsArray.add(
                    JsonObject()
                        .put("id", s.id)
                        .put("action", s.action)
                        .put("success", s.success)
                        .put("error", s.error)
                        .put("timestamp", s.timestamp)
                )
            }
            return JsonObject()
                .put("type", "SET_DIAGNOSTIC")
                .put("operationSetId", setId)
                .put("steps", stepsArray)
                .put("success", steps.all { it.success })
        }

        private data class StepRecord(
            val id: String,
            val action: String,
            val success: Boolean,
            val error: String?,
            val timestamp: Long
        )
    }
}
