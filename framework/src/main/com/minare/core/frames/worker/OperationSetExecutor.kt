package com.minare.core.frames.worker

import com.hazelcast.map.IMap
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.Assert as AssertAnnotation
import com.minare.core.entity.annotations.FunctionCall as FunctionCallAnnotation
import com.minare.core.entity.annotations.Trigger as TriggerAnnotation
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.models.Entity
import com.minare.core.operation.models.FailurePolicy
import com.minare.core.storage.interfaces.StateStore
import com.minare.worker.coordinator.models.OperationCompletion
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import kotlin.reflect.full.callSuspend
import kotlin.reflect.jvm.kotlinFunction

/**
 * Ephemeral, single-pass executor for an OperationSet.
 *
 * Processes set members sequentially by setIndex within the worker's coroutine context,
 * occupying the operation handling thread until the set yields or exhausts.
 *
 * Member types:
 *   MUTATE / CREATE / DELETE — dispatched to the worker's standard handlers via
 *   the local event bus, preserving hook and delta-storage behaviour.
 *
 *   FUNCTION_CALL — hydrates the target entity and invokes a method annotated
 *   with @FunctionCall. The return value becomes the step context for subsequent members.
 *
 *   ASSERT — invokes a method annotated with @Assert. Expects Boolean return.
 *   Failure applies the set's FailurePolicy.
 *
 *   TRIGGER — fire-and-forget invocation of a method annotated with @Trigger.
 *   Does not suspend or update step context.
 *
 * Entity instances are hydrated once per entityId and reused across all steps
 * targeting that entity, providing a stateful illusion for the duration of the set.
 * After a successful MUTATE, the delta is applied to the in-memory instance so
 * that subsequent predicate steps see the updated state.
 *
 * FailurePolicy:
 *   CONTINUE — proceed regardless of Assert failure.
 *   ABORT    — halt. Completed mutation deltas stand.
 *   ROLLBACK — halt and reverse completed mutations through the normal handler path,
 *              producing an auditable delta trail.
 */
class OperationSetExecutor(
    private val vertx: Vertx,
    private val stateStore: StateStore,
    private val entityFactory: EntityFactory,
    private val reflectionCache: ReflectionCache,
    private val instanceId: String,
    private val completionMap: IMap<String, OperationCompletion>,
    private val workerScope: CoroutineScope
) {
    private val log = LoggerFactory.getLogger(OperationSetExecutor::class.java)

    // Mutations that completed successfully, in execution order.
    private val completedMutations = mutableListOf<JsonObject>()

    // Entity state captured before any step runs, keyed by entityId.
    // Used to construct rollback operations.
    private val beforeStates = mutableMapOf<String, JsonObject>()

    // Hydrated entity instances, keyed by entityId. Reused across steps so that
    // predicate methods see a consistent, evolving view of entity state.
    private val hydratedEntities = mutableMapOf<String, Entity>()

    // Return value of the most recently completed FunctionCall or Assert.
    private var stepContext: Any? = null

    companion object {
        const val ADDRESS_TRIGGER = "minare.operation.set.executor.trigger"
    }

    /**
     * Execute all members in setIndex order.
     * @return count of successfully processed members (mutations + predicate calls)
     */
    suspend fun execute(members: List<JsonObject>, logicalFrame: Long): Int {
        val failurePolicy = resolvePolicy(members.first())

        snapshotBeforeStates(members)

        var successCount = 0

        for (member in members.sortedBy { it.getInteger("setIndex", 0) }) {
            val succeeded = when (member.getString("action")) {
                "MUTATE", "CREATE", "DELETE" -> executeMutation(member, logicalFrame)
                "FUNCTION_CALL"              -> executeFunctionCall(member)
                "ASSERT"                     -> executeAssert(member, failurePolicy)
                "TRIGGER"                    -> { fireTrigger(member); true }
                else -> {
                    log.warn("Unknown action in OperationSet member: {}", member.getString("action"))
                    false
                }
            }

            if (succeeded) {
                successCount++
            } else {
                when (failurePolicy) {
                    FailurePolicy.ABORT    -> return successCount
                    FailurePolicy.ROLLBACK -> { rollback(logicalFrame); return successCount }
                    FailurePolicy.CONTINUE -> Unit // unreachable
                }
            }
        }

        return successCount
    }

    // ── Mutation ─────────────────────────────────────────────────────────────

    private suspend fun executeMutation(member: JsonObject, logicalFrame: Long): Boolean {
        val operationId = member.getString("id") ?: return logUnknownId(member)
        val address = "worker.process.${member.getString("action")}.$instanceId"

        val context = JsonObject()
            .put("operation", member)
            .put("frameNumber", logicalFrame)

        return try {
            val result = vertx.eventBus()
                .request<JsonObject>(address, context)
                .await()
                .body()

            if (result.getBoolean("success", false)) {
                // For CREATE operations, the handler assigns the entityId.
                // Patch it into the member so rollback can reverse it.
                val assignedEntityId = result.getString("entityId")
                if (assignedEntityId != null && member.getString("entityId") == null) {
                    member.put("entityId", assignedEntityId)
                }

                completedMutations.add(member)
                completionMap["frame-$logicalFrame:op-$operationId"] = OperationCompletion(
                    operationId = operationId,
                    workerId = instanceId
                )

                // Sync the delta into the cached entity instance so subsequent
                // predicate steps see updated state without a store re-read.
                val entityId = member.getString("entityId")
                val entityType = member.getString("entityType")
                val delta = member.getJsonObject("delta")
                if (entityId != null && entityType != null && delta != null) {
                    val entity = hydratedEntities[entityId]
                        ?: hydrateEntity(entityId, entityType).also { hydratedEntities[entityId] = it }
                    applyDeltaToInstance(entity, delta)
                }

                true
            } else {
                log.error("Mutation failed in OperationSet member {}: {}", operationId, result.getString("error"))
                false
            }
        } catch (e: Exception) {
            log.error("Exception processing OperationSet mutation {}", operationId, e)
            false
        }
    }

    // ── FunctionCall ─────────────────────────────────────────────────────────

    private suspend fun executeFunctionCall(member: JsonObject): Boolean {
        val entityId   = member.getString("entityId")   ?: return logMissingField(member, "entityId")
        val entityType = member.getString("entityType") ?: return logMissingField(member, "entityType")
        val function   = member.getString("function")   ?: return logMissingField(member, "function")

        return try {
            val entity = resolveEntity(entityId, entityType)
            val result = invokeAnnotatedMethod(entity, function, FunctionCallAnnotation::class.java, stepContext)
            stepContext = result
            true
        } catch (e: Exception) {
            log.error("FunctionCall failed for {}.{}(): {}", entityType, function, e.message, e)
            false
        }
    }

    // ── Assert ────────────────────────────────────────────────────────────────

    private suspend fun executeAssert(member: JsonObject, failurePolicy: FailurePolicy): Boolean {
        val entityId   = member.getString("entityId")   ?: return logMissingField(member, "entityId")
        val entityType = member.getString("entityType") ?: return logMissingField(member, "entityType")
        val function   = member.getString("function")   ?: return logMissingField(member, "function")

        return try {
            val entity = resolveEntity(entityId, entityType)
            val result = invokeAnnotatedMethod(entity, function, AssertAnnotation::class.java, stepContext)
            stepContext = result

            val passed = result as? Boolean
                ?: run {
                    log.warn(
                        "Assert {}.{}() returned non-Boolean: {}. Treating as failure.",
                        entityType, function, result
                    )
                    false
                }

            if (!passed) {
                log.info(
                    "Assert {}.{}() failed — applying policy {}",
                    entityType, function, failurePolicy
                )
            }

            passed
        } catch (e: Exception) {
            log.error("Assert failed for {}.{}(): {}", entityType, function, e.message, e)
            false
        }
    }

    // ── Trigger ───────────────────────────────────────────────────────────────

    private fun fireTrigger(member: JsonObject) {
        val entityId   = member.getString("entityId")   ?: return
        val entityType = member.getString("entityType") ?: return
        val function   = member.getString("function")   ?: return

        // Capture current stepContext — trigger is fire-and-forget, should not
        // race on the mutable field after we advance.
        val capturedContext = stepContext

        workerScope.launch {
            try {
                val entity = resolveEntity(entityId, entityType)
                invokeAnnotatedMethod(entity, function, TriggerAnnotation::class.java, capturedContext)
            } catch (e: Exception) {
                log.error("Trigger {}.{}() threw: {}", entityType, function, e.message, e)
            }
        }
    }

    // ── Rollback ──────────────────────────────────────────────────────────────

    /**
     * Reverse completed mutations in reverse execution order.
     * Each reversal goes through the normal handler path so that deltas are captured.
     *
     * MUTATE  → reverse MUTATE restoring the pre-set field values
     * CREATE  → DELETE
     * DELETE  → not yet supported (logged as warning)
     */
    private suspend fun rollback(logicalFrame: Long) {
        log.info("Rolling back OperationSet — {} mutations to reverse", completedMutations.size)

        for (member in completedMutations.reversed()) {
            val entityId   = member.getString("entityId")   ?: continue
            val entityType = member.getString("entityType") ?: continue
            val action     = member.getString("action")     ?: continue

            val reverseOp = when (action) {
                "MUTATE" -> buildReverseMutate(member, entityId, entityType)
                "CREATE" -> buildReverseDelete(entityId, entityType)
                "DELETE" -> {
                    log.warn("DELETE rollback not yet supported for entity {} — delta stands", entityId)
                    null
                }
                else -> null
            } ?: continue

            val context = JsonObject()
                .put("operation", reverseOp)
                .put("frameNumber", logicalFrame)

            val address = "worker.process.${reverseOp.getString("action")}.$instanceId"

            try {
                vertx.eventBus().request<JsonObject>(address, context).await()
                log.debug("Rolled back {} for entity {}", action, entityId)
            } catch (e: Exception) {
                log.error("Rollback of {} for entity {} failed: {}", action, entityId, e.message, e)
            }
        }
    }

    private suspend fun buildReverseMutate(
        original: JsonObject,
        entityId: String,
        entityType: String
    ): JsonObject {
        // The delta fields from the original operation tell us which fields were targeted.
        // Restore them to the values captured before the set ran.
        val originalDelta = original.getJsonObject("delta") ?: JsonObject()
        val beforeState   = beforeStates[entityId]?.getJsonObject("state") ?: JsonObject()
        val currentEntity = stateStore.findOneJson(entityId)

        val restoredFields = JsonObject()
        for (field in originalDelta.fieldNames()) {
            beforeState.getValue(field)?.let { restoredFields.put(field, it) }
        }

        return JsonObject()
            .put("id",         java.util.UUID.randomUUID().toString())
            .put("entityId",   entityId)
            .put("entityType", entityType)
            .put("action",     "MUTATE")
            .put("delta",      restoredFields)
            .put("version",    currentEntity?.getLong("version"))
            .put("timestamp",  System.currentTimeMillis())
    }

    private fun buildReverseDelete(entityId: String, entityType: String): JsonObject {
        return JsonObject()
            .put("id",         java.util.UUID.randomUUID().toString())
            .put("entityId",   entityId)
            .put("entityType", entityType)
            .put("action",     "DELETE")
            .put("delta",      JsonObject())
            .put("timestamp",  System.currentTimeMillis())
    }

    // ── Entity resolution ────────────────────────────────────────────────────

    /**
     * Return the cached entity instance for [entityId], hydrating on first access.
     * All predicate steps targeting the same entity share a single instance,
     * giving a stateful illusion for the duration of the set.
     */
    private suspend fun resolveEntity(entityId: String, entityType: String): Entity {
        return hydratedEntities.getOrPut(entityId) {
            hydrateEntity(entityId, entityType)
        }
    }

    private suspend fun hydrateEntity(entityId: String, entityType: String): Entity {
        val entityJson = stateStore.findOneJson(entityId)
            ?: throw IllegalStateException("Entity $entityId not found for hydration")

        val entity = entityFactory.getNew(entityType).apply {
            _id     = entityId
            type    = entityType
            version = entityJson.getLong("version") ?: 1L
        }

        val stateJson = entityJson.getJsonObject("state") ?: JsonObject()
        stateStore.setEntityState(entity, entityType, stateJson)

        return entity
    }

    /**
     * Apply a mutation delta to the in-memory entity instance via reflection.
     * Keeps the cached instance in sync with the store after a successful MUTATE.
     */
    private fun applyDeltaToInstance(entity: Entity, delta: JsonObject) {
        val fields = reflectionCache.getFields(entity.javaClass)
        for (fieldName in delta.fieldNames()) {
            val field = fields.firstOrNull { it.name == fieldName } ?: continue
            field.isAccessible = true
            val value = delta.getValue(fieldName)
            try {
                when (field.type) {
                    Int::class.java, java.lang.Integer::class.java ->
                        field.setInt(entity, (value as? Number)?.toInt() ?: continue)
                    Long::class.java, java.lang.Long::class.java ->
                        field.setLong(entity, (value as? Number)?.toLong() ?: continue)
                    Double::class.java, java.lang.Double::class.java ->
                        field.setDouble(entity, (value as? Number)?.toDouble() ?: continue)
                    Float::class.java, java.lang.Float::class.java ->
                        field.setFloat(entity, (value as? Number)?.toFloat() ?: continue)
                    Boolean::class.java, java.lang.Boolean::class.java ->
                        field.setBoolean(entity, value as? Boolean ?: continue)
                    String::class.java ->
                        field.set(entity, value as? String)
                    else ->
                        field.set(entity, value)
                }
            } catch (e: Exception) {
                log.warn("Failed to apply delta field '{}' to {}: {}", fieldName, entity.javaClass.simpleName, e.message)
            }
        }
    }

    // ── Reflection invocation ─────────────────────────────────────────────────

    /**
     * Invoke [functionName] on [entity], gated by [requiredAnnotation].
     * Only methods carrying the specified annotation are eligible.
     *
     * Both suspend and non-suspend methods are supported. If the method cannot be
     * located or lacks the annotation, a loud log is emitted and an exception is thrown.
     */
    private suspend fun invokeAnnotatedMethod(
        entity: Any,
        functionName: String,
        requiredAnnotation: Class<out Annotation>,
        context: Any?
    ): Any? {
        val annotatedMethods = reflectionCache.getFunctions(entity.javaClass)
            .filter { it.isAnnotationPresent(requiredAnnotation) }

        val jMethod = annotatedMethods.firstOrNull { it.name == functionName }

        if (jMethod == null) {
            val available = annotatedMethods.joinToString { it.name }
            val annotationName = requiredAnnotation.simpleName
            throw IllegalStateException(
                "Method '$functionName' not found on ${entity.javaClass.simpleName} " +
                "with @$annotationName. Available @$annotationName methods: [$available]"
            )
        }

        jMethod.isAccessible = true
        val kFunction = jMethod.kotlinFunction ?: run {
            // Plain Java method — invoke directly
            return if (jMethod.parameterCount >= 1 && context != null) {
                jMethod.invoke(entity, context)
            } else {
                jMethod.invoke(entity)
            }
        }

        // kotlinFunction.parameters includes the receiver at index 0.
        // A declared-parameterless method has parameters.size == 1 (receiver only).
        // A suspend method adds a Continuation at the end, but callSuspend handles that.
        val hasExtraParam = kFunction.parameters.size >= 2

        return if (hasExtraParam && context != null) {
            if (kFunction.isSuspend) kFunction.callSuspend(entity, context)
            else kFunction.call(entity, context)
        } else {
            if (kFunction.isSuspend) kFunction.callSuspend(entity)
            else kFunction.call(entity)
        }
    }

    // ── Before-state snapshot ─────────────────────────────────────────────────

    private suspend fun snapshotBeforeStates(members: List<JsonObject>) {
        val entityIds = members
            .mapNotNull { it.getString("entityId") }
            .distinct()

        stateStore.findJson(entityIds).forEach { (id, json) ->
            beforeStates[id] = json
        }
    }

    // ── Logging helpers ───────────────────────────────────────────────────────

    private fun logUnknownId(member: JsonObject): Boolean {
        log.error("OperationSet member missing 'id': {}", member.encode())
        return false
    }

    private fun logMissingField(member: JsonObject, field: String): Boolean {
        log.error("OperationSet member missing '{}': {}", field, member.encode())
        return false
    }

    private fun resolvePolicy(firstMember: JsonObject): FailurePolicy {
        return try {
            FailurePolicy.valueOf(firstMember.getString("failurePolicy") ?: "ABORT")
        } catch (e: IllegalArgumentException) {
            log.warn("Unknown failurePolicy value '{}', defaulting to ABORT", firstMember.getString("failurePolicy"))
            FailurePolicy.ABORT
        }
    }
}
