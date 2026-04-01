package com.minare.core.entity.services

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.Mutable
import com.minare.core.entity.annotations.State
import com.minare.core.entity.annotations.VersionPolicy
import com.minare.core.storage.interfaces.MutationResult
import com.minare.core.storage.interfaces.StateStore
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.lang.reflect.Field

/**
 * Service responsible for handling entity mutations without creating full Entity objects.
 * This replaces the Entity.mutate() functionality but operates directly on JsonObjects.
 *
 * Updated to use direct ReflectionCache calls instead of Entity wrapper methods.
 */
@Singleton
class MutationService @Inject constructor(
    private val reflectionCache: ReflectionCache,
    private val entityFactory: EntityFactory,
    private val stateStore: StateStore
) {
    private val log = LoggerFactory.getLogger(MutationService::class.java)

    /**
     * Result of a successful mutation, carrying the atomic before/after snapshots.
     */
    data class MutateResult(
        val before: JsonObject,
        val after: JsonObject,
        val version: Long
    )

    /**
     * Process a mutation request for an entity.
     *
     * Validates @Mutable field filtering in Kotlin, then performs an atomic
     * version-check + merge + version-bump + snapshot via a single Lua EVAL.
     * No separate read required — the version policy is enforced inside Redis.
     *
     * @param entityId The ID of the entity to mutate
     * @param entityType The type of the entity
     * @param requestObject The mutation request containing state changes and version
     * @return MutateResult on success, or a JsonObject with success=false on rejection
     */
    suspend fun mutate(entityId: String, entityType: String, requestObject: JsonObject): Any {
        log.debug("Processing mutation for entity $entityId of type $entityType")

        val delta = requestObject.getJsonObject("state") ?: JsonObject()
        val incomingVersion = requestObject.getLong("version", 0L)

        val entityClass = entityFactory.useClass(entityType)
            ?: return JsonObject()
                .put("success", false)
                .put("message", "Unknown entity type: $entityType")

        val prunedDelta = getMutateDelta(delta, entityClass)

        if (prunedDelta.isEmpty) {
            return JsonObject()
                .put("success", false)
                .put("message", "Entity $entityId no valid mutable fields found")
        }

        // Resolve version policy from entity class annotation
        val versionAnnotation = entityClass.getAnnotation(VersionPolicy::class.java)
        val versionPolicyName = versionAnnotation?.rule?.name

        try {
            val mutationResult = stateStore.mutateAndReturn(
                entityId,
                prunedDelta,
                versionPolicyName,
                incomingVersion
            )

            return when (mutationResult) {
                null -> JsonObject()
                    .put("success", false)
                    .put("message", "Entity $entityId not found during atomic mutation")

                is MutationResult.VersionRejected -> JsonObject()
                    .put("success", false)
                    .put("message", "Entity $entityId version rejected: incoming $incomingVersion vs current ${mutationResult.currentVersion}")

                is MutationResult.Success -> MutateResult(
                    before = mutationResult.before,
                    after = mutationResult.after,
                    version = mutationResult.version
                )
            }
        } catch (e: Exception) {
            log.error("Failed to mutate entity state: $entityId", e)
            return JsonObject()
                .put("success", false)
                .put("message", "Entity $entityId mutation failed: ${e.message}")
        }
    }

    /**
     * Filter the mutation delta to only include fields that are marked as @Mutable
     */
    private fun getMutateDelta(delta: JsonObject, entityClass: Class<*>): JsonObject {
        if (delta.isEmpty) {
            return JsonObject()
        }

        // Use direct ReflectionCache call instead of Entity wrapper method
        val mutableFields = reflectionCache.getFieldsWithAnnotation<Mutable>(entityClass.kotlin)
        val result = JsonObject()

        delta.fieldNames().forEach { fieldName ->
            val field = findFieldByStateName(mutableFields, fieldName)
            if (field != null) {
                result.put(fieldName, delta.getValue(fieldName))
            }
        }

        return result
    }

    /**
     * Find a field by its state name (from @State annotation or field name)
     */
    private fun findFieldByStateName(fields: List<Field>, stateName: String): Field? {
        return fields.find {
            val stateAnnotation = it.getAnnotation(State::class.java)
            val fieldStateName = stateAnnotation?.fieldName?.takeIf { name -> name.isNotEmpty() } ?: it.name
            fieldStateName == stateName
        }
    }
}