package com.minare.core.entity.services

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.Mutable
import com.minare.core.entity.annotations.State
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
    private val stateStore: StateStore,
    private val versioningService: EntityVersioningService
) {
    private val log = LoggerFactory.getLogger(MutationService::class.java)

    /**
     * Process a mutation request for an entity
     *
     * @param entityId The ID of the entity to mutate
     * @param entityType The type of the entity
     * @param requestObject The mutation request object containing state changes
     * @return Result object indicating success or failure with message
     */
    suspend fun mutate(entityId: String, entityType: String, requestObject: JsonObject): JsonObject {
        log.debug("Processing mutation for entity $entityId of type $entityType")

        val delta = requestObject.getJsonObject("state") ?: JsonObject()
        val requestedVersion = requestObject.getLong("version", 0L)

        // Get the current entity state from Redis
        val currentJson = stateStore.findEntityJson(entityId)
            ?: return JsonObject()
                .put("success", false)
                .put("message", "Entity not found: $entityId")

        val currentVersion = currentJson.getLong("version", 1L)

        // Get the entity class for reflection (no Entity instance needed)
        val entityClass = entityFactory.useClass(entityType)
            ?: return JsonObject()
                .put("success", false)
                .put("message", "Unknown entity type: $entityType")

        // Process the mutation delta using direct ReflectionCache calls
        val prunedDelta = getMutateDelta(delta, entityClass)

        if (prunedDelta.isEmpty) {
            return JsonObject()
                .put("success", false)
                .put("message", "No valid mutable fields found")
        }

        // TODO: Old feature will probably be changed all to hell in V0.4.0
        val allowedChanges = filterDeltaByConsistencyLevel(
            prunedDelta,
            requestedVersion,
            currentVersion,
            entityClass
        )

        if (allowedChanges.isEmpty) {
            return JsonObject()
                .put("success", false)
                .put("message", "No allowed changes based on consistency rules")
        }

        // Apply the mutation to the entity state
        try {
            stateStore.saveState(entityId, allowedChanges)

            // Temporarily disabled
            // We need to fix delta updates to merge rather than replace
            // Revisit in V3
            // versioningService.bubbleVersions(entityId)

            return JsonObject()
                .put("success", true)
                .put("message", "Mutation successful")
        } catch (e: Exception) {
            log.error("Failed to mutate entity state: $entityId", e)
            return JsonObject()
                .put("success", false)
                .put("message", "Mutation failed: ${e.message}")
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
     * Filter changes based on consistency level rules
     * Updated to use direct ReflectionCache calls instead of Entity wrapper methods
     */
    private fun filterDeltaByConsistencyLevel(
        delta: JsonObject,
        requestedVersion: Long,
        currentVersion: Long,
        entityClass: Class<*>
    ): JsonObject {
        if (delta.isEmpty) {
            return JsonObject()
        }

        // Use direct ReflectionCache call instead of Entity wrapper method
        val mutableFields = reflectionCache.getFieldsWithAnnotation<Mutable>(entityClass.kotlin)

        // First check for any STRICT fields with version mismatch
        val strictViolation = delta.fieldNames().any { fieldName ->
            val field = findFieldByStateName(mutableFields, fieldName) ?: return@any false
            val mutableAnnotation = field.getAnnotation(Mutable::class.java)

            mutableAnnotation.consistency == Mutable.Companion.ConsistencyLevel.STRICT &&
                    currentVersion != requestedVersion
        }

        if (strictViolation) {
            log.warn("Strict consistency violation detected. Entity type: $entityClass.simpleName, Current: $currentVersion, Requested: $requestedVersion")
            return JsonObject()
        }

        // Filter remaining fields based on consistency levels
        val result = JsonObject()

        delta.fieldNames().forEach { fieldName ->
            val field = findFieldByStateName(mutableFields, fieldName) ?: return@forEach
            val mutableAnnotation = field.getAnnotation(Mutable::class.java)

            when (mutableAnnotation.consistency) {
                Mutable.Companion.ConsistencyLevel.OPTIMISTIC ->
                    result.put(fieldName, delta.getValue(fieldName))
                Mutable.Companion.ConsistencyLevel.PESSIMISTIC -> {
                    if (requestedVersion >= currentVersion) {
                        result.put(fieldName, delta.getValue(fieldName))
                    }
                }
                Mutable.Companion.ConsistencyLevel.STRICT ->
                    result.put(fieldName, delta.getValue(fieldName)) // Already checked above
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