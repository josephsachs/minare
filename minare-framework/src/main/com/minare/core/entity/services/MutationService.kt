package com.minare.core.entity.services

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.Mutable
import com.minare.core.entity.annotations.State
import com.minare.core.entity.annotations.VersionPolicy
import com.minare.core.entity.annotations.VersionPolicy.Companion.VersionPolicyType
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
        val deltaEntityVersion = requestObject.getLong("version", 0L)

        val currentJson = stateStore.findEntityJson(entityId)
            ?: return JsonObject()
                .put("success", false)
                .put("message", "Entity not found: $entityId")

        val storedEntityVersion = currentJson.getLong("version", 1L)

        val entityClass = entityFactory.useClass(entityType)
            ?: return JsonObject()
                .put("success", false)
                .put("message", "Unknown entity type: $entityType")

        val prunedDelta = getMutateDelta(delta, entityClass)

        if (prunedDelta.isEmpty) {
            return JsonObject()
                .put("success", false)
                .put("message", "Entity $entityId valid mutable fields found")
        }

        val allowedChanges = validateDelta(
            entityId,
            prunedDelta,
            deltaEntityVersion,
            storedEntityVersion,
            entityClass
        )

        if (allowedChanges.isEmpty) {
            return JsonObject()
                .put("success", false)
                .put("message", "No allowed changes based on consistency rules")
        }

        try {
            stateStore.saveState(entityId, allowedChanges)

            return JsonObject()
                .put("success", true)
                .put("message", "Entity $entityId mutation successful")
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
     * Filter changes based on consistency level rules
     * Updated to use direct ReflectionCache calls instead of Entity wrapper methods
     */
    private fun validateDelta(
        entityId: String,
        delta: JsonObject,
        deltaVersion: Long,
        stateVersion: Long,
        entityClass: Class<*>
    ): JsonObject {
        if (delta.isEmpty) {
            return JsonObject()
        }

        val mutableFields = reflectionCache.getFieldsWithAnnotation<Mutable>(entityClass.kotlin)
        val invalidations: MutableMap<String, String> = mutableMapOf()
        val changes = JsonObject()

        // First check for version mismatch
        if (checkVersion(deltaVersion, stateVersion, entityClass, invalidations)) {

            // Filter the delta for what we permit through
            for (fieldName in delta.fieldNames()) {
                // Make extra sure this matches a genuine state field
                val field = findFieldByStateName(mutableFields, fieldName)

                if (field == null) {
                    log.warn("Entity $entityId mutation tried to mutate non-state field ${fieldName}")
                    continue
                }

                val mutableAnnotation = field.getAnnotation(Mutable::class.java)

                when (mutableAnnotation.validationPolicy) {
                    Mutable.Companion.ValidationPolicy.NONE -> {
                        // We're allowing it, come what may
                        changes.put(fieldName, delta.getValue(fieldName))
                    }
                    Mutable.Companion.ValidationPolicy.FIELD -> {
                        if (invalidations.keys.contains(fieldName)) {
                            // Skip it
                            log.warn("Entity $entityId mutation field omitted: ${fieldName} because ${invalidations[fieldName]}")
                        } else {
                            changes.put(fieldName, delta.getValue(fieldName))
                        }
                    }
                    Mutable.Companion.ValidationPolicy.ENTITY -> {
                        if (invalidations.keys.contains(fieldName)) {
                            changes.clear() // Reject everything and bail
                            break
                        } else {
                            changes.put(fieldName, delta.getValue(fieldName))
                        }
                    }
                    Mutable.Companion.ValidationPolicy.OPERATION -> {
                        if (invalidations.keys.contains(fieldName)) {
                            throw NotImplementedError("Operation-level atomicity coming soon")
                        } else {
                            changes.put(fieldName, delta.getValue(fieldName))
                        }
                    }
                }
            }
        }

        if (invalidations.isNotEmpty()) {
            log.warn("Entity $entityId mutation delta failed validations: ${invalidations.entries}")
        }

        if (changes.isEmpty) {
            log.warn("Entity $entityId mutation contained no possible changes")
        }

        return changes
    }

    private fun checkVersion(
        incoming: Long,
        current: Long,
        entityClass: Class<*>,
        invalidations: MutableMap<String, String>
    ): Boolean {
        val entityName = entityClass.simpleName
        val versionAnnotation = entityClass.getAnnotation(VersionPolicy::class.java) ?: return true

        return if (versionAnnotation.rule == VersionPolicyType.MUST_MATCH &&
                incoming != current
            ) {
                invalidations[entityName] = "Attempted to apply non-matching Entity version"
                false
            } else if (versionAnnotation.rule == VersionPolicyType.ONLY_NEXT &&
                incoming != current + 1
            ) {
                invalidations[entityName] = "Attempted to apply invalid entity version"
                false
            } else if (versionAnnotation.rule == VersionPolicyType.ALLOW_NEWER &&
                incoming <= current
            ) {
                invalidations[entityName] = "Attempted to apply old entity version"
                false
            } else {
                true
            }
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