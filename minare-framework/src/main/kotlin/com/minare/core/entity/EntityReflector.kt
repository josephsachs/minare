package com.minare.core.entity

import com.google.inject.Singleton
import com.google.inject.Inject
import com.minare.core.models.Entity
import com.minare.core.entity.annotations.EntityType
import com.minare.core.entity.annotations.ParentReference
import com.minare.core.entity.annotations.State
import kotlin.reflect.KClass
import java.util.concurrent.ConcurrentHashMap

/**
 * Injectable utility class for building reflection caches for entity classes.
 */
@Singleton
class EntityReflector @Inject constructor() {

    // Cache of entity type to reflection information
    private val reflectionCacheByType = ConcurrentHashMap<String, ReflectionCache>()
    private val reflectionCacheByClass = ConcurrentHashMap<Class<*>, ReflectionCache>()

    /**
     * Gets or builds the reflection cache for an entity type.
     */
    fun getReflectionCache(entityType: String): ReflectionCache? = reflectionCacheByType[entityType]

    /**
     * Gets or builds the reflection cache for an entity class.
     */
    fun getReflectionCache(entityClass: Class<*>): ReflectionCache =
            reflectionCacheByClass.computeIfAbsent(entityClass) { buildReflectionCache(it) }

    /**
     * Kotlin-friendly version that works with KClass.
     */
    fun getReflectionCache(entityClass: KClass<*>): ReflectionCache =
            getReflectionCache(entityClass.java)

    /**
     * Builds reflection cache for an entity class.
     */
    fun buildReflectionCache(entityClass: Class<*>): ReflectionCache {
        val entityAnnotation = entityClass.getAnnotation(EntityType::class.java)
            ?: throw IllegalArgumentException("Class ${entityClass.name} is not annotated with @Entity")

        val entityType = entityAnnotation.value

        val parentReferenceFields = mutableListOf<ReflectionCache.ParentReferenceField>()

        // Process all fields in the class hierarchy
        var currentClass: Class<*>? = entityClass
        while (currentClass != null && currentClass != Any::class.java) {
            processClassFields(currentClass, parentReferenceFields)
            currentClass = currentClass.superclass
        }

        val cache = ReflectionCache(entityType, parentReferenceFields)

        // Store in both maps for efficient lookup
        reflectionCacheByType[entityType] = cache

        return cache
    }

    private fun processClassFields(
            clazz: Class<*>,
    parentReferenceFields: MutableList<ReflectionCache.ParentReferenceField>
    ) {
        // Kotlin-style stream processing
        val classFields = clazz.declaredFields
                .filter { it.isAnnotationPresent(State::class.java) &&
            it.isAnnotationPresent(ParentReference::class.java) }
            .map { field ->
                val parentRef = field.getAnnotation(ParentReference::class.java)
            val bubbleVersion = parentRef.bubble_version
            ReflectionCache.ParentReferenceField(field.name, bubbleVersion)
        }

        parentReferenceFields.addAll(classFields)
    }

    /**
     * Gets all parent reference field paths for all known entity types.
     * This is useful for building MongoDB traversal queries.
     */
    fun getAllParentReferenceFieldPaths(): List<String> =
    reflectionCacheByType.values
            .flatMap { it.parentReferenceFields }
            .map { "state.${it.fieldName}" }
            .distinct()

    /**
     * Kotlin-friendly version that works with KClass.
     */
    fun registerEntityClasses(entityClasses: List<KClass<out Entity>>) {
        entityClasses.forEach { buildReflectionCache(it.java) }
    }
}