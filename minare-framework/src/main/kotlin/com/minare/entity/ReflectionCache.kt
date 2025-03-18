package com.minare.core.entity

import com.minare.core.entity.annotations.EntityType
import java.lang.reflect.Field
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass
import kotlin.reflect.full.findAnnotation

/**
 * Centralized cache for entity reflection data. Lazy-loads and stores
 * reflection information to avoid repeated reflection operations.
 */
class ReflectionCache {
    // Cache entity classes by type name and KClass object
    private val classesByType = ConcurrentHashMap<String, KClass<*>>()
    private val fieldsByClass = ConcurrentHashMap<KClass<*>, List<Field>>()

    /**
     * Gets all fields for an entity class, loading from cache or reflection
     */
    fun getFields(entityClass: KClass<*>): List<Field> {
        return fieldsByClass.computeIfAbsent(entityClass) { clazz ->
            // Collect all fields from the class hierarchy
            val fields = mutableListOf<Field>()
            var currentClass: Class<*>? = clazz.java

            while (currentClass != null && currentClass != Any::class.java) {
                fields.addAll(currentClass.declaredFields)
                currentClass = currentClass.superclass
            }

            fields
        }
    }

    /**
     * Gets fields with a specific annotation
     */
    inline fun <reified A : Annotation> getFieldsWithAnnotation(
        entityClass: KClass<*>
    ): List<Field> {
        val allFields = getFields(entityClass)
        return allFields.filter { it.isAnnotationPresent(A::class.java) }
    }

    /**
     * Overloaded version that takes a Java Class
     */
    inline fun <reified A : Annotation> getFieldsWithAnnotation(
        entityClass: Class<*>
    ): List<Field> {
        return getFieldsWithAnnotation<A>(entityClass.kotlin)
    }

    /**
     * Register entity classes using the EntityFactory
     */
    fun registerFromEntityFactory(entityFactory: EntityFactory) {
        // Get all entity type names from the factory
        val entityTypeNames = entityFactory.getTypeNames()

        // Register each type using its class
        entityTypeNames.forEach { typeName ->
            entityFactory.useClass(typeName)?.let { javaClass ->
                val kClass = javaClass.kotlin

                // Cache fields by class
                getFields(kClass)

                // Cache by type name
                classesByType[typeName] = kClass
            }
        }
    }

    /**
     * Register individual entity classes for pre-caching
     */
    fun registerEntityClasses(entityClasses: List<KClass<*>>) {
        entityClasses.forEach { entityClass ->
            // Cache by class
            getFields(entityClass)

            // Cache by type name if available
            entityClass.findAnnotation<EntityType>()?.let { annotation ->
                classesByType[annotation.value] = entityClass
            }
        }
    }

    /**
     * Get entity class by type name
     */
    fun getClassByType(typeName: String): KClass<*>? {
        return classesByType[typeName]
    }

    /**
     * Clear all cached data
     */
    fun clearCache() {
        fieldsByClass.clear()
        classesByType.clear()
    }

    /**
     * Future-proofing: Hook for Redis integration
     * This will be implemented later to load cached reflection data from Redis
     */
    fun loadFromCache(typeName: String): Boolean {
        // Placeholder for future Redis implementation
        return false
    }
}