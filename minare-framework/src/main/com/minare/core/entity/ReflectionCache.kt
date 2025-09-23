package com.minare.core.entity

import com.google.inject.Singleton
import com.minare.core.entity.factories.EntityFactory
import io.vertx.core.impl.logging.LoggerFactory
import java.lang.reflect.Field
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass

/**
 * Centralized cache for entity reflection data. Lazy-loads and stores
 * reflection information to avoid repeated reflection operations.
 */
class ReflectionCache {
    private val classesByType = ConcurrentHashMap<String, KClass<*>>()
    private val fieldsByClass = ConcurrentHashMap<KClass<*>, List<Field>>()

    private val log = LoggerFactory.getLogger(ReflectionCache::class.java)

    /**
     * Gets all fields for an entity class, loading from cache or reflection
     */
    fun getFields(entityClass: KClass<*>): List<Field> {
        return fieldsByClass.computeIfAbsent(entityClass) { clazz ->

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
        val entityTypeNames = entityFactory.getTypeNames()

        entityTypeNames.forEach { typeName ->
            entityFactory.useClass(typeName)?.let { javaClass ->
                val kClass = javaClass.kotlin
                getFields(kClass)
                classesByType[typeName] = kClass
            }
        }
    }

    /**
     * Clear all cached data
     */
    fun clearCache() {
        fieldsByClass.clear()
    }
}