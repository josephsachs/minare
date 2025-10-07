package com.minare.core.entity

import com.google.inject.Singleton
import com.minare.core.entity.factories.EntityFactory
import io.vertx.core.impl.logging.LoggerFactory
import java.lang.reflect.Field
import java.lang.reflect.Method
import java.util.concurrent.ConcurrentHashMap
import kotlin.reflect.KClass

/**
 * Centralized cache for entity reflection data. Lazy-loads and stores
 * reflection information to avoid repeated reflection operations.
 */
class ReflectionCache {
    private val KClassesByType = ConcurrentHashMap<String, KClass<*>>()
    private val JClassesByType = ConcurrentHashMap<String, Class<*>>()
    private val fieldsByKClass = ConcurrentHashMap<KClass<*>, List<Field>>()
    private val fieldsByJClass = ConcurrentHashMap<Class<*>, List<Field>>()
    private val KClassesByFunction = ConcurrentHashMap<KClass<out Annotation>, Set<KClass<*>>>()
    private val JClassesByFunction = ConcurrentHashMap<Class<out Annotation>, Set<Class<*>>>()
    private val functionsByKClass = ConcurrentHashMap<KClass<*>, List<Method>>()
    private val functionsByJClass = ConcurrentHashMap<Class<*>, List<Method>>()

    private val log = LoggerFactory.getLogger(ReflectionCache::class.java)

    /**
     * Gets fields for an entity class
     */
    fun getFields(entityClass: KClass<*>): List<Field> {
        return fieldsByKClass.computeIfAbsent(entityClass) { clazz ->

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
     * Gets fields for an entity class
     */
    fun getFields(entityClass: Class<*>): List<Field> {
        return fieldsByJClass.computeIfAbsent(entityClass) { clazz ->

            val fields = mutableListOf<Field>()
            var currentClass: Class<*>? = clazz

            while (currentClass != null && currentClass != Any::class.java) {
                fields.addAll(currentClass.declaredFields)
                currentClass = currentClass.superclass
            }

            fields
        }
    }

    /**
     * Gets fields with a given annotation
     */
    inline fun <reified A : Annotation> getFieldsWithAnnotation(
        entityClass: KClass<*>
    ): List<Field> {
        val allFields = getFields(entityClass)
        return allFields.filter { it.isAnnotationPresent(A::class.java) }
    }

    /**
     * Gets fields with a given annotation
     */
    inline fun <reified A : Annotation> getFieldsWithAnnotation(
        entityClass: Class<*>
    ): List<Field> {
        return getFieldsWithAnnotation<A>(entityClass.kotlin)
    }

    /**
     * Gets methods for an entity class (Java Class)
     */
    fun getFunctions(entityClass: Class<*>): List<Method> {
        return functionsByJClass.computeIfAbsent(entityClass) { clazz ->
            val methods = mutableListOf<Method>()
            var currentClass: Class<*>? = clazz

            while (currentClass != null && currentClass != Any::class.java) {
                methods.addAll(currentClass.declaredMethods)
                currentClass = currentClass.superclass
            }

            methods
        }
    }

    /**
     * Gets methods for an entity class (Kotlin KClass)
     */
    fun getFunctions(entityClass: KClass<*>): List<Method> {
        return functionsByKClass.computeIfAbsent(entityClass) { kClass ->
            getFunctions(kClass.java)
        }
    }

    /**
     * Gets methods with a given annotation
     */
    inline fun <reified A : Annotation> getFunctionsWithAnnotation(
        entityClass: KClass<*>
    ): List<Method> {
        val allMethods = getFunctions(entityClass)
        return allMethods.filter { it.isAnnotationPresent(A::class.java) }
    }

    /**
     * Gets methods with a given annotation
     */
    inline fun <reified A : Annotation> getFunctionsWithAnnotation(
        entityClass: Class<*>
    ): List<Method> {
        return getFunctionsWithAnnotation<A>(entityClass.kotlin)
    }

    fun getKTypesHavingFunctionImpl(annotationClass: KClass<out Annotation>): Set<KClass<*>> {
        return KClassesByFunction.computeIfAbsent(annotationClass) {
            KClassesByType.values.filter { entityClass ->
                val allMethods = getFunctions(entityClass)
                allMethods.any { it.isAnnotationPresent(annotationClass.java) }
            }.toSet()
        }
    }

    fun getJTypesHavingFunctionImpl(annotationClass: Class<out Annotation>): Set<Class<*>> {
        return JClassesByFunction.computeIfAbsent(annotationClass) {
            JClassesByType.values.filter { entityClass ->
                val allMethods = getFunctions(entityClass)
                allMethods.any { it.isAnnotationPresent(annotationClass) }
            }.toSet()
        }
    }

    /**
     * Gets all entity types that have at least one function with the specified annotation
     */
    inline fun <reified A : Annotation> getKTypesHavingFunction(): Set<KClass<*>> {
        return getKTypesHavingFunctionImpl(A::class)
    }

    /**
     * Gets all entity types that have at least one function with the specified annotation
     */
    inline fun <reified A : Annotation> getJTypesHavingFunction(): Set<Class<*>> {
        return getJTypesHavingFunctionImpl(A::class.java)
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
                KClassesByType[typeName] = kClass
            }
        }
    }

    /**
     * Clear all cached data
     */
    fun clearCache() {
        fieldsByKClass.clear()
        fieldsByJClass.clear()
    }
}