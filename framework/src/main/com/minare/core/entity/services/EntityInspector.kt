package com.minare.core.entity.services

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.models.Entity
import java.lang.reflect.Field
import kotlin.reflect.KClass

@Singleton
class EntityInspector @Inject constructor(
    private val reflectionCache: ReflectionCache,
    private val entityFactory: EntityFactory
) {
    fun getFieldsOfType(entityType: String, annotationTypes: List<KClass<out Annotation>>): List<Field> {
        if (entityType.isBlank()) return emptyList()

        entityFactory.useClass(entityType)?.let { entityClass ->
            val allFields = reflectionCache.getFields(entityClass.kotlin)
            val fields = allFields.filter { field ->
                annotationTypes.any { annotationType ->
                    field.isAnnotationPresent(annotationType.java)
                }
            }
            fields.forEach { field ->
                field.isAccessible = true
            }
            return fields
        }
        return emptyList()
    }

    fun getFieldsOfType(entity: Entity, annotationTypes: List<KClass<out Annotation>>): List<Field> {
        val type = entity.type
        if (type.isBlank()) return emptyList()
        return getFieldsOfType(type, annotationTypes)
    }
}