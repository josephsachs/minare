package com.minare.core.entity.services

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.models.Entity
import com.minare.core.storage.interfaces.StateStore
import io.vertx.core.impl.logging.LoggerFactory
import java.lang.reflect.Field
import kotlin.reflect.KClass

@Singleton
class EntityInspector @Inject constructor(
    private val reflectionCache: ReflectionCache,
    private val entityFactory: EntityFactory,
    private val stateStore: StateStore
) {
    private val log = LoggerFactory.getLogger(EntityInspector::class.java)

    suspend fun getFieldsOfType(entityId: String, annotationTypes: List<KClass<out Annotation>>): List<Field> {
        val entityType = stateStore.findEntityType(entityId)
        if (entityType.isNullOrBlank()) {
            return emptyList()
        }

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

    suspend fun getFieldsOfType(entity: Entity, annotationTypes: List<KClass<out Annotation>>): List<Field> {
        if (entity.type.isNullOrBlank()) return emptyList()
        entityFactory.useClass(entity.type!!)?.let { entityClass ->
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
}