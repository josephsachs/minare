package com.minare.core.entity.factories

import com.google.inject.Injector
import com.minare.core.entity.models.Entity
import org.slf4j.LoggerFactory
import com.google.inject.Inject
import com.google.inject.Singleton
import kotlin.reflect.KClass

/**
 * Base EntityFactory that handles entity creation via Guice injection.
 * Subclasses only need to provide a map of type names to entity classes.
 *
 * The framework automatically:
 * - Creates instances via Guice (so @Inject fields work)
 * - Falls back to base Entity for unknown types
 * - Handles all the boilerplate
 */
abstract class EntityFactory @Inject constructor() {
    private val log = LoggerFactory.getLogger(EntityFactory::class.java)

    @Inject
    private lateinit var injector: Injector

    /**
     * Subclasses override this to register their entity types.
     * Example:
     *   override val entityTypes = mapOf(
     *       "Player" to PlayerEntity::class.java,
     *       "Enemy" to EnemyEntity::class.java
     *   )
     */
    protected abstract val entityTypes: Map<String, Class<out Entity>>

    /**
     * Get the class for a given type name, or null if not found.
     */
    open fun useClass(type: String): Class<out Entity>? {
        return entityTypes[type]
    }

    /**
     * Create a new entity by type name.
     * Automatically uses Guice to inject dependencies.
     */
    open fun getNew(type: String): Entity {
        val entityClass = entityTypes[type]
        return if (entityClass != null) {
            try {
                injector.getInstance(entityClass)
            } catch (e: Exception) {
                log.error("EntityFactory: Failed to create entity of type: $type", e)
                log.warn("EntityFactory: Falling back to base Entity")
                Entity()
            }
        } else {
            log.warn("EntityFactory: Unknown entity type: $type - falling back to base Entity")
            Entity()
        }
    }

    /**
     * Create an entity from a class object.
     * Automatically uses Guice to inject dependencies.
     */
    open fun createEntity(entityClass: Class<*>): Entity {
        return try {
            injector.getInstance(entityClass as Class<out Entity>)
        } catch (e: Exception) {
            log.error("Failed to create entity from class: ${entityClass.name}", e)
            log.warn("Falling back to base Entity")
            Entity()
        }
    }

    /**
     * Get all registered type names.
     */
    open fun getTypeNames(): List<String> {
        return entityTypes.keys.toList()
    }

    /**
     * Get all registered type classes.
     */
    open fun getTypeList(): List<KClass<*>> {
        return entityTypes.values.map { it.kotlin }
    }
}