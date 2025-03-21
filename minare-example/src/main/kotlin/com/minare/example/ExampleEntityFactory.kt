package com.minare.example

import com.minare.core.entity.EntityFactory
import com.minare.core.entity.ReflectionCache
import com.minare.core.models.Entity
import com.minare.example.models.Node
import com.minare.persistence.EntityStore
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.reflect.KClass

@Singleton
class ExampleEntityFactory @Inject constructor(
    private val reflectionCache: ReflectionCache,
    private val entityStore: EntityStore
) : EntityFactory {
    private val log = LoggerFactory.getLogger(ExampleEntityFactory::class.java)
    private val classes: HashMap<String, Class<*>> = HashMap()

    init {
        // Register our entity types - use lowercase for consistency in lookups
        classes["node"] = Node::class.java
        classes["entity"] = Entity::class.java  // Base entity type as fallback

        log.info("Registered entity types: ${classes.keys.joinToString()}")
    }

    override fun useClass(type: String): Class<*>? {
        val normalizedType = type.lowercase()
        val result = classes[normalizedType]
        if (result == null) {
            log.warn("Unknown entity type requested: $type")
        }
        return result
    }

    override fun getNew(type: String): Entity {
        val normalizedType = type.lowercase()
        val entity = when (normalizedType) {
            "node" -> Node()
            else -> {
                if (normalizedType != "entity") {
                    log.warn("Unknown entity type requested: $type, returning generic Entity")
                }
                Entity()
            }
        }

        // Inject dependencies
        return ensureDependencies(entity)
    }

    /**
     * Type-safe entity creation method
     */
    @Suppress("UNCHECKED_CAST")
    override fun <T : Entity> createEntity(entityClass: Class<T>): T {
        val entity = when {
            Node::class.java.isAssignableFrom(entityClass) -> Node() as T
            Entity::class.java.isAssignableFrom(entityClass) -> Entity() as T
            else -> {
                log.warn("Unknown entity class requested: ${entityClass.name}, returning generic Entity")
                Entity() as T
            }
        }

        // Inject dependencies
        return ensureDependencies(entity)
    }

    /**
     * Helper method to get registered entity types
     */
    override fun getTypeNames(): List<String> {
        return classes.keys.toList()
    }

    override fun getTypeList(): List<KClass<*>> {
        return listOf(
            Node::class,
            Entity::class
        )
    }

    /**
     * Ensure an entity has all required dependencies injected
     */
    override fun <T : Entity> ensureDependencies(entity: T): T {
        // Inject dependencies if they're not already initialized
        entity.reflectionCache = reflectionCache
        entity.entityStore = entityStore

        return entity
    }
}