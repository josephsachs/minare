package com.minare.example

import com.minare.core.entity.EntityFactory
import com.minare.core.models.Entity
import com.minare.example.core.models.Node
import org.slf4j.LoggerFactory
import javax.inject.Singleton
import kotlin.reflect.KClass

@Singleton
class ExampleEntityFactory : EntityFactory {
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
        return when (normalizedType) {
            "node" -> Node()
            else -> {
                if (normalizedType != "entity") {
                    log.warn("Unknown entity type requested: $type, returning generic Entity")
                }
                Entity()
            }
        }
    }

    /**
     * Type-safe entity creation method
     */
    @Suppress("UNCHECKED_CAST")
    override fun <T : Entity> createEntity(entityClass: Class<T>): T {
        return when {
            Node::class.java.isAssignableFrom(entityClass) -> Node() as T
            Entity::class.java.isAssignableFrom(entityClass) -> Entity() as T
            else -> {
                log.warn("Unknown entity class requested: ${entityClass.name}, returning generic Entity")
                Entity() as T
            }
        }
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
}