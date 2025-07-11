package com.minare.example

import com.minare.core.entity.EntityFactory
import com.minare.core.models.Entity
import com.minare.example.models.Node
import org.slf4j.LoggerFactory
import javax.inject.Singleton
import kotlin.reflect.KClass

/**
 * Example EntityFactory implementation.
 * Updated to remove dependency injection since Entity is now a pure data class.
 */
@Singleton
class ExampleEntityFactory : EntityFactory {
    private val log = LoggerFactory.getLogger(ExampleEntityFactory::class.java)
    private val classes: HashMap<String, Class<*>> = HashMap()

    init {
        classes["node"] = Node::class.java
        classes["entity"] = Entity::class.java

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