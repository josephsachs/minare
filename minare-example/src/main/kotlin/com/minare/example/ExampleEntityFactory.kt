package com.minare.example

import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.models.Entity
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
    private val classes: HashMap<String, Class<out Entity>> = HashMap()

    init {
        classes["Node"] = Node::class.java
        classes["Entity"] = Entity::class.java

        log.info("Registered entity types: ${classes.keys.joinToString()}")
    }

    override fun useClass(type: String): Class<out Entity>? {
        val normalizedType = type
        val result = classes[normalizedType]
        if (result == null) {
            log.warn("Unknown entity type requested: $type")
        }
        return result
    }

    override fun getNew(type: String): Entity {
        return when (type) {
            "Node" -> Node()
            else -> {
                if (type != "Entity") {
                    log.warn("Unknown entity type requested: $type, returning generic Entity")
                }
                Entity()
            }
        }
    }

    override fun createEntity(entityClass: Class<*>): Entity {
        return when {
            Node::class.java.isAssignableFrom(entityClass) -> Node()
            Entity::class.java.isAssignableFrom(entityClass) -> Entity()
            else -> {
                log.warn("Unknown entity class requested: ${entityClass.name}, returning generic Entity")
                Entity()
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