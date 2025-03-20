package com.minare.core.entity

import com.minare.core.models.Entity
import org.slf4j.LoggerFactory
import javax.inject.Singleton
import kotlin.reflect.KClass

/**
 * Default implementation of EntityFactory that provides basic entity creation.
 * Applications can replace this with their own implementation by binding a different
 * implementation to the EntityFactory interface in their Guice module.
 */
@Singleton
class DefaultEntityFactory : EntityFactory {
    private val log = LoggerFactory.getLogger(DefaultEntityFactory::class.java)
    private val classes: HashMap<String, Class<*>> = HashMap()

    init {
        // Register base entity type
        classes["entity"] = Entity::class.java
    }

    override fun useClass(type: String): Class<*>? {
        return classes[type.lowercase()]
    }

    override fun getNew(type: String): Entity {
        return when (type.lowercase()) {
            "entity" -> Entity()
            else -> {
                log.warn("Unknown entity type: $type - falling back to base Entity")
                Entity() // Default fallback
            }
        }
    }

    /**
     * Create a new instance of an entity based on its class
     * This implementation supports the base Entity class
     */
    @Suppress("UNCHECKED_CAST")
    override fun <T : Entity> createEntity(entityClass: Class<T>): T {
        return when {
            entityClass.isAssignableFrom(Entity::class.java) -> Entity() as T
            else -> {
                log.warn("Unsupported entity class: ${entityClass.name} - falling back to base Entity")
                Entity() as T
            }
        }
    }

    override fun getTypeNames(): List<String> {
        return classes.keys.toList()
    }

    override fun getTypeList(): List<KClass<*>> {
        return listOf(Entity::class)
    }
}