package com.minare.core.entity.factories

import com.minare.core.entity.models.Entity
import org.slf4j.LoggerFactory
import javax.inject.Singleton
import kotlin.reflect.KClass

/**
 * Default implementation of EntityFactory that provides basic entity creation.
 * Applications can replace this with their own implementation by binding a different
 * implementation to the EntityFactory interface in their Guice module.
 *
 * Updated to remove dependency injection since Entity is now a pure data class.
 * This framework class only knows about the base Entity, not application-specific entities.
 */
@Singleton
class DefaultEntityFactory : EntityFactory {
    private val log = LoggerFactory.getLogger(DefaultEntityFactory::class.java)
    private val classes: HashMap<String, Class<out Entity>> = HashMap()

    init {
        classes["Entity"] = Entity::class.java
    }

    override fun useClass(type: String): Class<out Entity>? {
        return classes[type]
    }

    override fun getNew(type: String): Entity {
        return when (type) {
            "Entity" -> Entity()
            else -> {
                log.warn("Unknown entity type: $type - falling back to base Entity")
                Entity()
            }
        }
    }

    override fun createEntity(entityClass: Class<*>): Entity {
        return when {
            entityClass.isAssignableFrom(Entity::class.java) -> Entity()
            else -> {
                log.warn("Unsupported entity class: ${entityClass.name} - falling back to base Entity")
                Entity()
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