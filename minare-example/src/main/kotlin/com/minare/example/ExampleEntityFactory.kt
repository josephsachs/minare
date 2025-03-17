package com.minare.example

import com.minare.core.entity.EntityFactory
import com.minare.core.models.Entity
import com.minare.example.core.models.Node
import kotlin.reflect.KClass

class TestEntityFactory : EntityFactory {
    private val classes: HashMap<String, Class<*>> = HashMap()

    init {
        // Register our base types
        classes.put("Node", Node::class.java)
    }

    override fun useClass(type: String): Class<*>? {
        return classes[type.lowercase()]
    }

    override fun getNew(type: String): Entity {
        return when (type.lowercase()) {
            "Node" -> Node()
            else -> Entity()
        }
    }

    /**
     * Helper method to get registered entity types - implement based on your EntityFactory
     */
    override fun getTypeNames(): List<String> {
        return listOf("Node")
    }

    override fun getTypeList(): List<KClass<*>> {
        return listOf(
            Node::class
        )
    }
}