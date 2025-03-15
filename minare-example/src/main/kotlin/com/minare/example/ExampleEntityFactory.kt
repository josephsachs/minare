package com.minare.example

import com.minare.core.entity.EntityFactory
import com.minare.core.models.Entity

class TestEntityFactory : _root_ide_package_.com.minare.core.entity.EntityFactory {
    private val classes: HashMap<String, Class<*>> = HashMap()

    init {
        // Register our base types
        classes.put("node", Node::class.java)
    }

    override fun useClass(type: String): Class<*>? {
        return classes[type.lowercase()]
    }

    override fun getNew(type: String): Entity {
        return when (type.lowercase()) {
            "node" -> Node()
            else -> Entity()
        }
    }
}