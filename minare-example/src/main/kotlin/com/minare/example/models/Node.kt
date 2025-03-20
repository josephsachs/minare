package com.minare.example.models

import com.minare.core.models.Entity
import com.minare.core.entity.annotations.Child
import com.minare.core.entity.annotations.EntityType
import com.minare.core.entity.annotations.Parent
import com.minare.core.entity.annotations.State

@EntityType("Node")
class Node() : Entity() {
    init {
        // Set the type property based on the EntityType annotation
        type = "Node"
    }

    @State
    var label: String = ""

    @State
    @Parent
    var parentId: String? = null

    @State
    @Child
    var childIds: MutableList<String> = mutableListOf()

    @State
    var color: String = "#CCCCCC" // Default boring gray color

    /**
     * Add a child node to this node
     * Updates the in-memory relationship only - caller must persist both entities
     */
    fun addChild(child: Node) {
        child._id?.let { childId ->
            if (!childIds.contains(childId)) {
                childIds.add(childId)
            }
        }

        child.parentId = this._id
    }
}