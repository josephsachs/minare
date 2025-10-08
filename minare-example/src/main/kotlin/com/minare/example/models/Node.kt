package com.minare.example.models

import com.google.inject.Inject
import com.minare.core.entity.annotations.*
import com.minare.core.entity.models.Entity
import io.vertx.kotlin.core.Vertx
import org.slf4j.LoggerFactory

@EntityType("Node")
class Node() : Entity() {
    private val log = LoggerFactory.getLogger(Node::class.java) // Bad, tick testing only

    init {
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
    @Mutable
    var color: String = "#CCCCCC"

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

    @Task
    fun tick(workerId: String? = null) {
        log.info("Task for Entity with $_id; current color is $color")
    }
}