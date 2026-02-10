package com.minare.integration.models

import com.minare.core.entity.annotations.*
import com.minare.core.entity.models.Entity
import org.slf4j.LoggerFactory
import java.io.Serializable

data class NodeDiag(
    val timestamp: Long,
    val message: String,
    val level: String  // e.g., "INFO", "WARN", "ERROR"
): Serializable

@EntityType("Node")
class Node(): Entity() {
    private val log = LoggerFactory.getLogger(Node::class.java)

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

    @Property
    var diagnostics: MutableList<NodeDiag> = mutableListOf()

    fun addChild(child: Node) {
        child._id?.let { childId ->
            if (!childIds.contains(childId)) {
                childIds.add(childId)
            }
        }
        child.parentId = this._id
    }

    @Task
    suspend fun tick() {
    }
}