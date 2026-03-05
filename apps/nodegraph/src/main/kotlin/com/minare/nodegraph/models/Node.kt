package com.minare.nodegraph.models

import com.minare.core.entity.annotations.*
import com.minare.core.entity.models.Entity
import org.slf4j.LoggerFactory
import java.io.Serializable

/**
 * Records the operation that last mutated this entity, captured at packaging time.
 * Serialized via Jackson as a nested data class within entity state.
 */
data class OperationRecord(
    val id: String,
    val entityId: String,
    val action: String,
    val frame: Long,
    val timestamp: Long,
    val delta: Map<String, Any?>? = null
) : Serializable

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

    @State
    @Mutable
    var lastOperation: OperationRecord? = null

    fun addChild(child: Node) {
        child._id?.let { childId ->
            if (!childIds.contains(childId)) {
                childIds.add(childId)
            }
        }
        child.parentId = this._id
    }

    @Task
    suspend fun tick() { }
}