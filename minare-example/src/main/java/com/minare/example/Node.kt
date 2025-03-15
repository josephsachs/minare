package com.minare.example

import com.minare.core.models.Entity
import com.minare.core.models.annotations.entity.ChildReference
import com.minare.core.models.annotations.entity.EntityType
import com.minare.core.models.annotations.entity.ParentReference
import com.minare.core.models.annotations.entity.State
import io.vertx.core.json.JsonObject

@EntityType("node")
class Node : Entity {
    @State
    val label: String

    @State
    @ParentReference
    var parentId: String? = null

    @State
    @ChildReference
    val childIds: MutableList<String> = mutableListOf()

    @State
    val value: Int

    constructor(label: String, value: Int) : super("node") {
        this.label = label
        this.value = value
    }

    override fun toJson(): JsonObject {
        val json = super.toJson()
        val stateJson = JsonObject()
            .put("label", label)
            .put("parentId", parentId)
            .put("childIds", childIds)
            .put("value", value)

        return json.put("state", stateJson)
    }

    fun addChild(child: Node) {
        childIds.add(child.id)
        child.parentId = this.id
    }
}