package com.minare.example

import com.minare.core.models.Entity
import com.minare.core.entity.annotations.ChildReference
import com.minare.core.entity.annotations.EntityType
import com.minare.core.entity.annotations.ParentReference
import com.minare.core.entity.annotations.State

@EntityType("node")
class Node() : Entity() {
    @State
    var label: String = ""

    @State
    @ParentReference
    var parentId: String? = null

    @State
    @ChildReference
    var childIds: MutableList<String> = mutableListOf()

    @State
    var value: Int = 0

    fun addChild(child: Node) {
        child._id?.let { childIds.add(it) }
        child.parentId = this._id
    }
}