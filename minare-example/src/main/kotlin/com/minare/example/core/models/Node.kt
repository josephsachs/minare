package com.minare.example.core.models

import com.minare.core.models.Entity
import com.minare.core.entity.annotations.Child
import com.minare.core.entity.annotations.EntityType
import com.minare.core.entity.annotations.Parent
import com.minare.core.entity.annotations.State

@EntityType("node")
class Node() : Entity() {
    @State
    var label: String = ""

    @State
    @Parent
    var parentId: String? = null

    @State
    @Child
    var childIds: MutableList<String> = mutableListOf()

    @State
    var value: Int = 0

    fun addChild(child: Node) {
        child._id?.let { childIds.add(it) }
        child.parentId = this._id
    }
}