package com.minare.example

import com.minare.core.models.Entity
import com.minare.core.models.annotations.entity.ChildReference
import com.minare.core.models.annotations.entity.EntityType
import com.minare.core.models.annotations.entity.ParentReference
import com.minare.core.models.annotations.entity.State

@EntityType("node")
class Node : Entity() {
    @State
    @ParentReference
    val parent = String

    @State
    @ChildReference
    val child = String

    @State
    val value = Int
}