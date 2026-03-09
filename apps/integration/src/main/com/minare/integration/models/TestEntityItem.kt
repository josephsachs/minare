package com.minare.integration.models

import com.minare.core.entity.annotations.EntityType
import com.minare.core.entity.annotations.State
import com.minare.core.entity.models.Entity
import org.slf4j.LoggerFactory

@EntityType("TestEntityItem")
class TestEntityItem: Entity() {
    private val log = LoggerFactory.getLogger(TestEntityItem::class.java)

    init {
        type = "TestEntityItem"
    }

    @State
    var label: String = ""
}