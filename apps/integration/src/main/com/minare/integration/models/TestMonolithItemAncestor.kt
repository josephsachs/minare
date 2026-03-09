package com.minare.integration.models

import com.google.inject.Inject
import com.minare.core.entity.annotations.EntityType
import com.minare.core.entity.models.Entity
import io.vertx.core.Vertx
import org.slf4j.LoggerFactory

@EntityType("TestMonolithItemAncestor")
class TestMonolithItemAncestor @Inject constructor(
    private val vertx: Vertx
): Entity() {
    private val log = LoggerFactory.getLogger(TestMonolithItemAncestor::class.java)

    init {
        type = "TestMonolithItemAncestor"
    }
}