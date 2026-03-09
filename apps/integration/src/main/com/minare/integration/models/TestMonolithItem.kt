package com.minare.integration.models

import com.google.inject.Inject
import com.minare.core.entity.annotations.*
import com.minare.core.entity.models.Entity
import com.minare.core.transport.models.Channel
import com.minare.core.transport.models.Context
import io.vertx.core.Vertx
import org.slf4j.LoggerFactory
import java.io.Serializable
import java.util.UUID

@EntityType("TestMonolithItem")
class TestMonolithItem @Inject constructor(
    private val vertx: Vertx
): Entity() {
    private val log = LoggerFactory.getLogger(TestMonolithItem::class.java)

    init {
        type = "TestMonolithItem"
    }

    @State
    @Mutable
    var label: String = ""

    @State
    @Mutable
    // Test enum behavior
    var groupIdentifier: TestMonolithItemGroupIdentifier = TestMonolithItemGroupIdentifier.NONE

    @State
    @Mutable
    // Enum when serialized as Set
    var lockedToGroups: Set<TestMonolithItemGroupIdentifier> = mutableSetOf()

    @State
    @Mutable
    // Change these several times in quick succession
    var x: Int = 0
    var y: Int = 0

    @State
    @Mutable
    // Ensure this behaves like a list across the lifecycle
    var listOfNotes: MutableList<String> = mutableListOf()

    @State
    @Mutable
    // Ensure this behaves like a list across the lifecycle
    var dataCollection: Map<String, TestMonolithItemDataClass> = mutableMapOf()

    @State
    @Mutable
    @Parent
    // Test this with and without an Entity reference
    var ancestor: TestMonolithItemAncestor? = null

    @State
    @Mutable
    // Choice of Channel doesn't matter, just to interact with shared resources
    var cachedItems: Map<String, Channel> = mutableMapOf()
}

data class TestMonolithItemDataClass(
    var id1: String = UUID.randomUUID().toString(),
    var id2: String = UUID.randomUUID().toString(),
    var id3: String = UUID.randomUUID().toString(),
    var id4: String = UUID.randomUUID().toString(),
    var id5: String = UUID.randomUUID().toString()
): Serializable

enum class TestMonolithItemGroupIdentifier {
    NONE,
    CYAN,
    MAGENTA,
    YELLOW
}