package com.minare.integration.models

import com.google.inject.Inject
import com.minare.core.entity.annotations.*
import com.minare.core.entity.models.Entity
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.io.Serializable

@EntityType("TestEntityCollection")
class TestEntityCollection @Inject constructor(
    private val vertx: Vertx
): Entity() {
    private val log = LoggerFactory.getLogger(TestEntityCollection::class.java)

    init {
        type = "TestEntityCollection"
    }

    @State
    var label: String = ""

    @State
    @Child
    @Mutable
    var collection: MutableList<TestEntityItem> = mutableListOf()

    @State
    @Mutable
    var noise: MutableMap<String, String> = mutableMapOf()

    fun addItem(item: TestEntityItem) {
        item._id?.let { itemId ->
            if (!collection.any({ it._id == itemId })) {
                collection.add(item)
            }
        }
    }

    @Task
    fun tick() {
        vertx.eventBus().publish(ADDRESS_TEST_ENTITY_ITEM_TICK,
            JsonObject()
                .put("id", _id)
                .put("type", "TestEntityCollection")
                .put("label", label)
                .put("collection", collection)
                .put("noise", noise.toString())
        )
    }

    companion object {
        const val ADDRESS_TEST_ENTITY_ITEM_TICK = "address.testentityitem.tick"
    }
}