package com.minare.example

import com.minare.MinareApplication
import com.minare.persistence.EntityStore
import io.vertx.core.Future
import io.vertx.ext.web.Router
import org.slf4j.LoggerFactory
import javax.inject.Inject

class ExampleApplication @Inject constructor(
    private val entityStore: EntityStore
) : MinareApplication() {
    fun initializeNodeGraph(): Future<Void> {
        // Create nodes with only label and value
        val nodeA = Node(label = "A", value = 1)
        val nodeB = Node(label = "B", value = 2)
        val nodeC = Node(label = "C", value = 3)
        val nodeD = Node(label = "D", value = 4)
        val nodeE = Node(label = "E", value = 5)
        val nodeF = Node(label = "F", value = 6)
        val nodeG = Node(label = "G", value = 7)
        val nodeH = Node(label = "H", value = 8)
        val nodeI = Node(label = "I", value = 9)
        val nodeJ = Node(label = "J", value = 10)

        // First save all nodes to generate IDs
        return entityStore.save(nodeA)
            .compose { entityStore.save(nodeB) }
            .compose { entityStore.save(nodeC) }
            .compose { entityStore.save(nodeD) }
            .compose { entityStore.save(nodeE) }
            .compose { entityStore.save(nodeF) }
            .compose { entityStore.save(nodeG) }
            .compose { entityStore.save(nodeH) }
            .compose { entityStore.save(nodeI) }
            .compose { entityStore.save(nodeJ) }
            .compose {
                // Now build the relationships and update
                nodeA.addChild(nodeB)
                nodeA.addChild(nodeC)

                nodeB.addChild(nodeD)
                nodeB.addChild(nodeE)

                nodeC.addChild(nodeF)

                nodeE.addChild(nodeG)
                nodeE.addChild(nodeH)

                nodeF.addChild(nodeI)
                nodeF.addChild(nodeJ)

                // Save the updated nodes
                entityStore.save(nodeA)
                    .compose { entityStore.save(nodeB) }
                    .compose { entityStore.save(nodeC) }
                    .compose { entityStore.save(nodeE) }
                    .compose { entityStore.save(nodeF) }
                    .mapEmpty()
            }
    }

    override fun onStart(): Future<Void> {
        return initializeNodeGraph()
            .onSuccess { log.info("Node graph successfully initialized") }
            .onFailure { err -> log.error("Failed to initialize node graph", err) }
    }
}