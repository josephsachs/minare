package com.minare.example

import com.minare.MinareApplication
import com.minare.example.core.models.Node
import com.minare.persistence.EntityStore
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import javax.inject.Inject

class ExampleApplication @Inject constructor(
    private val entityStore: EntityStore
) : MinareApplication() {

    suspend fun initializeNodeGraph() {
        withContext(Dispatchers.IO) {
            try {
                // Create nodes
                val nodeA = Node().apply { label = "A"; value = 1 }
                val nodeB = Node().apply { label = "B"; value = 2 }
                val nodeC = Node().apply { label = "C"; value = 3 }
                val nodeD = Node().apply { label = "D"; value = 4 }
                val nodeE = Node().apply { label = "E"; value = 5 }
                val nodeF = Node().apply { label = "F"; value = 6 }
                val nodeG = Node().apply { label = "G"; value = 7 }
                val nodeH = Node().apply { label = "H"; value = 8 }
                val nodeI = Node().apply { label = "I"; value = 9 }
                val nodeJ = Node().apply { label = "J"; value = 10 }

                // First batch: Save all nodes to generate IDs
                saveAllNodes(listOf(nodeA, nodeB, nodeC, nodeD, nodeE, nodeF, nodeG, nodeH, nodeI, nodeJ))

                // Build the relationships
                nodeA.addChild(nodeB)
                nodeA.addChild(nodeC)
                nodeB.addChild(nodeD)
                nodeB.addChild(nodeE)
                nodeC.addChild(nodeF)
                nodeE.addChild(nodeG)
                nodeE.addChild(nodeH)
                nodeF.addChild(nodeI)
                nodeF.addChild(nodeJ)

                // Save nodes with relationships
                saveAllNodes(listOf(nodeA, nodeB, nodeC, nodeE, nodeF))
            } catch (e: Exception) {
                //log.error("Error initializing node graph", e)
                throw e
            }
        }
    }

    private suspend fun saveAllNodes(nodes: List<Node>) {
        for (node in nodes) {
            entityStore.save(node).await()
        }
    }

    fun onStart(): Future<Void> {
        val promise = Promise.promise<Void>()

        CoroutineScope(vertx.dispatcher()).launch {
            try {
                initializeNodeGraph()
                //log.info("Node graph successfully initialized")
                promise.complete()
            } catch (e: Exception) {
                //log.error("Failed to initialize node graph", e)
                promise.fail(e)
            }
        }

        return promise.future()
    }
}