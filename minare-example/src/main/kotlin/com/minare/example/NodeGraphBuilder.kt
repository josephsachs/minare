package com.minare.example.core.models

import com.minare.core.entity.factories.EntityFactory
import com.minare.example.models.Node
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.graph.SimpleDirectedGraph
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton
import com.minare.controller.EntityController
import io.vertx.core.json.JsonObject
import kotlin.math.max
import kotlin.math.min
import kotlin.random.Random

/**
 * Utility class for building interesting node graph structures
 */
@Singleton
class NodeGraphBuilder @Inject constructor(
    private val entityFactory: EntityFactory,
    private val entityController: EntityController
) {
    private val log = LoggerFactory.getLogger(NodeGraphBuilder::class.java)

    /**
     * Build an asymmetric tree structure with the given parameters
     *
     * @param maxDepth Maximum depth of the tree
     * @param maxBranchingFactor Maximum number of children a node can have
     * @param totalNodes Approximate total number of nodes to generate
     * @param randomSeed Optional seed for reproducible random generation
     * @return The root node of the generated tree and a list of all nodes
     */
    suspend fun buildAsymmetricTree(
        maxDepth: Int = 5,
        maxBranchingFactor: Int = 5,
        totalNodes: Int = 100,
        randomSeed: Long? = null
    ): Pair<Node, List<Node>> {
        val graph = SimpleDirectedGraph<Node, DefaultEdge>(DefaultEdge::class.java)
        val allNodes = mutableListOf<Node>()
        val random = randomSeed?.let { Random(it) } ?: Random
        val rootNode = entityFactory.createEntity(Node::class.java) as Node
        rootNode.label = "Root"

        try {
            val savedRootNode = entityController.create(rootNode) as Node

            log.debug("Created root node with ID: ${savedRootNode._id}")

            graph.addVertex(savedRootNode)
            allNodes.add(savedRootNode)

            val nodesByLevel = mutableMapOf<Int, MutableList<Node>>().apply {
                put(0, mutableListOf(savedRootNode))
            }

            var nodesCreated = 1
            val nodesToCreate = min(totalNodes, 1000)

            for (level in 0 until maxDepth) {
                val parentNodes = nodesByLevel[level] ?: break
                val childNodes = mutableListOf<Node>()

                for (parent in parentNodes) {
                    val levelFactor = 1.0 - (level.toDouble() / maxDepth)
                    val baseBranchingFactor = max(1, (maxBranchingFactor * levelFactor).toInt())
                    val branchingFactor = random.nextInt(1, baseBranchingFactor + 1)

                    repeat(branchingFactor) { index ->
                        if (nodesCreated >= nodesToCreate) return@repeat

                        val child = entityFactory.createEntity(Node::class.java) as Node
                        child.label = "${parent.label}-${index + 1}"

                        val savedChild = entityController.create(child) as Node

                        parent.addChild(savedChild)

                        try {
                            val delta = JsonObject().put("childIds", savedChild._id)

                            val updatedParent = entityController.saveState(parent._id!!, delta, false) as Node

                            graph.addVertex(savedChild)
                            graph.addEdge(updatedParent, savedChild)

                            if (!graph.containsVertex(updatedParent)) {
                                graph.addVertex(updatedParent)
                            }

                            childNodes.add(savedChild)
                            allNodes.add(savedChild)
                        } catch (e: Exception) {
                            log.error("Failed to update parent-child relationship: ${e.message}")
                            graph.addVertex(savedChild)
                            childNodes.add(savedChild)
                            allNodes.add(savedChild)
                        }

                        nodesCreated++
                    }
                }

                if (childNodes.isNotEmpty()) {
                    nodesByLevel[level + 1] = childNodes
                } else {
                    break
                }
            }

            log.info("Created asymmetric tree with $nodesCreated nodes (target: $totalNodes)")

            return Pair(savedRootNode, allNodes)

        } catch (e: Exception) {
            log.error("Failed to create root node: ${e.message}")
            return Pair(rootNode, emptyList())
        }
    }
}