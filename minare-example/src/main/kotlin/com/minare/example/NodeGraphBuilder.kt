package com.minare.example.core.models

import com.minare.core.entity.EntityFactory
import com.minare.example.models.Node
import com.minare.persistence.EntityStore
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.graph.SimpleDirectedGraph
import org.slf4j.LoggerFactory
import java.util.*
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.math.max
import kotlin.math.min
import kotlin.random.Random

/**
 * Utility class for building interesting node graph structures
 */
@Singleton
class NodeGraphBuilder @Inject constructor(
    private val entityFactory: EntityFactory,
    private val entityStore: EntityStore
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
        // Use JGraphT to model the tree during construction
        val graph = SimpleDirectedGraph<Node, DefaultEdge>(DefaultEdge::class.java)
        val allNodes = mutableListOf<Node>()

        // Setup random with optional seed for reproducibility
        val random = randomSeed?.let { Random(it) } ?: Random

        // Create root node
        val rootNode = entityFactory.createEntity(Node::class.java) as Node
        rootNode.label = "Root"

        // Save the root node first to get a MongoDB-assigned ID
        try {
            val savedRootNode = entityStore.save(rootNode) as Node
            log.debug("Created root node with ID: ${savedRootNode._id}")

            graph.addVertex(savedRootNode)
            allNodes.add(savedRootNode)

            // Track nodes at each level for breadth-first generation
            val nodesByLevel = mutableMapOf<Int, MutableList<Node>>().apply {
                put(0, mutableListOf(savedRootNode))
            }

            var nodesCreated = 1
            val nodesToCreate = min(totalNodes, 1000) // Cap at 1000 for safety

            // Generate the tree level by level
            for (level in 0 until maxDepth) {
                val parentNodes = nodesByLevel[level] ?: break
                val childNodes = mutableListOf<Node>()

                // For each parent node at this level
                for (parent in parentNodes) {
                    // Calculate how many children this node should have
                    // More variation at deeper levels, less at the top
                    val levelFactor = 1.0 - (level.toDouble() / maxDepth)
                    val baseBranchingFactor = max(1, (maxBranchingFactor * levelFactor).toInt())
                    val branchingFactor = random.nextInt(1, baseBranchingFactor + 1)

                    // Create children for this parent
                    repeat(branchingFactor) { index ->
                        if (nodesCreated >= nodesToCreate) return@repeat

                        // Create a new child node
                        val child = entityFactory.createEntity(Node::class.java) as Node
                        child.label = "${parent.label}-${index + 1}"

                        // Save the child to get a MongoDB-assigned ID
                        val savedChild = entityStore.save(child) as Node
                        log.debug("Created child node with ID: ${savedChild._id}")

                        // Now that both parent and child have IDs, establish the relationship
                        // Important: We must modify the actual saved entities, not the original references
                        parent.addChild(savedChild)

                        // We'll only update the parent entity since the child's references were just created
                        // and don't need another update
                        try {
                            log.debug("BEFORE saving parent: ${parent._id}")
                            val updatedParent = entityStore.save(parent) as Node
                            log.debug("AFTER saving parent: ${updatedParent._id}")

                            // Update our references to use the latest versions
                            graph.addVertex(savedChild)
                            graph.addEdge(updatedParent, savedChild)

                            // If parent isn't in the graph yet (shouldn't happen, but just in case)
                            if (!graph.containsVertex(updatedParent)) {
                                graph.addVertex(updatedParent)
                            }

                            // Add the child to our tracking collections
                            childNodes.add(savedChild)
                            allNodes.add(savedChild)
                        } catch (e: Exception) {
                            log.error("Failed to update parent-child relationship: ${e.message}")
                            // We'll still keep the child node in our collection even if the relationship update fails
                            graph.addVertex(savedChild)
                            childNodes.add(savedChild)
                            allNodes.add(savedChild)
                        }

                        nodesCreated++
                    }
                }

                // Store the children for the next level
                if (childNodes.isNotEmpty()) {
                    nodesByLevel[level + 1] = childNodes
                } else {
                    // No more children at this level, we're done
                    break
                }
            }

            log.info("Created asymmetric tree with $nodesCreated nodes (target: $totalNodes)")

            // Output tree statistics
            outputTreeStats(graph, savedRootNode)

            return Pair(savedRootNode, allNodes)

        } catch (e: Exception) {
            log.error("Failed to create root node: ${e.message}")
            // Return an empty result since we couldn't even create the root
            return Pair(rootNode, emptyList())
        }
    }

    /**
     * Output statistics about the generated tree
     */
    private fun outputTreeStats(graph: Graph<Node, DefaultEdge>, rootNode: Node) {
        val depths = mutableMapOf<Int, Int>() // depth -> count
        val branchingFactors = mutableListOf<Int>()

        // Calculate statistics using BFS
        val queue = LinkedList<Pair<Node, Int>>() // (node, depth)
        queue.add(rootNode to 0)
        val visited = mutableSetOf<String>()
        rootNode._id?.let { visited.add(it) }

        while (queue.isNotEmpty()) {
            val (node, depth) = queue.poll()

            // Count nodes at this depth
            depths[depth] = (depths[depth] ?: 0) + 1

            // Count outgoing edges (branching factor)
            val outDegree = graph.outgoingEdgesOf(node).size
            if (outDegree > 0) {
                branchingFactors.add(outDegree)
            }

            // Add children to queue
            graph.outgoingEdgesOf(node).forEach { edge ->
                val child = graph.getEdgeTarget(edge)
                child._id?.let { childId ->
                    if (childId !in visited) {
                        visited.add(childId)
                        queue.add(child to depth + 1)
                    }
                }
            }
        }

        // Output statistics
        log.info("Tree depth: ${depths.keys.maxOrNull() ?: 0}")
        log.info("Nodes per level: ${depths.entries.sortedBy { it.key }.joinToString { "${it.key}=${it.value}" }}")

        val avgBranchingFactor = if (branchingFactors.isNotEmpty()) {
            branchingFactors.average()
        } else {
            0.0
        }
        log.info("Average branching factor: %.2f".format(avgBranchingFactor))
    }
}