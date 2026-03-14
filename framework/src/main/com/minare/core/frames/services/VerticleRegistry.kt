package com.minare.core.frames.services

import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import com.google.inject.Inject
import com.google.inject.Singleton

/**
 * Manages the registry of frame worker verticle instances in the cluster.
 * This is the source of truth for instance-level frame routing: which instances
 * exist, which are active, and which should receive manifests.
 *
 * Verticles self-register at start() with their instanceId. The frame system
 * (manifest builder, completion tracker, affinity resolver) consumes
 * getActiveInstances() to determine routing targets.
 *
 * Separate from WorkerRegistry, which handles node-level fleet bootstrap
 * (infra-registered by HOSTNAME, used for readiness checks).
 */
@Singleton
class VerticleRegistry @Inject constructor(
    private val verticleRegistryMap: VerticleRegistryMap,
    private val activeVerticleSet: ActiveVerticleSet
) {
    private val log = LoggerFactory.getLogger(VerticleRegistry::class.java)

    data class InstanceState(
        val instanceId: String,
        val nodeId: String = "",
        val status: InstanceStatus,
        val lastHeartbeat: Long = System.currentTimeMillis(),
        val addedAt: Long = System.currentTimeMillis()
    ) {
        fun toJson(): JsonObject {
            return JsonObject()
                .put("instanceId", instanceId)
                .put("nodeId", nodeId)
                .put("status", status.name)
                .put("lastHeartbeat", lastHeartbeat)
                .put("addedAt", addedAt)
        }

        companion object {
            fun fromJson(json: JsonObject): InstanceState {
                return InstanceState(
                    instanceId = json.getString("instanceId"),
                    nodeId = json.getString("nodeId") ?: "",
                    status = InstanceStatus.valueOf(json.getString("status")),
                    lastHeartbeat = json.getLong("lastHeartbeat"),
                    addedAt = json.getLong("addedAt")
                )
            }
        }
    }

    enum class InstanceStatus {
        PENDING,      // Registered but not yet active
        ACTIVE,       // Participating in frames
        UNHEALTHY,    // Missed frame completions
        REMOVING      // Scheduled for removal
    }

    /**
     * Register a new verticle instance. Called by FrameWorkerVerticle at start().
     * Starts in PENDING state — activation happens when the coordinator accepts it.
     */
    fun addInstance(instanceId: String, nodeId: String) {
        log.info("Adding instance {} (node: {}) to verticle registry", instanceId, nodeId)
        val state = InstanceState(
            instanceId = instanceId,
            nodeId = nodeId,
            status = InstanceStatus.PENDING
        )
        verticleRegistryMap.put(instanceId, state.toJson())
    }

    /**
     * Activate an instance. Transitions from PENDING to ACTIVE.
     * @return true if activation was successful
     */
    fun activateInstance(instanceId: String): Boolean {
        val json = verticleRegistryMap.get(instanceId)
        val state = json?.let { InstanceState.fromJson(it) }

        return when {
            state == null -> {
                log.warn("Unknown instance {} attempted activation", instanceId)
                false
            }
            state.status == InstanceStatus.PENDING -> {
                log.info("Instance {} activated successfully", instanceId)
                verticleRegistryMap.put(instanceId, state.copy(
                    status = InstanceStatus.ACTIVE,
                    lastHeartbeat = System.currentTimeMillis()
                ).toJson())
                activeVerticleSet.put(instanceId)
                true
            }
            state.status == InstanceStatus.ACTIVE -> {
                log.debug("Instance {} already active, updating heartbeat", instanceId)
                verticleRegistryMap.put(instanceId, state.copy(
                    lastHeartbeat = System.currentTimeMillis()
                ).toJson())
                activeVerticleSet.put(instanceId)
                true
            }
            else -> {
                log.warn("Instance {} attempted activation in state {}", instanceId, state.status)
                false
            }
        }
    }

    /**
     * Schedule an instance for removal.
     */
    fun scheduleRemoval(instanceId: String) {
        log.info("Scheduling removal of instance {}", instanceId)
        val json = verticleRegistryMap.get(instanceId)
        if (json != null) {
            val state = InstanceState.fromJson(json)
            verticleRegistryMap.put(instanceId, state.copy(status = InstanceStatus.REMOVING).toJson())
            activeVerticleSet.remove(instanceId)
        }
    }

    /**
     * Update instance heartbeat.
     */
    fun updateHeartbeat(instanceId: String) {
        val json = verticleRegistryMap.get(instanceId) ?: return
        val state = InstanceState.fromJson(json)
        verticleRegistryMap.put(instanceId, state.copy(
            lastHeartbeat = System.currentTimeMillis()
        ).toJson())
    }

    /**
     * Get set of active instance IDs. Used by the frame system for
     * manifest distribution, completion tracking, and affinity resolution.
     */
    fun getActiveInstances(): Set<String> {
        return activeVerticleSet.entries()
    }

    /**
     * Get distinct active nodes (by nodeId).
     * Used for fleet-level bootstrap readiness checks.
     */
    fun getActiveNodes(): Set<String> {
        return verticleRegistryMap.values()
            .map { InstanceState.fromJson(it) }
            .filter { it.status == InstanceStatus.ACTIVE && it.nodeId.isNotEmpty() }
            .map { it.nodeId }
            .toSet()
    }

    /**
     * Get instance state.
     */
    fun getInstanceState(instanceId: String): InstanceState? {
        val json = verticleRegistryMap.get(instanceId)
        return json?.let { InstanceState.fromJson(it) }
    }

    /**
     * Get all instances and their states (for monitoring).
     */
    fun getAllInstances(): Map<String, InstanceState> {
        return verticleRegistryMap.entries().associate {
            it.key to InstanceState.fromJson(it.value)
        }
    }

    /**
     * Check if a specific instance is healthy and active.
     */
    fun isInstanceHealthy(instanceId: String): Boolean {
        val json = verticleRegistryMap.get(instanceId)
        return json?.let { InstanceState.fromJson(it).status == InstanceStatus.ACTIVE } ?: false
    }

    /**
     * Get expected instance count (not removing).
     */
    fun getExpectedInstanceCount(): Int {
        return getAllInstances().count {
            it.value.status != InstanceStatus.REMOVING
        }
    }

    /**
     * Remove an instance immediately.
     */
    fun removeInstanceImmediately(instanceId: String): Boolean {
        val removed = verticleRegistryMap.remove(instanceId)
        activeVerticleSet.remove(instanceId)
        if (removed != null) {
            log.warn("Instance {} removed immediately from verticle registry", instanceId)
            return true
        }
        return false
    }

    /**
     * Reset the registry (useful for testing).
     */
    fun reset() {
        verticleRegistryMap.clear()
        activeVerticleSet.clear()
        log.info("Verticle registry reset")
    }
}
