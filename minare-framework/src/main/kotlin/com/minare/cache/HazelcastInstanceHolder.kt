package com.minare.cache

import com.hazelcast.core.HazelcastInstance
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import java.util.concurrent.atomic.AtomicReference

/**
 * Singleton holder for the Hazelcast instance used by Vert.x clustering.
 * This ensures we use a single Hazelcast instance throughout the application
 * instead of creating multiple instances.
 */
object HazelcastInstanceHolder {
    private val clusterManagerRef = AtomicReference<HazelcastClusterManager>()

    /**
     * Sets the cluster manager. Should be called once during application startup.
     *
     * @param manager The HazelcastClusterManager created for Vert.x clustering
     * @throws IllegalStateException if called more than once
     */
    fun setClusterManager(manager: HazelcastClusterManager) {
        if (!clusterManagerRef.compareAndSet(null, manager)) {
            throw IllegalStateException("HazelcastClusterManager has already been set")
        }
    }

    /**
     * Gets the Hazelcast instance from the cluster manager.
     *
     * @return The single Hazelcast instance used by the application
     * @throws IllegalStateException if the cluster manager hasn't been initialized
     */
    fun getInstance(): HazelcastInstance {
        val manager = clusterManagerRef.get()
            ?: throw IllegalStateException(
                "HazelcastClusterManager not initialized. " +
                        "Ensure MinareApplication.start() has been called."
            )

        return manager.hazelcastInstance
    }

    /**
     * Checks if the cluster manager has been initialized.
     */
    fun isInitialized(): Boolean = clusterManagerRef.get() != null

    /**
     * Gets the cluster manager itself (for advanced use cases).
     *
     * @return The HazelcastClusterManager or null if not initialized
     */
    fun getClusterManager(): HazelcastClusterManager? = clusterManagerRef.get()

    /**
     * Resets the holder. Should only be used in tests.
     */
    internal fun reset() {
        clusterManagerRef.set(null)
    }
}