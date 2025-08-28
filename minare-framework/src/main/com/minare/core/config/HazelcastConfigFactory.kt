package com.minare.core.config

import com.hazelcast.config.Config
import com.hazelcast.config.SerializerConfig
import com.minare.core.utils.json.JsonObjectSerializer
import io.vertx.core.json.JsonObject
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import org.slf4j.LoggerFactory

/**
 * Factory for creating configured Hazelcast cluster managers.
 * Centralizes all Hazelcast configuration including serialization setup.
 */
object HazelcastConfigFactory {
    private val log = LoggerFactory.getLogger(HazelcastConfigFactory::class.java)

    /**
     * Creates a HazelcastClusterManager with all necessary configuration,
     * including JsonObject serialization support.
     */
    fun createConfiguredClusterManager(): HazelcastClusterManager {
        val config = Config()

        // Set cluster name from environment
        config.clusterName = System.getenv("HAZELCAST_CLUSTER_NAME") ?: "minare-cluster"
        log.info("Configuring Hazelcast cluster: ${config.clusterName}")

        // Configure JsonObject serialization
        configureJsonObjectSerialization(config)

        // Add any other Hazelcast configuration here
        // For example:
        // - Network configuration
        // - Map configurations
        // - Partition group configuration
        // - Near cache configuration

        return HazelcastClusterManager(config)
    }

    /**
     * Configures JsonObject serialization for Hazelcast.
     */
    private fun configureJsonObjectSerialization(config: Config) {
        val jsonObjectSerializerConfig = SerializerConfig()
            .setImplementation(JsonObjectSerializer())
            .setTypeClass(JsonObject::class.java)

        config.serializationConfig.addSerializerConfig(jsonObjectSerializerConfig)

        log.info("Configured JsonObject serializer with type ID: ${JsonObjectSerializer.TYPE_ID}")
    }

    /**
     * Creates a standalone Hazelcast configuration for testing or other purposes.
     * Uses the same configuration as the cluster manager.
     */
    fun createStandaloneConfig(): Config {
        val config = Config()
        config.clusterName = System.getenv("HAZELCAST_CLUSTER_NAME") ?: "minare-cluster"
        configureJsonObjectSerialization(config)
        return config
    }
}