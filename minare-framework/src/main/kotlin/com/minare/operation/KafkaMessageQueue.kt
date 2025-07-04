package com.minare.operation

import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.kafka.admin.KafkaAdminClient
import io.vertx.kafka.admin.NewTopic
import io.vertx.kafka.client.producer.KafkaProducer
import io.vertx.kafka.client.producer.KafkaProducerRecord
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Production-ready Kafka implementation of MessageQueue.
 * Handles topic creation, configuration, and reliable message sending.
 */
@Singleton
class KafkaMessageQueue @Inject constructor(
    private val vertx: Vertx
) : MessageQueue {

    private val log = LoggerFactory.getLogger(KafkaMessageQueue::class.java)

    // Configuration
    private val bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092"
    private val topicPartitions = System.getenv("KAFKA_TOPIC_PARTITIONS")?.toInt() ?: 3
    private val topicReplicationFactor = System.getenv("KAFKA_TOPIC_REPLICATION_FACTOR")?.toShort() ?: 1
    private val producerRetries = System.getenv("KAFKA_PRODUCER_RETRIES")?.toInt() ?: 3
    private val producerAcks = System.getenv("KAFKA_PRODUCER_ACKS") ?: "1"
    private val producerCompressionType = System.getenv("KAFKA_PRODUCER_COMPRESSION") ?: "snappy"
    private val producerLingerMs = System.getenv("KAFKA_PRODUCER_LINGER_MS") ?: "10"
    private val producerBatchSize = System.getenv("KAFKA_PRODUCER_BATCH_SIZE") ?: "16384"

    // Track initialized topics to avoid repeated creation attempts
    private val initializedTopics = mutableSetOf<String>()
    private val initMutex = Mutex()

    // Lazy initialize producers and admin client
    private val producer: KafkaProducer<String, String> by lazy {
        createProducer()
    }

    private val adminClient: KafkaAdminClient by lazy {
        createAdminClient()
    }

    init {
        log.info("Initializing KafkaMessageQueue with bootstrap servers: {}", bootstrapServers)
        log.info("Producer configuration - acks: {}, retries: {}, compression: {}",
            producerAcks, producerRetries, producerCompressionType)
    }

    private fun createProducer(): KafkaProducer<String, String> {
        val config = mutableMapOf<String, String>()

        // Connection
        config["bootstrap.servers"] = bootstrapServers

        // Serialization
        config["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        config["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"

        // Reliability settings
        config["acks"] = producerAcks  // 0=none, 1=leader, all=all replicas
        config["retries"] = producerRetries.toString()
        config["max.in.flight.requests.per.connection"] = "5"
        // Only enable idempotence if acks=all
        config["enable.idempotence"] = (producerAcks == "all").toString()

        // Performance settings
        config["compression.type"] = producerCompressionType
        config["linger.ms"] = producerLingerMs
        config["batch.size"] = producerBatchSize
        config["buffer.memory"] = "33554432"  // 32MB

        // Timeout settings
        config["request.timeout.ms"] = "30000"
        config["delivery.timeout.ms"] = "120000"

        // Client identification
        config["client.id"] = "minare-producer-${System.getenv("HOSTNAME") ?: "unknown"}"

        log.debug("Creating Kafka producer with config: {}", config)

        return KafkaProducer.create(vertx, config)
    }

    private fun createAdminClient(): KafkaAdminClient {
        val config = mutableMapOf<String, String>()
        config["bootstrap.servers"] = bootstrapServers
        config["client.id"] = "minare-admin-${System.getenv("HOSTNAME") ?: "unknown"}"
        config["request.timeout.ms"] = "30000"

        log.debug("Creating Kafka admin client")

        return KafkaAdminClient.create(vertx, config)
    }

    /**
     * Ensure topic exists before sending messages
     */
    private suspend fun ensureTopicExists(topic: String) {
        // Quick check without lock
        if (initializedTopics.contains(topic)) {
            return
        }

        // Acquire lock for initialization
        initMutex.withLock {
            // Double-check inside lock
            if (initializedTopics.contains(topic)) {
                return
            }

            try {
                log.info("Checking if topic {} exists", topic)

                // List existing topics
                val topics = adminClient.listTopics().await()

                if (topics.contains(topic)) {
                    log.debug("Topic {} already exists", topic)
                    initializedTopics.add(topic)
                    return
                }

                // Create topic
                log.info("Creating topic {} with {} partitions and replication factor {}",
                    topic, topicPartitions, topicReplicationFactor)

                val newTopic = NewTopic(topic, topicPartitions, topicReplicationFactor).apply {
                    // Topic configuration
                    config = mapOf(
                        "retention.ms" to (7 * 24 * 60 * 60 * 1000).toString(), // 7 days
                        "segment.ms" to (60 * 60 * 1000).toString(), // 1 hour
                        "compression.type" to "producer", // Use producer's compression
                        "min.insync.replicas" to "1"
                    )
                }

                adminClient.createTopics(listOf(newTopic)).await()

                log.info("Successfully created topic {}", topic)
                initializedTopics.add(topic)

            } catch (e: Exception) {
                // Topic might have been created by another instance
                if (e.message?.contains("already exists") == true) {
                    log.debug("Topic {} was created by another instance", topic)
                    initializedTopics.add(topic)
                } else {
                    log.error("Failed to create topic {}", topic, e)
                    throw e
                }
            }
        }
    }

    override suspend fun send(topic: String, message: JsonArray) {
        try {
            // Ensure topic exists
            ensureTopicExists(topic)

            val record = KafkaProducerRecord.create<String, String>(
                topic,
                message.toString()
            )

            // Add headers for debugging/tracing
            record.addHeader("produced-by", "minare")
            record.addHeader("produced-at", System.currentTimeMillis().toString())

            if (log.isDebugEnabled) {
                log.debug("Sending message to topic {}: {}", topic, message.encodePrettily())
            }

            // Send with callback for production monitoring
            producer.send(record) { asyncResult ->
                if (asyncResult.failed()) {
                    log.error("Failed to send message to topic {} - {}", topic, asyncResult.cause().message, asyncResult.cause())
                    // In production, you might want to send this to a metrics system
                } else {
                    val metadata = asyncResult.result()
                    if (log.isTraceEnabled) {
                        log.trace("Message sent to topic {} partition {} offset {}",
                            metadata.topic, metadata.partition, metadata.offset)
                    }
                }
            }

        } catch (e: Exception) {
            log.error("Error sending message to topic {}", topic, e)
            // In production, you might want to send to a DLQ or metrics system
            throw e
        }
    }

    override suspend fun send(topic: String, key: String, message: JsonArray) {
        try {
            // Ensure topic exists
            ensureTopicExists(topic)

            val record = KafkaProducerRecord.create<String, String>(
                topic,
                key,
                message.toString()
            )

            // Add headers
            record.addHeader("produced-by", "minare")
            record.addHeader("produced-at", System.currentTimeMillis().toString())
            record.addHeader("message-key", key)

            if (log.isDebugEnabled) {
                log.debug("Sending keyed message to topic {} with key {}: {}",
                    topic, key, message.encodePrettily())
            }

            // Send with callback
            producer.send(record) { asyncResult ->
                if (asyncResult.failed()) {
                    log.error("Failed to send keyed message to topic {} with key {} - {}",
                        topic, key, asyncResult.cause().message, asyncResult.cause())
                } else {
                    val metadata = asyncResult.result()
                    if (log.isTraceEnabled) {
                        log.trace("Keyed message sent to topic {} partition {} offset {} with key {}",
                            metadata.topic, metadata.partition, metadata.offset, key)
                    }
                }
            }

        } catch (e: Exception) {
            log.error("Error sending keyed message to topic {} with key {}", topic, key, e)
            throw e
        }
    }

    /**
     * Graceful shutdown - flush any pending messages
     */
    suspend fun close() {
        try {
            log.info("Closing Kafka producer")
            producer.flush().await()
            producer.close().await()
            adminClient.close().await()
        } catch (e: Exception) {
            log.error("Error during Kafka client shutdown", e)
        }
    }
}