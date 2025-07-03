package com.minare.operation

import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.kafka.client.producer.KafkaProducer
import io.vertx.kafka.client.producer.KafkaProducerRecord
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Kafka implementation of MessageQueue.
 * Sends Operations to Kafka topics for processing by consumers.
 */
@Singleton
class KafkaMessageQueue @Inject constructor(
    private val vertx: Vertx
) : MessageQueue {

    private val log = LoggerFactory.getLogger(KafkaMessageQueue::class.java)

    // Lazy initialize the producer
    private val producer: KafkaProducer<String, String> by lazy {
        createProducer()
    }

    private fun createProducer(): KafkaProducer<String, String> {
        val config = mutableMapOf<String, String>()

        // Bootstrap servers from environment or default to localhost
        config["bootstrap.servers"] = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092"

        // Producer configuration
        config["key.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"
        config["value.serializer"] = "org.apache.kafka.common.serialization.StringSerializer"

        // Performance settings for fire-and-forget
        config["acks"] = "0"  // Don't wait for acknowledgment
        config["retries"] = "0"  // Don't retry failed sends
        config["linger.ms"] = "10"  // Small batching window
        config["compression.type"] = "snappy"  // Compress messages

        // Client ID for debugging
        config["client.id"] = "minare-operation-producer"

        log.info("Creating Kafka producer with bootstrap servers: {}", config["bootstrap.servers"])

        return KafkaProducer.create(vertx, config)
    }

    override suspend fun send(topic: String, message: JsonArray) {
        try {
            val record = KafkaProducerRecord.create<String, String>(
                topic,
                message.toString()
            )

            // Fire and forget - don't wait for result
            producer.send(record)

            log.debug("Sent message to Kafka topic {}: {}", topic, message)

        } catch (e: Exception) {
            log.error("Failed to send message to Kafka topic {}", topic, e)
            // For fire-and-forget, we just log the error
        }
    }

    override suspend fun send(topic: String, key: String, message: JsonArray) {
        try {
            val record = KafkaProducerRecord.create<String, String>(
                topic,
                key,
                message.toString()
            )

            // Fire and forget - don't wait for result
            producer.send(record)

            log.debug("Sent keyed message to Kafka topic {} with key {}: {}", topic, key, message)

        } catch (e: Exception) {
            log.error("Failed to send keyed message to Kafka topic {} with key {}", topic, key, e)
            // For fire-and-forget, we just log the error
        }
    }
}