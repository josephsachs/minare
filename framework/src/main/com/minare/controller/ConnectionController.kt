package com.minare.controller

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.application.config.FrameworkConfig
import com.minare.core.storage.interfaces.ChannelStore
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.transport.models.Connection
import com.minare.core.transport.upsocket.UpSocketVerticle
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

@Singleton
open class ConnectionController @Inject constructor() {
    @Inject private lateinit var vertx: Vertx
    @Inject private lateinit var frameworkConfig: FrameworkConfig
    @Inject private lateinit var connectionStore: ConnectionStore
    @Inject private lateinit var channelStore: ChannelStore

    private val log = LoggerFactory.getLogger(ConnectionController::class.java)

    suspend fun createConnection(): Connection {
        return connectionStore.create()
    }

    suspend fun getConnection(connectionId: String): Connection {
        return connectionStore.find(connectionId)
    }

    suspend fun hasConnection(connectionId: String): Boolean {
        return connectionStore.exists(connectionId)
    }

    /**
     * Override to provide auth and preflight on all incoming socket messages to the upsocket
     */
    open suspend fun onConnectionAttempt(message: String): Boolean {
        return true
    }

    open suspend fun onConnected(connection: Connection) {
        log.info("Client {} is now fully connected", connection.id)
    }

    suspend fun sendToUpSocket(connectionId: String, message: JsonObject) {
        try {
            val connection = connectionStore.find(connectionId)
            val instanceId = connection.upSocketInstanceId

            if (instanceId == null) {
                log.warn("No upSocketDeploymentId for connection {}, cannot send message", connectionId)
                return
            }
            vertx.eventBus().send(
                "${UpSocketVerticle.ADDRESS_SEND_TO_CONNECTION}.${instanceId}",
                JsonObject()
                    .put("connectionId", connectionId)
                    .put("message", message)
            )
        } catch (e: Exception) {
            log.warn("Failed to send message to connection {}: {}", connectionId, e.message)
        }
    }

    suspend fun cleanupConnection(connectionId: String) {
        try {
            channelStore.removeClientFromAllChannels(connectionId)
        } catch (e: Exception) {
            log.error("Error removing connection {} from channels: {}", connectionId, e.message)
        }

        try {
            connectionStore.delete(connectionId)
        } catch (e: Exception) {
            log.warn("Connection {} may already be deleted: {}", connectionId, e.message)
        }
    }
}