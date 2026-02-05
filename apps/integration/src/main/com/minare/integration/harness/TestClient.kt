package com.minare.integration.harness

import io.vertx.core.Vertx
import io.vertx.core.http.WebSocket
import io.vertx.core.http.WebSocketConnectOptions
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentLinkedQueue

class TestClient(
    private val vertx: Vertx,
    private val upHost: String = "localhost",
    private val upPort: Int = 4225,
    private val downHost: String = "localhost",
    private val downPort: Int = 4226
) {
    private var upSocket: WebSocket? = null
    private var downSocket: WebSocket? = null
    private var connectionId: String? = null

    private val receivedMessages = ConcurrentLinkedQueue<JsonObject>()
    private val messageWaiters = ConcurrentLinkedQueue<CompletableDeferred<JsonObject>>()

    private val log = LoggerFactory.getLogger(TestClient::class.java)

    suspend fun connect(timeoutMs: Long = 10000): String {
        return withTimeout(timeoutMs) {
            // Connect upsocket
            val upOptions = WebSocketConnectOptions()
                .setHost(upHost)
                .setPort(upPort)
                .setURI("/command")

            upSocket = vertx.createHttpClient()
                .webSocket(upOptions)
                .await()

            // Wait for connection_confirm
            val confirmDeferred = CompletableDeferred<JsonObject>()
            val syncDeferred = CompletableDeferred<JsonObject>()

            upSocket!!.textMessageHandler { msg ->
                log.info("UP RECEIVED: $msg")
                val json = JsonObject(msg)
                when (json.getString("type")) {
                    "connection_confirm" -> confirmDeferred.complete(json)
                    "initial_sync_complete" -> syncDeferred.complete(json)
                }
            }

            val confirm = confirmDeferred.await()
            connectionId = confirm.getString("connectionId")

            // Connect downsocket
            val downOptions = WebSocketConnectOptions()
                .setHost(downHost)
                .setPort(downPort)
                .setURI("/update")

            downSocket = vertx.createHttpClient()
                .webSocket(downOptions)
                .await()

            // Associate with connection
            downSocket!!.writeTextMessage(
                JsonObject().put("connectionId", connectionId).encode()
            )

            // Set up message handler for updates
            downSocket!!.textMessageHandler { msg ->
                log.info("DOWN RECEIVED: $msg")
                val json = JsonObject(msg)

                // Check if anyone is waiting for this message
                val waiter = messageWaiters.poll()
                if (waiter != null) {
                    waiter.complete(json)
                } else {
                    receivedMessages.add(json)
                }
            }

            syncDeferred.await()

            // Wait for initial_sync_complete
            // waitForMessage { it.getString("type") == "initial_sync_complete" }

            connectionId!!
        }
    }

    suspend fun send(message: JsonObject) {
        log.info("SENDING: $message")
        upSocket!!.writeTextMessage(message.encode())
    }

    suspend fun waitForMessage(
        timeoutMs: Long = 5000,
        predicate: (JsonObject) -> Boolean
    ): JsonObject {
        return withTimeout(timeoutMs) {
            // Check already received messages first
            val existing = receivedMessages.find { predicate(it) }
            if (existing != null) {
                receivedMessages.remove(existing)
                return@withTimeout existing
            }

            // Wait for new message
            val deferred = CompletableDeferred<JsonObject>()
            messageWaiters.add(deferred)

            var result = deferred.await()
            while (!predicate(result)) {
                receivedMessages.add(result)
                val nextDeferred = CompletableDeferred<JsonObject>()
                messageWaiters.add(nextDeferred)
                result = nextDeferred.await()
            }
            result
        }
    }

    suspend fun disconnect() {
        upSocket?.close()?.await()
        downSocket?.close()?.await()
    }

    fun getConnectionId(): String = connectionId!!
}