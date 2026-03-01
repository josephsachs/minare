package com.minare.integration.suites

import com.google.inject.Injector
import com.minare.core.frames.services.WorkerRegistry
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.integration.harness.Assertions.assertEquals
import com.minare.integration.harness.Assertions.assertNotNull
import com.minare.integration.harness.Assertions.assertTrue
import com.minare.integration.harness.Assertions.assertFalse
import com.minare.integration.harness.TestRunner
import com.minare.integration.harness.TestSuite
import io.vertx.core.Vertx
import io.vertx.core.http.WebSocket
import io.vertx.core.http.WebSocketConnectOptions
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory

class ConnectionTestSuite(private val injector: Injector) : TestSuite {
    override val name = "Connection Tests"
    private val log = LoggerFactory.getLogger(ConnectionTestSuite::class.java)

    private class BufferedSocket(val socket: WebSocket) {
        val messages = Channel<JsonObject>(Channel.UNLIMITED)

        init {
            socket.textMessageHandler { msg ->
                messages.trySend(JsonObject(msg))
            }
        }

        suspend fun waitForType(type: String, timeoutMs: Long = 5000): JsonObject {
            val stash = mutableListOf<JsonObject>()
            return withTimeout(timeoutMs) {
                try {
                    while (true) {
                        val msg = messages.receive()
                        if (msg.getString("type") == type) return@withTimeout msg
                        stash.add(msg)
                    }
                    @Suppress("UNREACHABLE_CODE")
                    throw IllegalStateException("unreachable")
                } finally {
                    stash.forEach { messages.trySend(it) }
                }
            }
        }

        suspend fun close() {
            socket.close().await()
            messages.close()
        }
    }

    override suspend fun run(runner: TestRunner) {
        val vertx = injector.getInstance(Vertx::class.java)
        val connectionStore = injector.getInstance(ConnectionStore::class.java)
        val workerRegistry = injector.getInstance(WorkerRegistry::class.java)
        val hostName = workerRegistry.getAllWorkers().firstNotNullOf { it.key }

        runner.test("upsocket connects and receives connection_confirm") {
            val up = connectBuffered(vertx, hostName, 4225, "/command")
            try {
                val confirm = up.waitForType("connection_confirm")
                assertNotNull(confirm.getString("connectionId")) { "connection_confirm should contain connectionId" }
            } finally {
                up.close()
            }
        }

        runner.test("connection exists in store after upsocket connects") {
            val up = connectBuffered(vertx, hostName, 4225, "/command")
            try {
                val connectionId = up.waitForType("connection_confirm").getString("connectionId")

                assertTrue(connectionStore.exists(connectionId)) { "Connection should exist in store" }

                val connection = connectionStore.find(connectionId)
                assertNotNull(connection.upSocketId) { "upSocketId should be set" }
                assertNotNull(connection.upSocketInstanceId) { "upSocketDeploymentId should be set" }
            } finally {
                up.close()
            }
        }

        runner.test("downsocket associates and receives down_socket_confirm") {
            val up = connectBuffered(vertx, hostName, 4225, "/command")
            try {
                val connectionId = up.waitForType("connection_confirm").getString("connectionId")

                val down = connectBuffered(vertx, hostName, 4226, "/update")
                try {
                    down.socket.writeTextMessage(JsonObject().put("connectionId", connectionId).encode())
                    val downConfirm = down.waitForType("down_socket_confirm")

                    assertEquals(connectionId, downConfirm.getString("connectionId")) {
                        "down_socket_confirm connectionId should match"
                    }
                } finally {
                    down.close()
                }
            } finally {
                up.close()
            }
        }

        runner.test("connection has both deployment IDs after full connect") {
            val up = connectBuffered(vertx, hostName, 4225, "/command")
            try {
                val connectionId = up.waitForType("connection_confirm").getString("connectionId")

                val down = connectBuffered(vertx, hostName, 4226, "/update")
                try {
                    down.socket.writeTextMessage(JsonObject().put("connectionId", connectionId).encode())
                    down.waitForType("down_socket_confirm")

                    val connection = connectionStore.find(connectionId)
                    assertNotNull(connection.upSocketInstanceId) { "upSocketDeploymentId should be set" }
                    assertNotNull(connection.downSocketInstanceId) { "downSocketDeploymentId should be set" }
                    assertNotNull(connection.downSocketId) { "downSocketId should be set" }
                } finally {
                    down.close()
                }
            } finally {
                up.close()
            }
        }

        runner.test("fully connected triggers initial_sync_complete on upsocket") {
            val up = connectBuffered(vertx, hostName, 4225, "/command")
            try {
                val connectionId = up.waitForType("connection_confirm").getString("connectionId")

                val down = connectBuffered(vertx, hostName, 4226, "/update")
                try {
                    down.socket.writeTextMessage(JsonObject().put("connectionId", connectionId).encode())
                    down.waitForType("down_socket_confirm")

                    val syncComplete = up.waitForType("initial_sync_complete")
                    assertNotNull(syncComplete) { "Should receive initial_sync_complete on upsocket" }
                } finally {
                    down.close()
                }
            } finally {
                up.close()
            }
        }

        runner.test("upsocket disconnect marks connection non-reconnectable") {
            val up = connectBuffered(vertx, hostName, 4225, "/command")
            val connectionId = up.waitForType("connection_confirm").getString("connectionId")

            up.close()
            kotlinx.coroutines.delay(500)

            val connection = connectionStore.find(connectionId)
            assertFalse(connection.reconnectable) { "Connection should be non-reconnectable after upsocket close" }
        }

        runner.test("downsocket reconnects with same connectionId") {
            val up = connectBuffered(vertx, hostName, 4225, "/command")
            try {
                val connectionId = up.waitForType("connection_confirm").getString("connectionId")

                val down1 = connectBuffered(vertx, hostName, 4226, "/update")
                down1.socket.writeTextMessage(JsonObject().put("connectionId", connectionId).encode())
                down1.waitForType("down_socket_confirm")
                up.waitForType("initial_sync_complete")

                down1.close()
                kotlinx.coroutines.delay(500)

                val down2 = connectBuffered(vertx, hostName, 4226, "/update")
                try {
                    down2.socket.writeTextMessage(JsonObject().put("connectionId", connectionId).encode())
                    val reconfirm = down2.waitForType("down_socket_confirm")

                    assertEquals(connectionId, reconfirm.getString("connectionId")) {
                        "Reconnected downsocket should confirm with same connectionId"
                    }
                } finally {
                    down2.close()
                }
            } finally {
                up.close()
            }
        }
    }

    private suspend fun connectBuffered(vertx: Vertx, host: String, port: Int, uri: String): BufferedSocket {
        val socket = vertx.createHttpClient()
            .webSocket(WebSocketConnectOptions().setHost(host).setPort(port).setURI(uri))
            .await()
        return BufferedSocket(socket)
    }
}