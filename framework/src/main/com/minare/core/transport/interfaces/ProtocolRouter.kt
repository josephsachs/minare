package com.minare.core.transport.interfaces

import com.google.inject.Inject
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import kotlin.coroutines.CoroutineContext

abstract class ProtocolRouter<T> @Inject constructor(
    protected val vertx: Vertx,
    protected val context: CoroutineContext
) {
    abstract fun setupRoutes(
        basePath: String,
        onConnection: suspend (T, String) -> Unit
    )

    abstract fun sendMessage(socket: T, message: JsonObject)

    abstract fun sendError(socket: T, error: Throwable, connectionId: String?)

    abstract fun sendConfirmation(socket: T, type: String, connectionId: String)

    abstract fun close(socket: T): Boolean

    abstract fun isClosed(socket: T): Boolean
}