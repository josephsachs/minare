package com.minare.core.transport.adapters

import com.minare.core.transport.interfaces.SocketProtocol
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.Vertx
import io.vertx.core.http.ServerWebSocket
import kotlin.coroutines.CoroutineContext

class WebsocketProtocol(
    vertx: Vertx,
    context: CoroutineContext,
    logger: VerticleLogger
) : SocketProtocol<ServerWebSocket>(vertx, context) {
    override val sockets: WebsocketStore = WebsocketStore()
    override val router: WebsocketRouter = WebsocketRouter(vertx, context, logger)
}