package com.minare.core.transport.interfaces

import io.vertx.core.Vertx
import kotlin.coroutines.CoroutineContext

abstract class SocketProtocol<T>(
    val vertx: Vertx,
    val context: CoroutineContext
) {
    abstract val router: ProtocolRouter<T>
    abstract val sockets: SocketStore<T>
}