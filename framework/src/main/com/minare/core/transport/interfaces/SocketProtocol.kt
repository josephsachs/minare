package com.minare.core.transport.interfaces

import com.google.inject.Inject
import io.vertx.core.Vertx
import kotlin.coroutines.CoroutineContext

abstract class SocketProtocol<T> @Inject constructor(
    val vertx: Vertx,
    val context: CoroutineContext
) {
    abstract val router: ProtocolRouter<T>
    abstract val sockets: SocketStore<T>
}