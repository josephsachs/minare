package com.minare.core.utils.types

import com.google.inject.Inject
import com.google.inject.Singleton
import io.vertx.core.Vertx

/**
 * A variable holder that maintains current state using vertx event bus.
 * Preferred for cases with rare writes and very frequent reads.
 */
class PushVar<T>(
    private val vertx: Vertx,
    private val address: String,
    initialValue: T,
    private val serializer: (T) -> Any = { it as Any },
    private val deserializer: (Any) -> T = { it as T }
) {
    @Singleton
    class Factory @Inject constructor(
        private val vertx: Vertx
    ) {
        fun <T> create(address: String,
                   initialValue: T,
                   serializer: (T) -> Any = { it as Any },
                   deserializer: (Any) -> T = { it as T }
        ): PushVar<T> {
            return PushVar(vertx, address, initialValue, serializer, deserializer)
        }
    }

    @Volatile
    private var localValue: T = initialValue

    init {
        vertx.eventBus().consumer<Any>(address) { message ->
            localValue = deserializer(message.body())
        }
    }

    fun get(): T = localValue

    fun set(value: T) {
        localValue = value
        vertx.eventBus().publish(address, serializer(value))
    }
}