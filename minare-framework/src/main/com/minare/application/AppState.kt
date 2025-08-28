package com.minare.application

import io.vertx.core.json.JsonObject

/**
 * Interface for distributed application state.
 * Provides a simple key-value store that works across all instances in the cluster.
 */
interface AppState {
    suspend fun get(key: String): String?

    suspend fun set(key: String, value: String)

    suspend fun getJson(key: String): JsonObject?

    suspend fun setJson(key: String, value: JsonObject)

    suspend fun remove(key: String): String?

    suspend fun exists(key: String): Boolean

    suspend fun keys(): Set<String>
}