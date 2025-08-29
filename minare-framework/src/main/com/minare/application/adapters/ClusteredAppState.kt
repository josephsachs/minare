package com.minare.application.adapters

import com.minare.application.interfaces.AppState
import io.vertx.core.json.JsonObject
import io.vertx.core.shareddata.AsyncMap
import io.vertx.kotlin.coroutines.await
import org.slf4j.LoggerFactory

/**
 * Clustered implementation of AppState using Vert.x shared data.
 * Provides distributed state across all instances in the cluster.
 */
class ClusteredAppState(
    private val sharedMap: AsyncMap<String, String>
) : AppState {
    private val log = LoggerFactory.getLogger(ClusteredAppState::class.java)

    override suspend fun get(key: String): String? {
        return try {
            sharedMap.get(key).await()
        } catch (e: Exception) {
            log.error("Failed to get value for key: $key", e)
            null
        }
    }

    override suspend fun set(key: String, value: String) {
        try {
            sharedMap.put(key, value).await()
            log.debug("Set app state: {} = {}", key, value)
        } catch (e: Exception) {
            log.error("Failed to set value for key: $key", e)
            throw e
        }
    }

    override suspend fun getJson(key: String): JsonObject? {
        val value = get(key)
        return value?.let {
            try {
                JsonObject(it)
            } catch (e: Exception) {
                log.error("Failed to parse JSON for key: $key", e)
                null
            }
        }
    }

    override suspend fun setJson(key: String, value: JsonObject) {
        set(key, value.encode())
    }

    override suspend fun remove(key: String): String? {
        return try {
            sharedMap.remove(key).await()
        } catch (e: Exception) {
            log.error("Failed to remove key: $key", e)
            null
        }
    }

    override suspend fun exists(key: String): Boolean {
        return try {
            sharedMap.get(key).await() != null
        } catch (e: Exception) {
            log.error("Failed to check existence of key: $key", e)
            false
        }
    }

    override suspend fun keys(): Set<String> {
        return try {
            sharedMap.keys().await()
        } catch (e: Exception) {
            log.error("Failed to get keys", e)
            emptySet()
        }
    }
}