package com.minare.controller

import com.minare.core.storage.interfaces.ContextStore
import io.vertx.core.json.JsonObject
import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.storage.interfaces.StateStore
import kotlinx.coroutines.CoroutineScope

/**
 * Controls channel and context models, including channel subscriptions
 * and entity channel contexts, and provides message broadcast capability.
 */
@Singleton
open class UpdateController @Inject constructor() {
    @Inject
    private lateinit var coroutineScope: CoroutineScope
    @Inject
    private lateinit var channelController: ChannelController
    @Inject
    private lateinit var contextStore: ContextStore
    @Inject
    private lateinit var stateStore: StateStore

    /**
     * Push to all channel listeners. Will broadcast a Json message
     * with the structure of a change update.
     */
    suspend fun resync(entityIds: List<String>) {
        coroutineScope.run {
            val entities = stateStore.findJson(entityIds)

            entities.forEach() { (key, value) ->
                val msg = getUpdateMessage(key, value)

                contextStore
                    .getChannelsByEntityId(key)
                    .forEach { channel ->
                        channelController.broadcast(channel, msg)
                    }
            }
        }
    }

    /**
     * Application hook
     * TODO: Have redis publisher and all others use the
     * TODO: application hook for update messages
     */
    open fun getUpdateMessage(entityId: String, entityJson: JsonObject): JsonObject {
        return JsonObject()
            .put("type", "update")
            .put("timestamp", System.currentTimeMillis())
            .put("updates", JsonObject().put(entityId, entityJson))
    }
}