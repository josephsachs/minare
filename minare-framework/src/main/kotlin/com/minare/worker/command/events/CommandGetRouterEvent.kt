package com.minare.worker.command.events

import com.google.inject.Inject
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import com.minare.worker.command.CommandVerticle
import com.minare.worker.command.CommandVerticle.Companion.ADDRESS_GET_ROUTER

class CommandGetRouterEvent @Inject constructor(
    private val vertx: Vertx
) {
    suspend fun register(context: CommandVerticle) {
        vertx.eventBus().consumer<JsonObject>(ADDRESS_GET_ROUTER) { message ->
            message.reply(JsonObject().put("routerId", context.router.toString()))
        }
    }
}