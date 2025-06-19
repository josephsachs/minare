package com.minare.worker.command.events

import com.google.inject.Inject
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import com.minare.worker.command.UpSocketVerticle

class UpSocketGetRouterEvent @Inject constructor(
    private val vertx: Vertx
) {
    suspend fun register(router: Router) {
        vertx.eventBus().consumer<JsonObject>(UpSocketVerticle.ADDRESS_GET_ROUTER) { message ->
            message.reply(JsonObject().put("routerId", router.toString()))
        }
    }
}