package com.minare.nodegraph.config

import com.google.inject.PrivateModule
import com.google.inject.Singleton
import com.minare.controller.ChannelController
import com.minare.controller.ConnectionController
import com.minare.controller.MessageController
import com.minare.controller.OperationController
import com.minare.nodegraph.controller.NodeGraphChannelController
import com.minare.nodegraph.controller.NodeGraphConnectionController
import com.minare.nodegraph.controller.NodeGraphMessageController
import com.minare.nodegraph.controller.NodeGraphOperationController
import org.slf4j.LoggerFactory

/**
 * Application-specific Guice module for the Example app.
 * This provides bindings specific to our example application.
 *
 * When combined with the framework through a child injector,
 * bindings defined here will override the framework's default bindings.
 */
class NodeGraphModule : PrivateModule() {
    private val log = LoggerFactory.getLogger(NodeGraphModule::class.java)

    override fun configure() {
        bind(ChannelController::class.java).to(NodeGraphChannelController::class.java).`in`(Singleton::class.java)
        bind(ConnectionController::class.java).to(NodeGraphConnectionController::class.java).`in`(Singleton::class.java)
        bind(OperationController::class.java).to(NodeGraphOperationController::class.java).`in`(Singleton::class.java)
        bind(MessageController::class.java).to(NodeGraphMessageController::class.java).`in`(Singleton::class.java)

        expose(ChannelController::class.java)
        expose(ConnectionController::class.java)
        expose(OperationController::class.java)
        expose(MessageController::class.java)
    }
}