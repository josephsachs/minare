package com.minare.application.config

import com.minare.exceptions.ConfigurationException
import io.vertx.core.impl.logging.LoggerFactory
import io.vertx.core.json.JsonObject

class FrameworkConfigBuilder {
    private val log = LoggerFactory.getLogger(FrameworkConfigBuilder::class.java)
    private var errors: MutableList<String> = mutableListOf()

    public fun build(file: JsonObject): FrameworkConfig {

        val config = FrameworkConfig()

        val sockets = file.getJsonObject("sockets")
        val upsocket = sockets.getJsonObject("up")
        config.sockets.up.host = require(upsocket.getString("host"), "sockets.up.host must be specified")
        config.sockets.up.port = require(upsocket.getInteger("port"), "sockets.up.port must be specified")
        config.sockets.up.basePath = require(upsocket.getString("base_path"), "sockets.up.base_path must be specified")
        config.sockets.up.heartbeatInterval = require(upsocket.getInteger("heartbeat_interval"), "sockets.up.heartbeat_interval must be specified").toLong()
        config.sockets.up.handshakeTimeout = require(upsocket.getInteger("handshake_timeout"), "sockets.up.handshake_timeout must be specified").toLong()

        val downsocket = sockets.getJsonObject("down")
        config.sockets.down.host = require(downsocket.getString("host"), "sockets.down.host must be specified")
        config.sockets.down.port = require(downsocket.getInteger("port"), "sockets.down.port must be specified")
        config.sockets.down.cacheTtl = require(downsocket.getInteger("cache_ttl"), "sockets.down.cache_ttl must be specified").toLong()

        val connection = sockets.getJsonObject("connection")
        config.sockets.connection.connectionExpiry = require(connection.getString("connection_expiry"), "sockets.connection.connection_expiry must be specified").toLong()
        config.sockets.connection.cleanupInterval = require(connection.getString("cleanup_interval"), "sockets.connection.cleanup_interval must be specified").toLong()
        config.sockets.connection.reconnectTimeout = require(connection.getString("reconnect_timeout"), "sockets.connection.reconnect_timeout must be specified").toLong()

        val entity = file.getJsonObject("entity")
        val entityUpdate = entity.getJsonObject("update")
        config.entity.update.batchInterval = require(entityUpdate.getInteger("batch_interval"), "entity.update.batch_interval must be specified").toLong()

        validate()

        return config
    }

    private fun require(value: String?, message: String): String {
        return value ?: run { errors.add(message); "" }
    }

    private fun require(value: Int?, message: String): Int {
        return value ?: run { errors.add(message); 0 }
    }

    private fun validate() {
        if (errors.isNotEmpty()) {
            throw ConfigurationException("Config errors:\n${errors.joinToString("\n")}")
        }
    }
}