package com.minare.application.config

import com.minare.exceptions.ConfigurationException
import io.vertx.core.json.JsonObject
import com.minare.application.config.FrameConfiguration.Companion.AutoSession
import java.awt.Frame

class FrameworkConfigBuilder {
    private var errors: MutableList<String> = mutableListOf()

    fun build(file: JsonObject): FrameworkConfig {

        val config = FrameworkConfig()
            .let { setSocketsConfig(file, it) }
            .let { setEntityConfig(file, it) }
            .let { setFramesConfig(file, it) }
            .let { setTasksConfig(file, it) }

        validate()

        return config
    }

    /**
     * Configuration of sockets parameters
     */
    private fun setSocketsConfig(file: JsonObject, config: FrameworkConfig): FrameworkConfig {
        val sockets = require(file.getJsonObject("sockets"), "sockets section must be specified")
        val upsocket = require(sockets.getJsonObject("up"), "sockets.up section must be specified")
        config.sockets.up.host = require(upsocket.getString("host"), "sockets.up.host must be specified")
        config.sockets.up.port = require(upsocket.getInteger("port"), "sockets.up.port must be specified")
        config.sockets.up.basePath = require(upsocket.getString("base_path"), "sockets.up.base_path must be specified")
        config.sockets.up.heartbeatInterval = require(upsocket.getInteger("heartbeat_interval"), "sockets.up.heartbeat_interval must be specified").toLong()
        config.sockets.up.handshakeTimeout = require(upsocket.getInteger("handshake_timeout"), "sockets.up.handshake_timeout must be specified").toLong()

        val downsocket = require(sockets.getJsonObject("down"), "sockets.down section must be specified")
        config.sockets.down.host = require(downsocket.getString("host"), "sockets.down.host must be specified")
        config.sockets.down.port = require(downsocket.getInteger("port"), "sockets.down.port must be specified")
        config.sockets.down.cacheTtl = require(downsocket.getInteger("cache_ttl"), "sockets.down.cache_ttl must be specified").toLong()

        val connection = require(sockets.getJsonObject("connection"), "sockets.connection section must be specified")
        config.sockets.connection.connectionExpiry = require(connection.getString("connection_expiry"), "sockets.connection.connection_expiry must be specified").toLong()
        config.sockets.connection.cleanupInterval = require(connection.getString("cleanup_interval"), "sockets.connection.cleanup_interval must be specified").toLong()
        config.sockets.connection.reconnectTimeout = require(connection.getString("reconnect_timeout"), "sockets.connection.reconnect_timeout must be specified").toLong()

        return config
    }

    /**
     * Set configuration for entities
     */
    private fun setEntityConfig(file: JsonObject, config: FrameworkConfig): FrameworkConfig {
        val entity = require(file.getJsonObject("entity"), "entity section must be specified")
        val entityUpdate = require(entity.getJsonObject("update"), "entity.update section must be specified")
        config.entity.update.batchInterval = require(entityUpdate.getInteger("batch_interval"), "entity.update.batch_interval must be specified").toLong()

        return config
    }

    /**
     * Set configuration for frame loop, sessions and timeline head
     */
    private fun setFramesConfig(file: JsonObject, config: FrameworkConfig): FrameworkConfig {
        val frames = require(file.getJsonObject("frames"), "frames section must be specified")
        config.frames.frameDuration = require(frames.getInteger("frame_duration"), "frames.frame_duration must be specified").toLong()
        config.frames.lookahead = require(frames.getInteger("lookahead"), "frames.lookahead must be specified")

        val session = require(frames.getJsonObject("session"), "frames.session section must be specified")
        val autoSession = require(session.getString("auto_session"), "frames.session.auto_session must be specified")

        config.frames.session.autoSession = when (autoSession) {
            "frames_per_session" -> AutoSession.FRAMES_PER_SESSION
            "never" -> AutoSession.NEVER
            else -> AutoSession.NEVER
        }

        if (config.frames.session.autoSession == AutoSession.FRAMES_PER_SESSION) {
            config.frames.session.framesPerSession = require(session.getInteger("frames_per_session"), "frames.session.frames_per_session is must be specified, due to frames.session.auto_session setting")
        }

        val timeline = frames.getJsonObject("timeline")

        if (!timeline.isEmpty) {
            val detach = require(timeline.getJsonObject("detach"), "frames.timeline.detach section must be specified, since frames.timeline exists")
            val flushOnDetach = require(detach.getString("flush"), "frames.timeline.detach.flush must be specified")
            val bufferOnDetach = require(detach.getString("buffer"), "frames.timeline.detach.buffer must be specified")
            config.frames.timeline.detach.buffer = toBoolean(flushOnDetach)
            config.frames.timeline.detach.buffer = toBoolean(bufferOnDetach)

            val replay = require(timeline.getJsonObject("replay"), "frames.timeline.replay section must be specified, since frames.timeline exists")
            val bufferOnReplay = require(replay.getString("buffer"), "frames.timeline.replay.buffer must be specified")
            val assignOnResume = require(replay.getString("assign_on_resume"), "frames.timeline.replay.assign_on_resume must be specified")
            config.frames.timeline.replay.buffer = toBoolean(bufferOnReplay)
            config.frames.timeline.replay.assignOnResume = toBoolean(assignOnResume)
        }

        return config
    }

    /**
     * Set configuration for tasks
     */
    private fun setTasksConfig(file: JsonObject, config: FrameworkConfig): FrameworkConfig {
        val tasks = file.getJsonObject("tasks")
        config.tasks.tickInterval = require(tasks.getInteger("tick_interval"), "tasks.tick_interval must be specified").toLong()

        return config
    }

    private fun require(value: String?, message: String): String {
        return value ?: run { errors.add(message); "" }
    }

    private fun require(value: Int?, message: String): Int {
        return value ?: run { errors.add(message); 0 }
    }

    private fun require(value: JsonObject?, message: String): JsonObject {
        return value ?: run { errors.add(message); JsonObject() }
    }

    private fun toBoolean(value: String): Boolean {
        return when (value) {
            "true" -> true
            "false" -> false
            else -> false
        }
    }

    private fun validate() {
        if (errors.isNotEmpty()) {
            throw ConfigurationException("Config errors:\n${errors.joinToString("\n")}")
        }
    }
}