package com.minare.application.config

import com.minare.exceptions.ConfigurationException
import io.vertx.core.json.JsonObject
import com.minare.core.frames.coordinator.services.SessionService.Companion.AutoSession
import com.minare.core.frames.services.SnapshotService.Companion.SnapshotStoreOption
import io.vertx.core.impl.logging.LoggerFactory

class FrameworkConfigBuilder {
    private val log = LoggerFactory.getLogger(FrameworkConfigBuilder::class.java)

    private var errors: MutableList<String> = mutableListOf()
    private var infos: MutableList<String> = mutableListOf()

    fun build(file: JsonObject): FrameworkConfig {
        val config = FrameworkConfig()
            .let { setSocketsConfig(file, it) }
            .let { setEntityConfig(file, it) }
            .let { setFramesConfig(file, it) }
            .let { setTasksConfig(file, it) }
            .let { setFilesystemConfig(file, it) }
            .let { setMongoConfig(file, it) }
            .let { setRedisConfig(file, it) }
            .let { setKafkaConfig(file, it) }
            .let { setDevelopmentConfig(file, it) }

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

        config.entity.factoryName = require(
            entity.getString("factory"),
            "entity.factory must specify a fully-qualified name of a valid EntityFactory implementation (e.g. `com.myapp.foo.bar.baz.MyEntityFactory`)"
        )

        val entityUpdate = require(entity.getJsonObject("update"), "entity.update section must be specified")
        val collectChanges = require(entityUpdate.getString("collect_changes"), "entity.update.collect_changes must be specified")
        config.entity.update.collectChanges = toBoolean(collectChanges)
        config.entity.update.interval = entityUpdate.getInteger("interval")?.toLong() ?: 0L

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

        val timeline = withInfo(frames.getJsonObject("timeline"), "frames.timeline section not specified, using defaults")

        if (!timeline.isEmpty) {
            val detach = require(timeline.getJsonObject("detach"), "frames.timeline.detach section must be specified, since frames.timeline exists")
            val flushOnDetach = require(detach.getString("flush_on_detach"), "frames.timeline.detach.flush_on_detach must be specified")
            val bufferWhenDetached = require(detach.getString("buffer_when_detached"), "frames.timeline.detach.buffer_when_detached must be specified")
            val assignOnResume = withInfo(detach.getString("assign_on_resume"), "frames.timeline.detach.assign_on_resume defaults to false unless specified")
            val replayOnResume = withInfo(detach.getString("replay_on_resume"), "frames.timeline.detach.replay_on_resume defaults to false unless specified")
            config.frames.timeline.detach.flushOnDetach = toBoolean(flushOnDetach)
            config.frames.timeline.detach.bufferWhenDetached = toBoolean(bufferWhenDetached)
            config.frames.timeline.detach.assignOnResume = toBoolean(assignOnResume)
            config.frames.timeline.detach.replayOnResume = toBoolean(replayOnResume)

            val replay = require(timeline.getJsonObject("replay"), "frames.timeline.replay section must be specified, since frames.timeline exists")
            val bufferWhileReplay = require(replay.getString("buffer_while_replay"), "frames.timeline.replay.buffer_while_replay must be specified")
            config.frames.timeline.replay.bufferWhileReplay = toBoolean(bufferWhileReplay)
        } else {
            config.frames.timeline.detach.flushOnDetach = true
            config.frames.timeline.detach.bufferWhenDetached = true
            config.frames.timeline.detach.assignOnResume = false
            config.frames.timeline.detach.replayOnResume = false
            config.frames.timeline.replay.bufferWhileReplay = true
        }

        val snapshot = withInfo(frames.getJsonObject("snapshot"), "frames.snapshot section not specified, snapshots not enabled")

        if (!snapshot.isEmpty) {
            val snapshotStore = require(snapshot.getString("store"), "frames.snapshot.store must be specified, since frames.snapshot section exists")
            config.frames.snapshot.enabled = true
            config.frames.snapshot.store = when (snapshotStore) {
                "mongo" -> SnapshotStoreOption.MONGO
                "json" -> SnapshotStoreOption.JSON
                else -> {
                    infos.add("frames.snapshot.store option not recognized, snapshots will not be stored (options: mongo, json)")
                    config.frames.snapshot.enabled = false
                    SnapshotStoreOption.NONE
                }
            }
        }

        return config
    }

    /**
     * Set configuration for tasks
     */
    private fun setTasksConfig(file: JsonObject, config: FrameworkConfig): FrameworkConfig {
        val tasks = require(file.getJsonObject("tasks"), "tasks section must be specified")
        config.tasks.tickInterval = require(tasks.getInteger("tick_interval"), "tasks.tick_interval must be specified").toLong()

        return config
    }

    /**
     * Set configuration for file system
     */
    private fun setFilesystemConfig(file: JsonObject, config: FrameworkConfig): FrameworkConfig {
        val filesystem = require(file.getJsonObject("filesystem"), "filesystem section must be specified")
        config.filesystem.storagePath = require(filesystem.getString("storage_path"), "filesystem.storage_path must be specified")

        return config
    }

    /**
     * Set configuration for MongoDB
     */
    private fun setMongoConfig(file: JsonObject, config: FrameworkConfig): FrameworkConfig {
        val mongo = withInfo(file.getJsonObject("mongo"), "mongo section not specified, any features configured to use mongo driver are implicitly disabled")

        if (!mongo.isEmpty) {
            config.mongo.host = require(mongo.getString("host"), "mongo.host must be specified, since mongo section exists")
            config.mongo.port = require(mongo.getInteger("port"), "mongo.port must be specified, since mongo section exists")
            config.mongo.database = require(mongo.getString("database"), "mongo.database must be specified, since mongo section exists")
            config.mongo.hasMongo = true
        }

        return config
    }

    /**
     * Set configuration for Redis
     */
    private fun setRedisConfig(file: JsonObject, config: FrameworkConfig): FrameworkConfig {
        val redis = require(file.getJsonObject("redis"), "redis section must be specified")
        config.redis.host = require(redis.getString("host"), "redis.host must be specified")
        config.redis.port = require(redis.getInteger("port"), "redis.port must be specified")
        return config
    }

    /**
     * Set configuration for Kafka
     */
    private fun setKafkaConfig(file: JsonObject, config: FrameworkConfig): FrameworkConfig {
        val kafka = require(file.getJsonObject("kafka"), "kafka section must be specified")
        config.kafka.host = require(kafka.getString("host"), "kafka.host must be specified")
        config.kafka.port = require(kafka.getInteger("port"), "kafka.port must be specified")
        config.kafka.groupId = kafka.getString("group_id")  ?: "minare-coordinator"
        return config
    }

    /**
     * Set configuration for developer settings
     */
    private fun setDevelopmentConfig(file: JsonObject, config: FrameworkConfig): FrameworkConfig {
        val development = withInfo(file.getJsonObject("development"), "development section not specified, development settings are implicitly disabled")

        if (!development.isEmpty) {
            config.development.resetData = toBoolean(
                withInfo(
                    development.getString("reset_data"),
                    "WARNING: development.reset_data will delete ALL data in configured data stores, DO NOT USE IN PRODUCTION"
                )
            )
        }

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

    private fun withInfo(value: String?, message: String): String {
        return value ?: run { infos.add(message); "" }
    }

    private fun withInfo(value: Int?, message: String): Int {
        return value ?: run { infos.add(message); 0 }
    }

    private fun withInfo(value: JsonObject?, message: String): JsonObject {
        return value ?: run { infos.add(message); JsonObject() }
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

    private fun logInfo() {
        if (errors.isNotEmpty()) {
            log.info("Info:\n${errors.joinToString("\n")}")
        }
    }
}