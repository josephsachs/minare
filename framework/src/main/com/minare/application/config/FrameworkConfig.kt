package com.minare.application.config

import com.minare.core.frames.coordinator.services.SessionService.Companion.AutoSession
import com.minare.core.frames.services.SnapshotService.Companion.SnapshotStoreOption
import com.minare.core.storage.interfaces.EntityGraphStoreOption

class FrameworkConfig {
    class SocketsSection {
        var up: UpSocketConfig = UpSocketConfig()
        var down: DownSocketConfig = DownSocketConfig()
        var connection: SocketConnectionConfig = SocketConnectionConfig()
    }

    class UpSocketConfig {
        var host: String = ""
        var port: Int = 0
        var basePath: String = ""
        var handshakeTimeout: Long = 0L
        var heartbeatInterval: Long = 0L
        var ack: Boolean = true
    }

    class DownSocketConfig {
        var host: String = ""
        var port: Int = 0
        var basePath: String = ""
        var tickInterval: Long = 0L
        var cacheTtl: Long = 0L
        var heartbeatInterval: Long = 0L
    }

    class SocketConnectionConfig {
        var cleanupInterval: Long = 0L
        var aggressiveCleanup: Boolean = false
        var reconnectTimeout: Long = 0L
        var connectionExpiry: Long = 0L
    }

    class EntityConfig {
        var factoryName: String = ""
        var update = EntityUpdateConfig()
        var graph = EntityGraphConfig()
    }

    class EntityUpdateConfig {
        var collectChanges: Boolean = true
        var interval: Long = 0L
    }

    class EntityGraphConfig {
        var store: EntityGraphStoreOption = EntityGraphStoreOption.NONE
    }

    class FramesConfig {
        var frameDuration: Long = 0L
        var lookahead: Int = 0
        var session: FrameSessionConfig = FrameSessionConfig()
        var timeline: FrameTimelineConfig = FrameTimelineConfig()
        var snapshot: FrameSnapshotConfig = FrameSnapshotConfig()
    }

    class FrameSessionConfig {
        var autoSession: AutoSession = AutoSession.NEVER
        var framesPerSession: Int = 0
    }

    class FrameTimelineConfig {
        var detach: FrameTimelineDetachConfig = FrameTimelineDetachConfig()
        var replay: FrameTimelineReplayConfig = FrameTimelineReplayConfig()
    }

    class FrameTimelineDetachConfig {
        var flushOnDetach: Boolean = true
        var bufferWhenDetached: Boolean = true
        var assignOnResume: Boolean = false
        var replayOnResume: Boolean = false
    }

    class FrameTimelineReplayConfig {
        var bufferWhileReplay: Boolean = true
    }

    class FrameSnapshotConfig {
        var enabled: Boolean = false
        var store: SnapshotStoreOption = SnapshotStoreOption.NONE
    }

    class TaskConfig {
        var tickInterval: Long = 0L
    }

    class FilesystemConfig {
        var storagePath: String = ""
    }

    class MongoConfig {
        var host: String = ""
        var port: Int = 0
        var database: String = ""
        var configured: Boolean = false
        var enabled: Boolean = false
    }

    class RedisConfig {
        var host: String = ""
        var port: Int = 0
    }

    class KafkaConfig {
        var host: String = ""
        var port: Int = 0
        var groupId: String = ""
    }

    class DevelopmentSettings {
        var resetData: Boolean = false
    }

    class HazelcastConfig {
        var clusterName: String = ""
    }

    var sockets = SocketsSection()
    var entity = EntityConfig()
    var frames = FramesConfig()
    var tasks = TaskConfig()
    var filesystem = FilesystemConfig()

    var mongo = MongoConfig()
    var redis = RedisConfig()
    var kafka = KafkaConfig()

    var development = DevelopmentSettings()
    var hazelcast = HazelcastConfig()
}