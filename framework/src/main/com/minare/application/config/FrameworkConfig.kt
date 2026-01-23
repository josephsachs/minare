package com.minare.application.config

import com.minare.core.frames.coordinator.services.SessionService.Companion.AutoSession

class FrameworkConfig {
    class SocketsSection {
        var up: SocketConfig = SocketConfig()
        var down: SocketConfig = SocketConfig()
        var connection: SocketConnectionConfig = SocketConnectionConfig()
    }

    class SocketConfig {
        var host: String = ""
        var port: Int = 0
        var basePath: String = ""
        var handshakeTimeout: Long = 0L
        var heartbeatInterval: Long = 0L

        var tickInterval: Long = 0L
        var cacheTtl: Long = 0L
    }

    class SocketConnectionConfig {
        var cleanupInterval: Long = 0L
        var reconnectTimeout: Long = 0L
        var connectionExpiry: Long = 0L
    }

    class EntityConfig {
        var factoryName: String = ""
        var update = EntityUpdateConfig()
    }

    class EntityUpdateConfig {
        var batchInterval: Long = 0L
    }

    class FramesConfig {
        var frameDuration: Long = 0L
        var lookahead: Int = 0
        var session: FrameSessionConfig = FrameSessionConfig()
        var timeline: FrameTimelineConfig = FrameTimelineConfig()
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

    class TaskConfig {
        var tickInterval: Long = 0L
    }

    var sockets = SocketsSection()
    var entity = EntityConfig()
    var frames = FramesConfig()
    var tasks = TaskConfig()
}