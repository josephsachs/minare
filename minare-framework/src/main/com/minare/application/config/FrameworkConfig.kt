package com.minare.application.config

class FrameworkConfig {
    class SocketsSection {
        var up: SocketConfig = SocketConfig()
        var down: SocketConfig = SocketConfig()
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

    class EntityConfig {
        var update = EntityUpdateConfig()
    }

    class EntityUpdateConfig {
        var batchInterval: Long = 0L
    }

    var sockets = SocketsSection()
    var entity = EntityConfig()
}