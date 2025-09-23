package com.minare.core.transport.models.message

import com.minare.core.transport.models.Connection

class SyncCommand constructor(
    val connection: Connection,
    val type: SyncCommandType,
    val select: Set<String>?
): CommandMessageObject {}

enum class SyncCommandType {
    CHANNEL,
    ENTITY
}