package com.minare.core.transport.models.message

import com.minare.core.transport.models.Connection

class HeartbeatResponse constructor(
    val connection: Connection,
    val timestamp: Long,
    val clientTimestamp: Long
): CommandMessageObject {}