package com.minare.core.transport.models

import java.io.Serializable

data class Context(
    val id: String,
    val entityId: String,
    val channelId: String,
    val created: Long
) : Serializable