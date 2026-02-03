package com.minare.core.transport.models

import java.io.Serializable

data class Channel(
    val id: String,
    val clients: Set<String>,
    val created: Long
) : Serializable