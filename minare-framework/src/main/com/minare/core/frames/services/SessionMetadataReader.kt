package com.minare.core.frames.services

import com.google.inject.Inject
import com.minare.core.operation.interfaces.MessageQueue
import io.vertx.core.json.JsonObject

class SessionMetadataReader @Inject constructor(
  private val messageQueue: MessageQueue
) {
  fun getMetadata(sessionId: String): JsonObject {
    // Implement here
    return JsonObject()
  }
}