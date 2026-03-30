package com.minare.core.storage.interfaces

import io.vertx.core.json.JsonObject

/**
 * Result of an atomic mutate-and-return operation.
 * Contains before/after entity snapshots and the new version,
 * all captured within a single Redis Lua evaluation.
 */
data class MutationResult(
    val before: JsonObject,
    val after: JsonObject,
    val version: Long
)