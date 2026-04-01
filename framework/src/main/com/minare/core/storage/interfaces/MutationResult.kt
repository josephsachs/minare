package com.minare.core.storage.interfaces

import io.vertx.core.json.JsonObject

/**
 * Result of an atomic mutate-and-return operation.
 * Returned by StateStore.mutateAndReturn() to distinguish between
 * a successful mutation, a version policy rejection, and a missing entity (null).
 */
sealed interface MutationResult {
    data class Success(
        val before: JsonObject,
        val after: JsonObject,
        val version: Long
    ) : MutationResult

    data class VersionRejected(
        val currentVersion: Long
    ) : MutationResult
}