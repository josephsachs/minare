package com.minare.core.storage.interfaces

import io.vertx.core.json.JsonObject

interface WriteBehindStore {
    /**
     * Persists an entity document for write-behind storage.
     * Updated to use JsonObject for consistency with the framework's JsonObject-first approach.
     *
     * @param entityDocument The entity document to persist (with _id, type, version, state)
     * @return The persisted entity document
     */
    suspend fun persistForWriteBehind(entityDocument: JsonObject): JsonObject
}