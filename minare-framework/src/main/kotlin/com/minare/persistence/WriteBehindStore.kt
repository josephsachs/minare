package com.minare.persistence

import com.minare.core.models.Entity

interface WriteBehindStore {
    /**
     * Updates the version numbers for multiple entities
     * @param entity entity to persist for write-behind
     * @return Entity persisted object
     */
    suspend fun persistForWriteBehind(entity: Entity): Entity
}