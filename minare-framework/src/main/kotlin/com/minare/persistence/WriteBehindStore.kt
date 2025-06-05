package com.minare.persistence

import com.minare.core.models.Entity

interface WriteBehindStore {
    suspend fun persistForWriteBehind(entity: Entity): Entity
}