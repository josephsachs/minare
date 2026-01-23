package com.minare.core.frames.services

import com.hazelcast.collection.ISet

/**
 * Interface for the distributed worker registry map.
 * This abstraction allows for cleaner dependency injection and potential
 * future implementations (e.g., for testing or different clustering solutions).
 */
interface ActiveWorkerSet {
    fun exists(workerId: String): Boolean
    fun put(workerId: String): Boolean
    fun remove(workerId: String): Boolean
    fun clear()
    fun entries(): Set<String>
}

/**
 * Hazelcast implementation of WorkerRegistrySet
 */
class HazelcastActiveWorkerSet(
    private val set: ISet<String>
) : ActiveWorkerSet {

    override fun exists(workerId: String): Boolean {
        return if (set.contains(workerId)) true else false
    }

    override fun put(workerId: String): Boolean {
        return set.add(workerId)
    }

    override fun remove(workerId: String): Boolean {
        return set.remove(workerId)
    }

    override fun clear() {
        set.clear()
    }

    override fun entries(): Set<String> {
        return set
    }
}