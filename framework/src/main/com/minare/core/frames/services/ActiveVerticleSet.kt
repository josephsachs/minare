package com.minare.core.frames.services

import com.hazelcast.collection.ISet

/**
 * Interface for the distributed active verticle set.
 * Tracks which verticle instances are currently active for frame routing.
 */
interface ActiveVerticleSet {
    fun exists(instanceId: String): Boolean
    fun put(instanceId: String): Boolean
    fun remove(instanceId: String): Boolean
    fun clear()
    fun entries(): Set<String>
}

/**
 * Hazelcast implementation of ActiveVerticleSet
 */
class HazelcastActiveVerticleSet(
    private val set: ISet<String>
) : ActiveVerticleSet {

    override fun exists(instanceId: String): Boolean {
        return set.contains(instanceId)
    }

    override fun put(instanceId: String): Boolean {
        return set.add(instanceId)
    }

    override fun remove(instanceId: String): Boolean {
        return set.remove(instanceId)
    }

    override fun clear() {
        set.clear()
    }

    override fun entries(): Set<String> {
        return set
    }
}
