package com.minare.worker.coordinator

import com.google.inject.Inject
import com.google.inject.Singleton
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import java.io.Serializable

/**
 * BackpressureManager handles backpressure state. Occurs under conditions such as
 * - Coordinator is too many steps ahead of workers and needs nard pause
 *
 * Causes 503 status to incoming upsocket messages for client to handle.
 *
 * TODO: Re-implement after frame loop behavior is fully validated.
 */
@Singleton
class BackpressureManager @Inject constructor(
    private val hazelcastInstance: HazelcastInstance
) {
    private val backpressureMap: IMap<String, BackpressureState> by lazy {
        hazelcastInstance.getMap("backpressure-state")
    }

    data class BackpressureState(
        val active: Boolean,
        val activatedAt: Long,
        val activatedAtFrame: Long,
        val reason: String,
        val bufferedOperations: Int,
        val maxBufferSize: Int
    ): Serializable

    fun isActive(): Boolean {
        return backpressureMap["global"]?.active ?: false
    }

    fun activate(frame: Long, bufferedOps: Int, maxBuffer: Int) {
        backpressureMap["global"] = BackpressureState(
            active = true,
            activatedAt = System.currentTimeMillis(),
            activatedAtFrame = frame,
            reason = "Buffer limit reached",
            bufferedOperations = bufferedOps,
            maxBufferSize = maxBuffer
        )
    }

    fun deactivate() {
        val current = backpressureMap["global"] ?: return
        backpressureMap["global"] = current.copy(active = false)
    }

    fun getBackpressureState(): BackpressureState? {
        return backpressureMap["global"]
    }
}