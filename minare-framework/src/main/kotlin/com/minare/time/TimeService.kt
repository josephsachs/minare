package com.minare.time

/**
 * Service for ensuring consistent time across nodes
 */
interface TimeService {
    companion object {
        // Very tight timeout for frame sync requirements
        private const val SYNC_TIMEOUT_SECONDS = 2L
    }

    /**
     * Synchronize system clock with the authoritative source.
     * This method should block until synchronization is complete,
     * because invoking class will need to wait to send ALL OK.
     *
     * @return Boolean
     */
    suspend fun syncTime(): Boolean

    /**
     * Wraps a call to obtain the time.
     *
     * @return Long
     */
    suspend fun getTime(): Long
}