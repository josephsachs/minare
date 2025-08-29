package com.minare.time

import kotlinx.coroutines.delay
import org.slf4j.LoggerFactory

/**
 * Simple implementation that reference's Docker Desktop's internal VM.
 *
 * In a perfect world this would not be necessary, but macOS is terrible
 * and this is the cleanest workaround.
 */
class DockerTimeService : TimeService {
    private val log = LoggerFactory.getLogger(DockerTimeService::class.java)

    override suspend fun syncTime(): Boolean {
        log.info("Pretending to sync...")
        delay(250) // Simulate reasonable latency for this command

        return true
    }

    override suspend fun getTime(): Long {
        /**
         * Docker desktop containers share a clock, so this returns synchronized
         * time by default. Wrap in a function to simulate latency if desired.
         */
        return System.currentTimeMillis()
    }
}