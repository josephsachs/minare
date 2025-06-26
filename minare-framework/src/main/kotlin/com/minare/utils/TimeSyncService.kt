package com.minare.utils

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.TimeUnit

/**
 * Service for synchronizing system clock with NTP servers.
 * Used to ensure conductor and orchestra nodes start and end frames at precisely the same time.
 *
 * This service blocks until synchronization is complete or fails hard if sync cannot be achieved.
 * Nodes should call syncTime() before signaling readiness to participate in frame coordination.
 */
class TimeSyncService {
    private val log = LoggerFactory.getLogger(TimeSyncService::class.java)

    private val ntpUrl = System.getenv("NTP_URL")
        ?: throw IllegalStateException("NTP_URL environment variable is required for frame synchronization")

    companion object {
        // Very tight timeout for frame sync requirements
        private const val SYNC_TIMEOUT_SECONDS = 2L
    }

    /**
     * Synchronize system clock with NTP server.
     * This method blocks until synchronization is complete.
     *
     * @throws RuntimeException if time synchronization fails - this should trigger conductor pause
     */
    suspend fun syncTime() {
        log.info("Starting time synchronization with NTP server: $ntpUrl")

        try {
            withContext(Dispatchers.IO) {
                val result = executeChronySyncCommand()

                if (result.exitCode == 0) {
                    log.info("Time synchronization completed successfully")
                } else {
                    val errorMessage = "Time sync failed - conductor must pause. " +
                            "Exit code: ${result.exitCode}, Error: ${result.errorOutput}"
                    log.error(errorMessage)
                    throw RuntimeException(errorMessage)
                }
            }
        } catch (e: Exception) {
            val errorMessage = "Time sync failed - conductor must pause. Error: ${e.message}"
            log.error(errorMessage, e)
            throw RuntimeException(errorMessage, e)
        }
    }

    /**
     * Execute chrony synchronization command with tight timeout.
     * Uses chronyd's one-shot sync mode to adjust system clock.
     */
    private fun executeChronySyncCommand(): CommandResult {
        // Use chronyd in one-shot mode to sync with specified NTP server
        // -q: quit after clock is set once
        // -t: timeout for server response
        val command = arrayOf(
            "chronyd",
            "-q",                           // One-shot mode
            "-t", SYNC_TIMEOUT_SECONDS.toString(),  // Timeout
            "server", ntpUrl               // NTP server to use
        )

        log.debug("Executing time sync command: ${command.joinToString(" ")}")

        val process = ProcessBuilder(*command)
            .redirectErrorStream(false)
            .start()

        val finished = process.waitFor(SYNC_TIMEOUT_SECONDS + 1, TimeUnit.SECONDS)

        if (!finished) {
            process.destroyForcibly()
            throw IOException("Time sync command timed out after ${SYNC_TIMEOUT_SECONDS + 1} seconds")
        }

        val stdout = process.inputStream.bufferedReader().readText()
        val stderr = process.errorStream.bufferedReader().readText()

        return CommandResult(
            exitCode = process.exitValue(),
            output = stdout,
            errorOutput = stderr
        )
    }

    /**
     * Result of executing a system command.
     */
    private data class CommandResult(
        val exitCode: Int,
        val output: String,
        val errorOutput: String
    )
}