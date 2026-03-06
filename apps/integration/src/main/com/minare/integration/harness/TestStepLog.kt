package com.minare.integration.harness

/**
 * Lightweight step recorder for integration tests.
 * Created per test by TestRunner; populated by the test body; emitted on failure only.
 */
class TestStepLog {

    data class Entry(
        val stepName: String,
        val elapsedMs: Long,
        val thread: String,
        val context: Map<String, String>,
        val exception: Throwable?
    )

    private val startedAt = System.currentTimeMillis()
    private val entries = mutableListOf<Entry>()

    /** Record a named step with optional key-value context. */
    fun step(name: String, vararg context: Pair<String, Any?>) {
        entries += Entry(
            stepName = name,
            elapsedMs = System.currentTimeMillis() - startedAt,
            thread = Thread.currentThread().name,
            context = context.associate { (k, v) -> k to (v?.toString() ?: "null") },
            exception = null
        )
    }

    /** Record a caught exception at a named step. */
    fun caught(name: String, e: Throwable, vararg context: Pair<String, Any?>) {
        entries += Entry(
            stepName = "!! $name",
            elapsedMs = System.currentTimeMillis() - startedAt,
            thread = Thread.currentThread().name,
            context = context.associate { (k, v) -> k to (v?.toString() ?: "null") },
            exception = e
        )
    }

    fun hasEntries(): Boolean = entries.isNotEmpty()

    /** Format as lines for appending to the failure report. */
    fun format(): List<String> {
        if (entries.isEmpty()) return emptyList()

        val lines = mutableListOf("  Step log:")
        for (entry in entries) {
            lines += "    +${entry.elapsedMs}ms  [${entry.thread}]  ${entry.stepName}"
            entry.context.forEach { (k, v) ->
                lines += "        $k = $v"
            }
            entry.exception?.let { ex ->
                lines += "        ${ex.javaClass.simpleName}: ${ex.message}"
                ex.stackTrace.take(4).forEach { frame ->
                    lines += "          at $frame"
                }
                ex.cause?.let { cause ->
                    lines += "        caused by: ${cause.javaClass.simpleName}: ${cause.message}"
                }
            }
        }
        return lines
    }
}
