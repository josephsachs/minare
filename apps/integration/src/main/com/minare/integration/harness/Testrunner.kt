package com.minare.integration.harness

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class TestRunner(private val suiteName: String, private val testLog: Logger) {
    private val results = mutableListOf<TestResult>()

    private fun out(message: String) {
        testLog.info(message)
    }

    suspend fun test(name: String, block: suspend (TestStepLog) -> Unit) {
        val stepLog = TestStepLog()
        val startTime = System.currentTimeMillis()
        try {
            block(stepLog)
            val duration = System.currentTimeMillis() - startTime
            results.add(TestResult(name, passed = true, durationMs = duration))
        } catch (e: AssertionError) {
            val duration = System.currentTimeMillis() - startTime
            results.add(TestResult(name, passed = false, durationMs = duration, error = e, stepLog = stepLog))
        } catch (e: Throwable) {
            val duration = System.currentTimeMillis() - startTime
            results.add(TestResult(name, passed = false, durationMs = duration, error = e, stepLog = stepLog))
        }
    }

    fun report(): Boolean {
        out("")
        out("=".repeat(60))
        out("TEST SUITE: $suiteName")
        out("=".repeat(60))

        results.forEach { result ->
            if (result.passed) {
                out("  ✓ ${result.name} (${result.durationMs}ms)")
            } else {
                out("  ✗ ${result.name} (${result.durationMs}ms)")
                result.error?.let { error ->
                    out("    ${error.javaClass.simpleName}: ${error.message}")
                    error.stackTrace.take(3).forEach { frame ->
                        out("      at $frame")
                    }
                }
            }
        }

        val passed = results.count { it.passed }
        val failed = results.count { !it.passed }
        val totalTime = results.sumOf { it.durationMs }

        out("-".repeat(60))
        out("Results: $passed passed, $failed failed (${totalTime}ms)")
        out("=".repeat(60))

        return failed == 0
    }

    /**
     * Emit step logs for all failed tests.
     * Called by IntegrationTestRunner after all suites complete.
     * No output if there are no failures with recorded steps.
     */
    fun reportFailureDetails() {
        val failures = results.filter { !it.passed && it.stepLog?.hasEntries() == true }
        if (failures.isEmpty()) return

        out("  Failure details — $suiteName:")
        failures.forEach { result ->
            out("  ┄ ${result.name}")
            result.stepLog?.format()?.forEach { line -> out(line) }
        }
    }

    fun getResults(): List<TestResult> = results.toList()
    fun hasFailed(): Boolean = results.any { !it.passed }
}
