package com.minare.integration.harness

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class TestRunner(private val suiteName: String, private val testLog: Logger) {
    private val results = mutableListOf<TestResult>()

    private fun out(message: String) {
        testLog.info(message)
    }

    suspend fun test(name: String, block: suspend () -> Unit) {
        val startTime = System.currentTimeMillis()
        try {
            block()
            val duration = System.currentTimeMillis() - startTime
            results.add(TestResult(name, passed = true, durationMs = duration))
        } catch (e: AssertionError) {
            val duration = System.currentTimeMillis() - startTime
            results.add(TestResult(name, passed = false, durationMs = duration, error = e))
        } catch (e: Throwable) {
            val duration = System.currentTimeMillis() - startTime
            results.add(TestResult(name, passed = false, durationMs = duration, error = e))
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

    fun getResults(): List<TestResult> = results.toList()
}