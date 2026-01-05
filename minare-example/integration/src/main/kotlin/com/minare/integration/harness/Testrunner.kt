package com.minare.integration.harness

import org.slf4j.LoggerFactory

class TestRunner(private val suiteName: String) {
    private val log = LoggerFactory.getLogger(TestRunner::class.java)
    private val results = mutableListOf<TestResult>()

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
        println("\n${"=".repeat(60)}")
        println("TEST SUITE: $suiteName")
        println("=".repeat(60))

        results.forEach { result ->
            if (result.passed) {
                println("  ✓ ${result.name} (${result.durationMs}ms)")
            } else {
                println("  ✗ ${result.name} (${result.durationMs}ms)")
                result.error?.let { error ->
                    println("    ${error.javaClass.simpleName}: ${error.message}")
                    error.stackTrace.take(3).forEach { frame ->
                        println("      at $frame")
                    }
                }
            }
        }

        val passed = results.count { it.passed }
        val failed = results.count { !it.passed }
        val totalTime = results.sumOf { it.durationMs }

        println("-".repeat(60))
        println("Results: $passed passed, $failed failed (${totalTime}ms)")
        println("=".repeat(60))

        return failed == 0
    }

    fun getResults(): List<TestResult> = results.toList()
}