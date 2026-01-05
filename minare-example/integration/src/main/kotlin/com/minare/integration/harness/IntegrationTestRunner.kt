package com.minare.integration.harness

import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

class IntegrationTestRunner {
    private val log = LoggerFactory.getLogger(IntegrationTestRunner::class.java)
    private val suiteResults = mutableListOf<Pair<String, Boolean>>()

    suspend fun runAll(vararg suites: TestSuite): Boolean {
        println("\n")
        println("╔════════════════════════════════════════════════════════════╗")
        println("║           MINARE INTEGRATION TEST SUITE                    ║")
        println("╚════════════════════════════════════════════════════════════╝")

        var allPassed = true

        for (suite in suites) {
            val runner = TestRunner(suite.name)
            try {
                suite.run(runner)
            } catch (e: Throwable) {
                log.error("Suite ${suite.name} threw unexpected error", e)
                println("  ✗ Suite crashed: ${e.javaClass.simpleName}: ${e.message}")
            }
            val suitePassed = runner.report()
            suiteResults.add(suite.name to suitePassed)
            if (!suitePassed) allPassed = false
        }

        printSummary()
        return allPassed
    }

    private fun printSummary() {
        println("\n")
        println("╔════════════════════════════════════════════════════════════╗")
        println("║                    SUMMARY                                 ║")
        println("╚════════════════════════════════════════════════════════════╝")

        suiteResults.forEach { (name, passed) ->
            val status = if (passed) "✓ PASS" else "✗ FAIL"
            println("  $status  $name")
        }

        val passedCount = suiteResults.count { it.second }
        val failedCount = suiteResults.count { !it.second }

        println("")
        if (failedCount == 0) {
            println("  All $passedCount test suites passed!")
        } else {
            println("  $passedCount passed, $failedCount failed")
        }
        println("")
    }

    suspend fun runAllAndExit(vararg suites: TestSuite) {
        val passed = runAll(*suites)
        if (passed) {
            log.info("All tests passed, exiting with code 0")
            exitProcess(0)
        } else {
            log.error("Some tests failed, exiting with code 1")
            exitProcess(1)
        }
    }
}