package com.minare.integration.harness

import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

class IntegrationTestRunner {
    private val log = LoggerFactory.getLogger(IntegrationTestRunner::class.java)
    private val testLog = LoggerFactory.getLogger("TEST_OUTPUT")
    private val suiteResults = mutableListOf<Pair<String, Boolean>>()

    private fun out(message: String) {
        testLog.info(message)
    }

    suspend fun runAll(vararg suites: TestSuite): Boolean {
        out("")
        out("╔════════════════════════════════════════════════════════════╗")
        out("║           MINARE INTEGRATION TEST SUITE                    ║")
        out("╚════════════════════════════════════════════════════════════╝")

        var allPassed = true

        for (suite in suites) {
            val runner = TestRunner(suite.name, testLog)
            try {
                suite.run(runner)
            } catch (e: Throwable) {
                log.error("Suite ${suite.name} threw unexpected error", e)
                out("  ✗ Suite crashed: ${e.javaClass.simpleName}: ${e.message}")
            }
            val suitePassed = runner.report()
            suiteResults.add(suite.name to suitePassed)
            if (!suitePassed) allPassed = false
        }

        printSummary()
        return allPassed
    }

    private fun printSummary() {
        out("")
        out("╔════════════════════════════════════════════════════════════╗")
        out("║                    SUMMARY                                 ║")
        out("╚════════════════════════════════════════════════════════════╝")

        suiteResults.forEach { (name, passed) ->
            val status = if (passed) "✓ PASS" else "✗ FAIL"
            out("  $status  $name")
        }

        val passedCount = suiteResults.count { it.second }
        val failedCount = suiteResults.count { !it.second }

        out("")
        if (failedCount == 0) {
            out("  All $passedCount test suites passed!")
        } else {
            out("  $passedCount passed, $failedCount failed")
        }
        out("")
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