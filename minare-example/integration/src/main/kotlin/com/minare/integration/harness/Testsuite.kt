package com.minare.integration.harness

interface TestSuite {
    val name: String
    suspend fun run(runner: TestRunner)
}