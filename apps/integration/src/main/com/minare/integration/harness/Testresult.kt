package com.minare.integration.harness

data class TestResult(
    val name: String,
    val passed: Boolean,
    val durationMs: Long = 0,
    val error: Throwable? = null
)