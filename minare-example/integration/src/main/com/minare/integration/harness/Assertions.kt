package com.minare.integration.harness

object Assertions {

    fun assertTrue(condition: Boolean, message: () -> String = { "Expected true" }) {
        if (!condition) {
            throw AssertionError(message())
        }
    }

    fun assertFalse(condition: Boolean, message: () -> String = { "Expected false" }) {
        if (condition) {
            throw AssertionError(message())
        }
    }

    fun <T> assertNotNull(value: T?, message: () -> String = { "Expected non-null value" }): T {
        if (value == null) {
            throw AssertionError(message())
        }
        return value
    }

    fun <T> assertNull(value: T?, message: () -> String = { "Expected null value" }) {
        if (value != null) {
            throw AssertionError(message())
        }
    }

    fun <T> assertEquals(expected: T, actual: T, message: () -> String = { "Expected $expected but got $actual" }) {
        if (expected != actual) {
            throw AssertionError(message())
        }
    }

    fun <T> assertNotEquals(unexpected: T, actual: T, message: () -> String = { "Expected value different from $unexpected" }) {
        if (unexpected == actual) {
            throw AssertionError(message())
        }
    }

    fun assertContains(haystack: String, needle: String, message: () -> String = { "Expected '$haystack' to contain '$needle'" }) {
        if (!haystack.contains(needle)) {
            throw AssertionError(message())
        }
    }

    fun assertStartsWith(value: String, prefix: String, message: () -> String = { "Expected '$value' to start with '$prefix'" }) {
        if (!value.startsWith(prefix)) {
            throw AssertionError(message())
        }
    }

    fun assertNotStartsWith(value: String, prefix: String, message: () -> String = { "Expected '$value' to not start with '$prefix'" }) {
        if (value.startsWith(prefix)) {
            throw AssertionError(message())
        }
    }

    fun <T> assertSize(collection: Collection<T>, expectedSize: Int, message: () -> String = { "Expected size $expectedSize but got ${collection.size}" }) {
        if (collection.size != expectedSize) {
            throw AssertionError(message())
        }
    }

    fun <T> assertNotEmpty(collection: Collection<T>, message: () -> String = { "Expected non-empty collection" }) {
        if (collection.isEmpty()) {
            throw AssertionError(message())
        }
    }

    fun assertGreaterThan(actual: Long, threshold: Long, message: () -> String = { "Expected $actual > $threshold" }) {
        if (actual <= threshold) {
            throw AssertionError(message())
        }
    }

    fun fail(message: String): Nothing {
        throw AssertionError(message)
    }
}