package com.minare.config

import com.google.inject.Injector

/**
 * This class provides a way to access the application's Guice Injector without
 * having to directly inject the Injector itself, which is not allowed by Guice.
 *
 * Should only be used in very specific scenarios where injector access is absolutely necessary.
 */
object InternalInjectorHolder {
    private var injector: Injector? = null

    /**
     * Sets the application injector
     */
    fun setInjector(value: Injector) {
        injector = value
    }

    /**
     * Gets the application injector
     * @throws IllegalStateException if the injector hasn't been set
     */
    fun getInjector(): Injector {
        return injector ?: throw IllegalStateException("Injector has not been initialized")
    }

    /**
     * Gets an instance of the specified type from the injector
     * @throws IllegalStateException if the injector hasn't been set
     */
    inline fun <reified T> getInstance(): T {
        return getInjector().getInstance(T::class.java)
    }
}