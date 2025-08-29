package com.minare.core.config

/**
 * Interface for modules that provide a database name.
 * Application modules can implement this interface to specify
 * which database they want to use.
 */
interface DatabaseNameProvider {
    /**
     * Get the database name for this application.
     * @return The name of the database to use.
     */
    fun getDatabaseName(): String
}