package com.minare.example

import com.minare.MinareApplication
import org.slf4j.LoggerFactory

/**
 * Main entry point for running the example application
 */
object ExampleMain {
    private val log = LoggerFactory.getLogger(ExampleMain::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        log.info("Starting Minare Example Application")

        // Start the application using the standard framework startup method
        MinareApplication.start(ExampleApplication::class.java, args)
    }
}