package com.minare.example

import com.minare.example.ExampleApplication
import com.minare.core.MinareApplication
import org.slf4j.LoggerFactory

/**
 * Main entry point for running the example application
 */
object ExampleMain {
    private val log = LoggerFactory.getLogger(ExampleMain::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        log.info("Starting Minare Example Application")


        MinareApplication.start(ExampleApplication::class.java, args)
    }
}