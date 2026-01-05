package com.minare.integration

import com.minare.core.MinareApplication
import org.slf4j.LoggerFactory

/**
 * Main entry point for running the example application
 */
object TestMain {
    private val log = LoggerFactory.getLogger(TestMain::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        log.info("Starting Minare Integration Test Application")


        MinareApplication.start(TestApplication::class.java, args)
    }
}