package com.minare.nodegraph

import com.minare.nodegraph.NodeGraphApplication
import com.minare.core.MinareApplication
import org.slf4j.LoggerFactory

/**
 * Main entry point for running the example application
 */
object Main {
    private val log = LoggerFactory.getLogger(Main::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        log.info("Starting NodeGraph Application")
        MinareApplication.start(NodeGraphApplication::class.java, args)
    }
}