package com.minare.integration.suites

import com.google.inject.Injector
import com.minare.integration.harness.Assertions.assertNotNull
import com.minare.integration.harness.TestRunner
import com.minare.integration.harness.TestSuite
import io.vertx.core.Vertx
import io.vertx.redis.client.RedisAPI
import io.vertx.ext.mongo.MongoClient
import io.vertx.kotlin.coroutines.await

class SmokeTestSuite(private val injector: Injector) : TestSuite {
    override val name = "Smoke Tests"

    override suspend fun run(runner: TestRunner) {
        runner.test("Injector is available") {
            assertNotNull(injector) { "Injector should not be null" }
        }

        runner.test("Vertx is available") {
            val vertx = injector.getInstance(Vertx::class.java)
            assertNotNull(vertx) { "Vertx should not be null" }
        }

        runner.test("Redis is connected") {
            val redis = injector.getInstance(RedisAPI::class.java)
            assertNotNull(redis) { "RedisAPI should not be null" }

            val pong = redis.ping(listOf("")).await()
            assertNotNull(pong) { "Redis PING should return response" }
        }

        runner.test("MongoDB is connected") {
            val mongo = injector.getInstance(MongoClient::class.java)
            assertNotNull(mongo) { "MongoClient should not be null" }

            val collections = mongo.collections.await()
            assertNotNull(collections) { "Should be able to list collections" }
        }
    }
}