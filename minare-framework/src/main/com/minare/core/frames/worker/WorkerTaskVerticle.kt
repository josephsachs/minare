package com.minare.core.frames.worker

import com.google.inject.Inject
import com.minare.core.entity.annotations.Task
import com.minare.core.work.WorkDispatchService
import com.minare.core.work.WorkDispatchService.Companion.ADDRESS_DISTRIBUTE_WORK_EVENT
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory


class WorkerTaskVerticle @Inject constructor(
    private val workDispatchService: WorkDispatchService,
    private val vertx: Vertx
): CoroutineVerticle() {
    private val log = LoggerFactory.getLogger(WorkerTaskVerticle::class.java)

    private lateinit var workerId: String

    override suspend fun start() {
        workerId = config.getString("workerId")

        registerListener()
    }

    suspend fun registerListener() {
        vertx.eventBus().consumer(ADDRESS_DISTRIBUTE_WORK_EVENT) { message ->
            launch {
                workDispatchService.workerHandle(message.body(), workerId)
            }
        }
    }
}