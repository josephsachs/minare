package com.minare.core.work

import com.google.inject.Inject
import com.google.inject.Singleton
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.minare.core.config.InternalInjectorHolder
import com.minare.core.frames.services.WorkerRegistry
import com.minare.core.utils.PushVar
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.EventWaiter
import io.vertx.core.Vertx
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

/**
 * WorkDispatchService handles distribution of work according to given strategy and command.
 */
@Singleton
class WorkDispatchService @Inject constructor(
    private val hazelcastInstance: HazelcastInstance,
    private val workerRegistry: WorkerRegistry,
    private val eventBusUtils: EventBusUtils,
    private val eventWaiter: EventWaiter
) {
    private val log = LoggerFactory.getLogger(WorkDispatchService::class.java)

    private val manifestMap: IMap<String, Map<String, Collection<Any?>>> by lazy {
        hazelcastInstance.getMap("work-dispatch-manifests")
    }

    private val eventConsumers = ConcurrentHashMap<String, MessageConsumer<JsonObject>>()
    private val completedWorkers: ConcurrentHashMap<String, MutableSet<String>> = ConcurrentHashMap()

    /**
     * Dispatch the given work
     * @param event Label to use for event name and manifest key, should be unique
     * @param strategy WorkDispatchStrategy to select method of work distribution
     * @param workUnit WorkUnit command object encapsulating prepare() and process() functions
     */
    suspend fun dispatch(
        event: String,
        strategy: WorkDispatchStrategy,
        workUnit: WorkUnit
    ) {
        val items = workUnit.prepare()

        manifestMap[event] = distribute(items, strategy)

        completedWorkers[event] = ConcurrentHashMap.newKeySet()

        eventBusUtils.publishWithTracing(
            ADDRESS_DISTRIBUTE_WORK_EVENT,
            JsonObject()
                .put("event", event)
                .put("workUnit", workUnit.javaClass.name)
        )

        eventWaiter.waitForAll("$ADDRESS_WORK_DONE_EVENT.$event")

        completedWorkers.remove(event)
        manifestMap.remove(event)
        eventConsumers.remove(event)?.unregister()

        eventBusUtils.publishWithTracing(
            "$ADDRESS_WORK_COMPLETE_EVENT.$event",
            JsonObject()
                .put("event", event)
        )
    }

    /**
     * Execute the distribution function
     */
    private suspend fun distribute(items: Collection<*>, strategy: WorkDispatchStrategy): Map<String, Collection<Any?>> {
        val workers = workerRegistry.getActiveWorkers()

        if (workers.isEmpty()) {
            log.info("WorkDispatchService: Task did not process because no workers were available, returning empty map")
            return emptyMap()
        }

        return when (strategy) {
            WorkDispatchStrategy.RANGE -> {
                if (items.isEmpty()) {
                    log.info("WorkDispatcher with strategy RANGE received no items, returning empty map")
                    return emptyMap()
                }

                val chunkSize = (items.size + workers.size - 1) / workers.size
                items.chunked(chunkSize)
                    .mapIndexed { index, chunk -> workers.toList()[index] to chunk }
                    .toMap()
            }
            WorkDispatchStrategy.CONSISTENT_HASH -> {
                throw IllegalStateException("Next time")
            }
            WorkDispatchStrategy.SCOPE -> {
                throw IllegalStateException("Even later")
            }
            WorkDispatchStrategy.UNIFORM -> {
                workers.associateWith { items }
            }
        }
    }

    /**
     * Obtain manifest and process work for this worker
     * @param message Event message containing event name and workUnit class
     * @param workerKey The registered name of this worker
     */
    suspend fun workerHandle(message: JsonObject, workerKey: String): Any? {
        val event = message.getString("event")
        val className = message.getString("workUnit")

        val workUnitClass = Class.forName(className) as Class<out WorkUnit>
        val workUnit = InternalInjectorHolder.getInjector().getInstance(workUnitClass)

        val manifestItems = manifestMap[event]?.get(workerKey)?.filterNotNull() ?: emptyList()

        val result = manifestItems.let {
            workUnit.process(it)
        }

        eventBusUtils.publishWithTracing(
            "$ADDRESS_WORK_DONE_EVENT.$event",
            JsonObject().put("workerId", workerKey)
        )

        return result
    }

    companion object {
        const val ADDRESS_DISTRIBUTE_WORK_EVENT = "work.dispatcher.distribute.work.event"
        const val ADDRESS_WORK_DONE_EVENT = "work.dispatcher.work.done.event"
        const val ADDRESS_WORK_COMPLETE_EVENT = "work.dispatcher.work.complete"

        enum class WorkDispatchStrategy {
            RANGE,
            CONSISTENT_HASH,
            SCOPE,
            UNIFORM
        }
    }
}