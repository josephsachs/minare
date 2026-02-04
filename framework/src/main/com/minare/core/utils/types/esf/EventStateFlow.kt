package com.minare.core.utils.types.esf

import com.minare.core.utils.types.LoopingList
import io.vertx.core.Vertx
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.Message
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch

typealias StateAction = suspend (StateFlowContext) -> Unit

/**
 * A simple state machine coordinated over the event bus.
 *
 * The supplied step functions run in invoking class's scope,
 * in the verticle context where the instance was created.
 */
class EventStateFlow(
    private val eventKey: String,
    private val coroutineScope: CoroutineScope,
    private val vertx: Vertx,
    private val looping: Boolean = false
) : StateFlowContext {
    private var _baseKey: String = "event.state.machine.$eventKey"

    @Volatile
    private var isProcessing: Boolean = false

    private val eventsMap = linkedMapOf<String, StateAction>()
    private lateinit var tracker: LoopingList<String>

    private val consumers = mutableMapOf<String, MessageConsumer<JsonObject>>()

    /**
     * Implementation of next(): Always proceeds immediately.
     */
    override suspend fun next() {
        triggerNextTransition()
    }

    /**
     * Implementation of tryNext(): Only proceeds if the state machine is not processing.
     */
    override suspend fun tryNext() {
        if (isProcessing) {
            println("State Machine $eventKey is busy processing a state, skipping transition.")
            return
        }
        triggerNextTransition()
    }

    /**
     * Internal logic to check the tracker, get the next state name, and send the event.
     */
    private suspend fun triggerNextTransition() {
        if (!::tracker.isInitialized) return

        if (tracker.hasNext()) {
            val nextStateName = tracker.next()

            vertx.eventBus().send(
                "$_baseKey.$nextStateName",
                JsonObject().put("trigger", "next"),
                DeliveryOptions().setLocalOnly(true)
            )
        } else {
            println("State Machine $eventKey finished.")
            cleanup()
        }
    }

    fun registerState(stateName: String, action: StateAction) {
        eventsMap[stateName] = action
    }

    fun start() {
        if (eventsMap.isEmpty()) return

        val stateNames = eventsMap.keys.toList()
        tracker = LoopingList(stateNames, looping)

        eventsMap.forEach { (stateName, action) ->
            val address = "$_baseKey.$stateName"

            val vertxHandler: (Message<JsonObject>) -> Unit = { _ ->
                isProcessing = true

                coroutineScope.launch {
                    try {
                        action(this@EventStateFlow)
                    } catch (e: Exception) {
                        System.err.println("Error executing state '$stateName' for $eventKey: ${e.message}")
                    } finally {
                        isProcessing = false
                    }
                }
            }
            consumers[address] = vertx.eventBus().localConsumer(address, vertxHandler)
        }

        coroutineScope.launch { next() }
    }

    fun cleanup() {
        consumers.values.forEach { it.unregister() }
        consumers.clear()
        if (::tracker.isInitialized) {
            tracker.reset()
        }

        isProcessing = false
    }
}