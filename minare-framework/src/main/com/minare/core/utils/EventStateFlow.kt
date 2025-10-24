package com.minare.core.utils

import io.vertx.core.Vertx
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.Message
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch

typealias StateAction = suspend (StateFlowContext) -> Unit

/**
 * A simple state machine coordinated by internal vert.x events.
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
            // All index management logic is hidden within the tracker
            val nextStateName = tracker.next()

            vertx.eventBus().send(
                "$_baseKey.$nextStateName",
                JsonObject().put("trigger", "next"),
                DeliveryOptions().setLocalOnly(true)
            )
        } else {
            // Tracker returned false in non-looping mode
            println("State Machine $eventKey finished.")
            cleanup()
        }
    }

    fun registerState(stateName: String, action: StateAction) {
        eventsMap[stateName] = action
    }

    fun start() {
        if (eventsMap.isEmpty()) return

        // Initialize the LoopingList with the ordered list of keys
        val stateNames = eventsMap.keys.toList()
        tracker = LoopingList(stateNames, looping)

        // 1. Register ALL consumers
        eventsMap.forEach { (stateName, action) ->
            val address = "$_baseKey.$stateName"

            val vertxHandler: (Message<JsonObject>) -> Unit = { _ ->

                // Set flag right before launch
                isProcessing = true

                coroutineScope.launch {
                    try {
                        action(this@EventStateFlow) // Execute state logic
                    } catch (e: Exception) {
                        // Log errors from the state action
                        System.err.println("Error executing state '$stateName' for $eventKey: ${e.message}")
                    } finally {
                        // CRUCIAL: Ensure flag is reset whether successful or failed
                        isProcessing = false
                    }
                }
            }
            consumers[address] = vertx.eventBus().localConsumer(address, vertxHandler)
        }

        // 2. Trigger the first state execution via next() (which calls triggerNextTransition)
        // We use next() here since isProcessing is initially false.
        coroutineScope.launch { next() }
    }

    fun cleanup() {
        consumers.values.forEach { it.unregister() }
        consumers.clear()
        if (::tracker.isInitialized) {
            tracker.reset()
        }
        // Ensure flag is reset on cleanup
        isProcessing = false
    }
}