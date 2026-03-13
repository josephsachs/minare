package com.minare.integration.models

import com.google.inject.Inject
import com.minare.core.entity.annotations.*
import com.minare.core.entity.models.Entity
import com.minare.core.storage.interfaces.StateStore
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

/**
 * Test entity for OperationSet predicate tests.
 *
 * @Property fields serve as the observability layer: each @FunctionCall,
 * @Assert and @Trigger records what it saw into the entity's properties,
 * then persists them via StateStore. Tests read properties after set
 * execution to verify hydration, ordering, context passing and timing.
 */
@EntityType("SetTestEntity")
class SetTestEntity @Inject constructor(
    private val stateStore: StateStore
) : Entity() {
    private val log = LoggerFactory.getLogger(SetTestEntity::class.java)

    init {
        type = "SetTestEntity"
    }

    // ── State ───────────────────────────────────────────────────────────────

    @State
    @Mutable
    var label: String = ""

    @State
    @Mutable
    var value: Int = 0

    // ── Properties (observability) ──────────────────────────────────────────

    /** Ordered log of steps that executed, with timestamps and context snapshots. */
    @Property
    var stepLog: MutableList<String> = mutableListOf()

    /** Timestamps keyed by step name — tests can verify ordering and measure duration. */
    @Property
    var timestamps: MutableMap<String, String> = mutableMapOf()

    /** Last context received by a predicate — tests verify pipe is flowing. */
    @Property
    var lastContext: String = ""

    /** Whether hydration was intact when each predicate ran. */
    @Property
    var hydrationChecks: MutableMap<String, String> = mutableMapOf()

    // ── @FunctionCall predicates ────────────────────────────────────────────

    /**
     * Computes a value and returns it to the pipe.
     * Records hydration state, timestamp, and what it computed.
     */
    @FunctionCall
    suspend fun compute(input: JsonObject?): JsonObject {
        val now = System.currentTimeMillis()
        val hydrated = _id != null && type == "SetTestEntity"

        hydrationChecks["compute"] = hydrated.toString()
        timestamps["compute"] = now.toString()

        val operand = input?.getInteger("operand") ?: value
        val result = operand * 2

        stepLog.add("compute:$result@$now")
        lastContext = input?.encode() ?: ""

        persistProperties()

        return JsonObject()
            .put("result", result)
            .put("entityId", _id)
            .put("computedAt", now)
    }

    /**
     * Reads the current entity state and returns it — useful for
     * an Assert in the next step that depends on this function's output.
     */
    @FunctionCall
    suspend fun snapshot(): JsonObject {
        val now = System.currentTimeMillis()
        hydrationChecks["snapshot"] = (_id != null).toString()
        timestamps["snapshot"] = now.toString()
        stepLog.add("snapshot:label=$label,value=$value@$now")

        persistProperties()

        return JsonObject()
            .put("label", label)
            .put("value", value)
            .put("entityId", _id)
    }

    // ── @Assert predicates ──────────────────────────────────────────────────

    /**
     * Asserts that a piped result matches expected value.
     * Receives the pipe from a prior FunctionCall.
     */
    @Assert
    suspend fun checkResult(context: JsonObject?): Boolean {
        val now = System.currentTimeMillis()
        hydrationChecks["checkResult"] = (_id != null).toString()
        timestamps["checkResult"] = now.toString()
        lastContext = context?.encode() ?: ""

        val result = context?.getInteger("result")
        val expected = context?.getInteger("expected") ?: (value * 2)
        val pass = result != null && result == expected

        stepLog.add("checkResult:result=$result,expected=$expected,pass=$pass@$now")

        persistProperties()
        return pass
    }

    /**
     * Always fails — used to test FailurePolicy (ABORT / ROLLBACK).
     */
    @Assert
    suspend fun alwaysFail(context: JsonObject?): Boolean {
        val now = System.currentTimeMillis()
        timestamps["alwaysFail"] = now.toString()
        stepLog.add("alwaysFail@$now")
        lastContext = context?.encode() ?: ""

        persistProperties()
        return false
    }

    /**
     * Always passes — used as a control or to verify assert execution.
     */
    @Assert
    suspend fun alwaysPass(context: JsonObject?): Boolean {
        val now = System.currentTimeMillis()
        timestamps["alwaysPass"] = now.toString()
        stepLog.add("alwaysPass@$now")

        persistProperties()
        return true
    }

    // ── @Trigger predicates ─────────────────────────────────────────────────

    /**
     * Fire-and-forget side effect. Records that it was invoked,
     * verifies hydration from the trigger's async context.
     */
    @Trigger
    suspend fun onTriggered(context: JsonObject?) {
        val now = System.currentTimeMillis()
        hydrationChecks["onTriggered"] = (_id != null && type == "SetTestEntity").toString()
        timestamps["onTriggered"] = now.toString()
        stepLog.add("onTriggered@$now")
        lastContext = context?.encode() ?: ""

        persistProperties()
    }

    // ── Internals ───────────────────────────────────────────────────────────

    private suspend fun persistProperties() {
        val id = _id ?: return
        val props = JsonObject()
            .put("stepLog", stepLog)
            .put("timestamps", timestamps)
            .put("lastContext", lastContext)
            .put("hydrationChecks", hydrationChecks)
        stateStore.saveProperties(id, props)
    }
}
