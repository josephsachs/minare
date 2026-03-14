package com.minare.integration.models

import com.minare.core.entity.annotations.EntityType
import com.minare.core.entity.annotations.Mutable
import com.minare.core.entity.annotations.State
import com.minare.core.entity.annotations.FunctionCall
import com.minare.core.entity.annotations.Assert
import com.minare.core.entity.annotations.Trigger
import com.minare.core.entity.models.Entity

/**
 * Entity used by OperationSetTestSuite.
 *
 * counter and label are mutable state targeted by MUTATE steps.
 * Annotated methods are invoked by the OperationSetExecutor via reflection,
 * gated by the corresponding annotation (@FunctionCall, @Assert, @Trigger).
 */
@EntityType("SetTestEntity")
class SetTestEntity : Entity() {

    init { type = "SetTestEntity" }

    @State @Mutable
    var counter: Int = 0

    @State @Mutable
    var label: String = ""

    // ── @FunctionCall methods ─────────────────────────────────────────────

    /** Returns the current counter value as step context for a following step. */
    @FunctionCall
    fun readCounter(): Int = counter

    /** Doubles the counter in-place and returns the new value. */
    @FunctionCall
    fun doubleCounter(): Int {
        counter *= 2
        return counter
    }

    // ── @Assert methods ───────────────────────────────────────────────────

    /** True when counter is strictly positive. */
    @Assert
    fun isPositive(): Boolean = counter > 0

    /** True when counter is strictly negative. */
    @Assert
    fun isNegative(): Boolean = counter < 0

    /** True when label has been set to a non-blank string. */
    @Assert
    fun hasLabel(): Boolean = label.isNotBlank()

    /** Always fails — used to test failure policies. */
    @Assert
    fun alwaysFail(): Boolean = false

    // ── @Trigger methods ──────────────────────────────────────────────────

    /** Fire-and-forget side-effect: sets label to a marker value. */
    @Trigger
    fun markTriggered() {
        label = "triggered"
    }
}
