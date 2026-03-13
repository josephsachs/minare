package com.minare.integration.models

import com.minare.core.entity.annotations.EntityType
import com.minare.core.entity.annotations.Mutable
import com.minare.core.entity.annotations.State
import com.minare.core.entity.models.Entity

/**
 * Entity used by OperationSetTestSuite.
 *
 * counter and label are mutable state targeted by MUTATE steps.
 * The methods below are invoked directly by FunctionCall and Assert members,
 * allowing tests to observe in-process results without involving the framework layer.
 */
@EntityType("SetTestEntity")
class SetTestEntity : Entity() {

    init { type = "SetTestEntity" }

    @State @Mutable
    var counter: Int = 0

    @State @Mutable
    var label: String = ""

    /** Returns the current counter value — useful as step context for a following Assert. */
    fun getCounter(): Int = counter

    /** True when counter is strictly positive. */
    fun isPositive(): Boolean = counter > 0

    /** True when counter is strictly negative. */
    fun isNegative(): Boolean = counter < 0

    /** True when label has been set to a non-blank string. */
    fun hasLabel(): Boolean = label.isNotBlank()
}
