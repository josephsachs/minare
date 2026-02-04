package com.minare.core.utils.types.esf

interface StateFlowContext {
    /** * Triggers the transition to the next state unconditionally.
     * This is intended for use inside a StateAction when the action is complete.
     */
    suspend fun next()

    /** * Attempts to trigger the transition to the next state only if no state is currently processing.
     * This is intended for external, potentially recurring, triggers (like timers).
     */
    suspend fun tryNext()
}