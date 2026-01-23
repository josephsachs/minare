package com.minare.core.work

/**
 * Interface defines distributed work for WorkDispatchService.
 * Preparation occurs in Coordinator context.
 * Processing runs in Worker context
 */
interface WorkUnit {
    /**
     * Prepare work to be distributed across workers
     * @return typically Collection<Any> of work items to distribute
     */
    suspend fun prepare(): Collection<Any>
    /**
     * Process the assigned work items
     * @param items work items assigned to this worker
     * @return result of processing (implementation-specific)
     */
    suspend fun process(items: Collection<Any>): Any?
}