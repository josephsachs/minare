package com.minare.core.storage.adapters

import com.google.inject.Singleton
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.minare.core.storage.interfaces.ContextStore
import com.minare.core.transport.models.Context
import org.slf4j.LoggerFactory
import javax.inject.Inject

@Singleton
class HazelcastContextStore @Inject constructor(
    private val hazelcast: HazelcastInstance
) : ContextStore {
    private val log = LoggerFactory.getLogger(HazelcastContextStore::class.java)
    private val map: IMap<String, Context> by lazy { hazelcast.getMap("contexts") }

    override suspend fun create(entityId: String, channelId: String): String {
        if (entityId.isBlank()) {
            throw IllegalArgumentException("Entity ID cannot be blank")
        }
        if (channelId.isBlank()) {
            throw IllegalArgumentException("Channel ID cannot be blank")
        }

        val contextId = "$entityId-$channelId"
        val context = Context(
            id = contextId,
            entityId = entityId,
            channelId = channelId,
            created = System.currentTimeMillis()
        )

        map.putIfAbsent(contextId, context)
        return contextId
    }

    override suspend fun remove(entityId: String, channelId: String): Boolean {
        val contextId = "$entityId-$channelId"
        return map.remove(contextId) != null
    }

    override suspend fun getChannelsByEntityId(entityId: String): List<String> {
        return map.values
            .filter { it.entityId == entityId }
            .map { it.channelId }
    }

    override suspend fun getEntityIdsByChannel(channelId: String): List<String> {
        return map.values
            .filter { it.channelId == channelId }
            .map { it.entityId }
    }
}