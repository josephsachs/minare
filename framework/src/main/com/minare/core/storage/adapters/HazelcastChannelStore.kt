package com.minare.core.storage.adapters

import com.google.inject.Singleton
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.minare.core.storage.interfaces.ChannelStore
import com.minare.core.transport.models.Channel
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.util.UUID
import javax.inject.Inject

@Singleton
class HazelcastChannelStore @Inject constructor(
    private val hazelcast: HazelcastInstance
) : ChannelStore {
    private val log = LoggerFactory.getLogger(HazelcastChannelStore::class.java)
    private val map: IMap<String, Channel> by lazy { hazelcast.getMap("channels") }

    override suspend fun createChannel(): String {
        val channelId = UUID.randomUUID().toString()
        val channel = Channel(
            id = channelId,
            clients = mutableSetOf(),
            created = System.currentTimeMillis()
        )
        map[channelId] = channel
        return channelId
    }

    override suspend fun addChannelClient(channelId: String, clientId: String): Boolean {
        val channel = map[channelId]
        if (channel == null) {
            log.warn("No channel found with ID: $channelId")
            return false
        }

        val updated = channel.copy(clients = channel.clients + clientId)
        map[channelId] = updated
        return clientId !in channel.clients
    }

    override suspend fun removeChannelClient(channelId: String, clientId: String): Boolean {
        val channel = map[channelId]
        if (channel == null) {
            log.warn("No channel found with ID: $channelId when removing client")
            return false
        }

        if (clientId !in channel.clients) {
            return false
        }

        val updated = channel.copy(clients = channel.clients - clientId)
        map[channelId] = updated
        return true
    }

    override suspend fun removeClientFromAllChannels(clientId: String): Int {
        var count = 0
        map.entries.forEach { entry ->
            if (clientId in entry.value.clients) {
                val updated = entry.value.copy(clients = entry.value.clients - clientId)
                map[entry.key] = updated
                count++
            }
        }
        return count
    }

    override suspend fun getChannel(channelId: String): JsonObject? {
        val channel = map[channelId] ?: return null
        return JsonObject()
            .put("_id", channel.id)
            .put("clients", JsonArray(channel.clients.toList()))
            .put("created", channel.created)
    }

    override suspend fun getClientIds(channelId: String): List<String> {
        val channel = map[channelId]
        if (channel == null) {
            log.warn("No channel found with ID: $channelId when getting client IDs")
            return emptyList()
        }
        return channel.clients.toList()
    }

    override suspend fun getChannelsForClient(clientId: String): List<String> {
        return map.entries
            .filter { clientId in it.value.clients }
            .map { it.key }
    }
}