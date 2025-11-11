package com.minare.core.utils

import com.google.inject.Inject
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.hazelcast.query.Predicates
import com.minare.core.entity.models.serializable.Vector2
import java.io.Serializable

/**
 * A grid-map implemented with Hazelcast's IMap.
 * Eventually consistent.
 */
class DistributedGridMap<T : Serializable> @Inject constructor(
    private val hz: HazelcastInstance,
    private val mapName: String
) {
    private var map: IMap<Vector2, T> = hz.getMap(mapName)

    /**
     * Atomically retrieves a single data point using the composite key.
     */
    fun get(xAxis: Int, yAxis: Int): T? {
        val key = Vector2(xAxis, yAxis)
        return map[key]
    }

    /**
     * Atomically updates a single data point.
     */
    fun put(xAxis: Int, yAxis: Int, data: T) {
        val key = Vector2(xAxis, yAxis)
        map.put(key, data)
    }

    class Factory(val baseMapName: String = "GridMap") {
        /**
         * Creates a new instance of DistributedGridMap specialized for type T.
         * The map name is derived from the generic type's simple name to ensure uniqueness
         * for different data types.
         */
        inline fun <reified T : Serializable> create(hazelcastInstance: HazelcastInstance): DistributedGridMap<T> {
            val typeName = T::class.java.simpleName
            val fullMapName = "${baseMapName}_$typeName"

            return DistributedGridMap(hazelcastInstance, fullMapName)
        }
    }
}