package com.minare.core.entity.services

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.models.Entity
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.utils.debug.DebugLogger
import com.minare.core.utils.debug.DebugLogger.Companion.DebugType
import com.minare.exceptions.EntitySerializationException
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch

@Singleton
class EntityObjectHydrator @Inject constructor(
    private val coroutineScope: CoroutineScope,
    private val entityFactory: EntityFactory,
    private val stateStore: StateStore
) {
    private val debug = DebugLogger()

    /**
     * Sets all the properties of Entity subtype instances from hot state store
     */
    suspend fun hydrate(entityJson: JsonObject): Entity {
        val entityType = entityJson.getString("type") ?: throw EntitySerializationException("Entity must have type")

        val entityClass = entityFactory.useClass(entityType)
            ?: throw EntitySerializationException("EntityFactory did not return requested type $entityType")

        val entity = entityFactory.createEntity(entityClass).apply {
            _id = entityJson.getString("_id")
            version = entityJson.getLong("version")
            type = entityType
        }

        try {
            val stateJson = entityJson.getJsonObject("state", JsonObject())
            val propertiesJson = entityJson.getJsonObject("properties", JsonObject())

            coroutineScope.launch() {
                stateStore.setEntityState(entity, entityType, stateJson)
                stateStore.setEntityProperties(entity, entityType, propertiesJson)
            }
        } catch (e: Exception) {
            debug.log(DebugType.ENTITY_HYDRATOR_WRITE_FAILED, listOf(e))
        }

        return entity
    }
}