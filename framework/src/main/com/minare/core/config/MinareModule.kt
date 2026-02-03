package com.minare.core.config

import com.google.inject.*
import com.google.inject.name.Names
import com.hazelcast.core.HazelcastInstance
import com.minare.application.config.FrameworkConfig
import com.minare.application.interfaces.AppState
import com.minare.cache.ConnectionCache
import com.minare.cache.InMemoryConnectionCache
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.factories.DefaultEntityFactory
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.services.EntityPublishService
import com.minare.core.entity.services.EntityVersioningService
import com.minare.core.entity.services.MutationService
import com.minare.core.entity.services.RedisEntityPublishService
import com.minare.core.operation.adapters.KafkaMessageQueue
import com.minare.core.operation.interfaces.MessageQueue
import com.minare.core.transport.downsocket.pubsub.PubSubChannelStrategy
import com.minare.core.transport.downsocket.pubsub.PerChannelPubSubStrategy
import com.minare.core.transport.CleanupVerticle
import com.minare.core.operation.MutationVerticle
import com.minare.core.factories.MinareVerticleFactory
import com.minare.core.frames.coordinator.handlers.DelayLateOperation
import com.minare.core.transport.downsocket.RedisPubSubWorkerVerticle
import com.minare.core.frames.services.SnapshotService.Companion.SnapshotStoreOption
import com.minare.core.storage.adapters.*
import com.minare.core.storage.interfaces.*
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.Vertx
import io.vertx.core.impl.logging.LoggerFactory
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClient
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.redis.client.Redis
import io.vertx.redis.client.RedisAPI
import io.vertx.redis.client.RedisOptions
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import com.minare.core.transport.downsocket.pubsub.UpdateBatchCoordinator
import com.minare.time.DockerTimeService
import com.minare.time.TimeService
import com.minare.core.frames.coordinator.handlers.LateOperationHandler
import com.minare.core.frames.services.*
import com.minare.core.transport.upsocket.handlers.SyncCommandHandler
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.exceptions.EntityFactoryException
import com.minare.worker.upsocket.CommandMessageHandler
import kotlinx.coroutines.CoroutineScope
import kotlin.coroutines.CoroutineContext

/**
 * Core framework Guice module that provides default bindings.
 * Applications can override these bindings by using a child injector.
 */
class MinareModule(
    private val frameworkConfig: FrameworkConfig
) : AbstractModule() {
    private val log = LoggerFactory.getLogger(MinareModule::class.java)

    override fun configure() {
        // Internal services, do not permit override
        bind(StateStore::class.java).to(RedisEntityStore::class.java).`in`(Singleton::class.java)

        if (frameworkConfig.mongo.enabled) {
            bind(EntityGraphStore::class.java).to(MongoEntityStore::class.java).`in`(Singleton::class.java)
        } else {
            log.warn("No entity graph store is available, binding no-op entity graph adapter")
            bind(EntityGraphStore::class.java).to(NoopEntityGraphStore::class.java).`in`(Singleton::class.java)
        }

        bind(EntityPublishService::class.java).to(RedisEntityPublishService::class.java).`in`(Singleton::class.java)
        bind(EntityVersioningService::class.java).`in`(Singleton::class.java)
        bind(MessageQueue::class.java).to(KafkaMessageQueue::class.java).`in`(Singleton::class.java)

        bind(ConnectionStore::class.java).to(HazelcastConnectionStore::class.java).`in`(Singleton::class.java)
        bind(ChannelStore::class.java).to(HazelcastChannelStore::class.java).`in`(Singleton::class.java)
        bind(ContextStore::class.java).to(HazelcastContextStore::class.java).`in`(Singleton::class.java)
        bind(DeltaStore::class.java).to(RedisDeltaStore::class.java).`in`(Singleton::class.java)

        when (frameworkConfig.frames.snapshot.store) {
            SnapshotStoreOption.MONGO -> bind(SnapshotStore::class.java).to(MongoSnapshotStore::class.java).`in`(Singleton::class.java)
            SnapshotStoreOption.JSON -> bind(SnapshotStore::class.java).to(JsonSnapshotStore::class.java).`in`(Singleton::class.java)
            else -> {
                log.warn("No snapshot store configured, binding no-op adapter")
                bind(SnapshotStore::class.java).to(NoopSnapshotStore::class.java).`in`(Singleton::class.java)
            }
        }

        bind(TimeService::class.java).to(DockerTimeService::class.java).`in`(Singleton::class.java)
        bind(MutationService::class.java).`in`(Singleton::class.java)
        bind(ConnectionCache::class.java).to(InMemoryConnectionCache::class.java).`in`(Singleton::class.java)
        bind(ReflectionCache::class.java).`in`(Singleton::class.java)

        bind(UpdateBatchCoordinator::class.java).`in`(Singleton::class.java)
        bind(CommandMessageHandler::class.java).`in`(Singleton::class.java)
        bind(SyncCommandHandler::class.java).`in`(Singleton::class.java)
        bind(DefaultEntityFactory::class.java).`in`(Singleton::class.java)

        // Overridable services
        bind(PubSubChannelStrategy::class.java).to(PerChannelPubSubStrategy::class.java).`in`(Singleton::class.java)
        bind(LateOperationHandler::class.java).to(DelayLateOperation::class.java)

        // Providers
        bind(AppState::class.java).toProvider(AppStateProvider::class.java).`in`(Singleton::class.java)

        bind(VerticleLogger::class.java).`in`(Singleton::class.java)

        // Workers
        bind(RedisPubSubWorkerVerticle::class.java)
        bind(MutationVerticle::class.java)
        bind(CleanupVerticle::class.java)
    }

    @Provides
    @Singleton
    fun provideEntityFactory(injector: Injector): EntityFactory {
        val entityFactoryName = frameworkConfig.entity.factoryName
        val clazz = try {
            Class.forName(entityFactoryName)
                .asSubclass(EntityFactory::class.java)
        } catch (e: ClassNotFoundException) {
            throw EntityFactoryException("No class found at $entityFactoryName")
        } catch (e: ClassCastException) {
            throw EntityFactoryException("The class found at $entityFactoryName is not a valid type of EntityFactory")
        }

        return injector.getInstance(clazz)
    }

    /**
     * Provides the MinareVerticleFactory
     */
    @Provides
    @Singleton
    fun provideMinareVerticleFactory(injector: Injector): MinareVerticleFactory {
        return MinareVerticleFactory(injector)
    }

    @Provides
    @Singleton
    fun provideCoroutineContext(vertx: Vertx): CoroutineContext {
        return vertx.dispatcher()
    }

    @Provides
    @Singleton
    fun provideCoroutineScope(coroutineContext: CoroutineContext): CoroutineScope {
        return CoroutineScope(coroutineContext)
    }

    @Provides
    @Singleton
    fun provideMongoClient(vertx: Vertx): MongoClient {
        val mongoUri =  "mongodb://${frameworkConfig.mongo.host}:${frameworkConfig.mongo.port}/${frameworkConfig.mongo.database}?replicaSet=rs0"
        val dbName = frameworkConfig.mongo.database

        log.info("Connecting to MongoDB at: $mongoUri with database: $dbName")

        val config = JsonObject()
            .put("connection_string", mongoUri)
            .put("db_name", dbName)
            .put("useObjectId", true)
            .put("writeConcern", "majority")
            .put("serverSelectionTimeoutMS", 5000)
            .put("connectTimeoutMS", 10000)
            .put("socketTimeoutMS", 60000)

        return MongoClient.createShared(vertx, config)
    }

    @Provides
    @Singleton
    fun provideHazelcastClusterManager(): HazelcastClusterManager {
        return HazelcastClusterManager()
    }

    @Provides
    @Singleton
    fun provideRedisAPI(vertx: Vertx): RedisAPI {
        val redisOptions = RedisOptions()
            .setConnectionString(
                "redis://${frameworkConfig.redis.host}:${frameworkConfig.redis.port}"
            )

        val redis = Redis.createClient(vertx, redisOptions)
        return RedisAPI.api(redis)
    }

    @Provides
    @Singleton
    fun provideHazelcastInstance(): HazelcastInstance {
        return HazelcastInstanceHolder.getInstance()
    }

    /**
     * Provides the distributed worker registry map
     */
    @Provides
    @Singleton
    fun provideWorkerRegistryMap(hazelcastInstance: HazelcastInstance): WorkerRegistryMap {
        val map = hazelcastInstance.getMap<String, JsonObject>("worker-registry")
        log.info("Initialized distributed worker registry map")
        return HazelcastWorkerRegistryMap(map)
    }

    /**
     * Provides the distributed worker registry map
     */
    @Provides
    @Singleton
    fun provideActiveWorkerSet(hazelcastInstance: HazelcastInstance): ActiveWorkerSet {
        val set = hazelcastInstance.getSet<String>("active-workers")
        log.info("Initialized distributed active worker set")
        return HazelcastActiveWorkerSet(set)
    }

    /**
     * Provides EventBusUtils for FrameCoordinatorVerticle
     */
    @Provides
    @Singleton
    fun provideEventBusUtils(vertx: Vertx, coroutineContext: CoroutineContext): EventBusUtils {
        return EventBusUtils(vertx, coroutineContext, "FrameCoordinatorVerticle")
    }
}