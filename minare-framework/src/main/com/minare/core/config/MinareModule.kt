package com.minare.core.config

import com.google.inject.*
import com.google.inject.multibindings.OptionalBinder
import com.google.inject.name.Names
import com.hazelcast.core.HazelcastInstance
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
import com.minare.core.frames.services.WorkerRegistryMap
import com.minare.core.frames.services.HazelcastWorkerRegistryMap
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
import javax.inject.Named
import com.minare.core.transport.downsocket.pubsub.UpdateBatchCoordinator
import com.minare.time.DockerTimeService
import com.minare.time.TimeService
import com.minare.core.frames.coordinator.handlers.LateOperationHandler
import com.minare.core.frames.services.ActiveWorkerSet
import com.minare.core.frames.services.HazelcastActiveWorkerSet
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.worker.upsocket.CommandMessageHandler
import kotlin.coroutines.CoroutineContext

/**
 * Core framework Guice module that provides default bindings.
 * Applications can override these bindings by using a child injector.
 */
class MinareModule : AbstractModule(), DatabaseNameProvider {
    private val log = LoggerFactory.getLogger(MinareModule::class.java)

    val uri = System.getenv("MONGO_URI") ?:
    throw IllegalStateException("MONGO_URI environment variable is required")

    override fun configure() {
        // Internal services, do not permit override
        bind(StateStore::class.java).to(RedisEntityStore::class.java).`in`(Singleton::class.java)
        bind(EntityGraphStore::class.java).to(MongoEntityStore::class.java).`in`(Singleton::class.java)
        bind(EntityPublishService::class.java).to(RedisEntityPublishService::class.java).`in`(Singleton::class.java)
        bind(EntityVersioningService::class.java).`in`(Singleton::class.java)
        bind(MessageQueue::class.java).to(KafkaMessageQueue::class.java).`in`(Singleton::class.java)

        bind(ConnectionStore::class.java).to(MongoConnectionStore::class.java).`in`(Singleton::class.java)
        bind(ChannelStore::class.java).to(MongoChannelStore::class.java).`in`(Singleton::class.java)
        bind(ContextStore::class.java).to(MongoContextStore::class.java).`in`(Singleton::class.java)
        bind(DeltaStore::class.java).to(RedisDeltaStore::class.java).`in`(Singleton::class.java)
        bind(SnapshotStore::class.java).to(MongoSnapshotStore::class.java).`in`(Singleton::class.java)

        bind(TimeService::class.java).to(DockerTimeService::class.java).`in`(Singleton::class.java)
        bind(MutationService::class.java).`in`(Singleton::class.java)
        bind(ConnectionCache::class.java).to(InMemoryConnectionCache::class.java).`in`(Singleton::class.java)
        bind(ReflectionCache::class.java).`in`(Singleton::class.java)

        bind(UpdateBatchCoordinator::class.java).`in`(Singleton::class.java)
        bind(CommandMessageHandler::class.java).`in`(Singleton::class.java)
        bind(DefaultEntityFactory::class.java).`in`(Singleton::class.java)

        // Overridable services
        bind(PubSubChannelStrategy::class.java).to(PerChannelPubSubStrategy::class.java).`in`(Singleton::class.java)
        bind(LateOperationHandler::class.java).to(DelayLateOperation::class.java)

        // Providers
        bind(AppState::class.java).toProvider(AppStateProvider::class.java).`in`(Singleton::class.java)

        // String variables
        bind(String::class.java)
            .annotatedWith(Names.named("mongoConnectionString"))
            .toInstance(uri)

        bind(String::class.java).annotatedWith(Names.named("channels")).toInstance("channels")
        bind(String::class.java).annotatedWith(Names.named("contexts")).toInstance("contexts")
        bind(String::class.java).annotatedWith(Names.named("entity_graph")).toInstance("entity_graph")
        bind(String::class.java).annotatedWith(Names.named("connections")).toInstance("connections")

        bind(Int::class.java)
            .annotatedWith(Names.named("intervalMs"))
            .toInstance(100)

        bind(VerticleLogger::class.java).`in`(Singleton::class.java)

        // Workers
        bind(RedisPubSubWorkerVerticle::class.java)
        bind(MutationVerticle::class.java)
        bind(CleanupVerticle::class.java)
    }

    @Provides
    @Singleton
    fun provideEntityFactory(
        injector: Injector,
        defaultFactory: DefaultEntityFactory
    ): EntityFactory {
        return try {
            injector.getInstance(Key.get(EntityFactory::class.java, Names.named("user")))
        } catch (e: Exception) {
            defaultFactory
        }
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
    fun provideMongoClient(vertx: Vertx, @Named("databaseName") dbName: String): MongoClient {
        log.info("Connecting to MongoDB at: $uri with database: $dbName")

        val config = JsonObject()
            .put("connection_string", uri)
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
        val redisUri = System.getenv("REDIS_URI")
            ?: throw IllegalStateException("REDIS_URI environment variable is required")

        val redisOptions = RedisOptions()
            .setConnectionString(redisUri)

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

    override fun getDatabaseName(): String = "minare"
}