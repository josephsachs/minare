package com.minare.config

import com.google.inject.*
import com.google.inject.name.Names
import com.hazelcast.config.Config
import com.hazelcast.core.Hazelcast
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.minare.application.AppState
import com.minare.cache.ConnectionCache
import com.minare.cache.HazelcastInstanceHolder
import com.minare.cache.InMemoryConnectionCache
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.EntityFactory
import com.minare.entity.*
import com.minare.operation.KafkaMessageQueue
import com.minare.operation.MessageQueue
import com.minare.pubsub.PubSubChannelStrategy
import com.minare.pubsub.PerChannelPubSubStrategy
import com.minare.worker.CleanupVerticle
import com.minare.worker.MutationVerticle
import com.minare.worker.MinareVerticleFactory
import com.minare.worker.RedisPubSubWorkerVerticle
import com.minare.worker.coordinator.WorkerRegistryMap
import com.minare.worker.coordinator.HazelcastWorkerRegistryMap
import com.minare.persistence.*
import com.minare.utils.VerticleLogger
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
import com.minare.persistence.EntityQueryStore
import com.minare.persistence.RedisEntityStore
import com.minare.persistence.StateStore
import com.minare.persistence.WriteBehindStore
import com.minare.pubsub.UpdateBatchCoordinator
import com.minare.time.DockerTimeService
import com.minare.time.TimeService
import com.minare.worker.upsocket.CommandMessageHandler
import com.minare.config.AppStateProvider
import com.minare.worker.coordinator.DelayLateOperations
import com.minare.worker.coordinator.LateOperationHandler
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
        bind(EntityStore::class.java).to(MongoEntityStore::class.java).`in`(Singleton::class.java)
        bind(EntityPublishService::class.java).to(RedisEntityPublishService::class.java).`in`(Singleton::class.java)
        bind(EntityVersioningService::class.java).`in`(Singleton::class.java)
        bind(EntityQueryStore::class.java).to(MongoEntityStore::class.java).`in`(Singleton::class.java)
        bind(WriteBehindStore::class.java).to(MongoEntityStore::class.java).`in`(Singleton::class.java)
        bind(MessageQueue::class.java).to(KafkaMessageQueue::class.java).`in`(Singleton::class.java)

        bind(ConnectionStore::class.java).to(MongoConnectionStore::class.java).`in`(Singleton::class.java)
        bind(ChannelStore::class.java).to(MongoChannelStore::class.java).`in`(Singleton::class.java)
        bind(ContextStore::class.java).to(MongoContextStore::class.java).`in`(Singleton::class.java)

        bind(TimeService::class.java).to(DockerTimeService::class.java).`in`(Singleton::class.java)
        bind(MutationService::class.java).`in`(Singleton::class.java)
        bind(ConnectionCache::class.java).to(InMemoryConnectionCache::class.java).`in`(Singleton::class.java)
        bind(ReflectionCache::class.java).`in`(Singleton::class.java)

        bind(UpdateBatchCoordinator::class.java).`in`(Singleton::class.java)
        bind(CommandMessageHandler::class.java).`in`(Singleton::class.java)

        // Overridable services
        bind(PubSubChannelStrategy::class.java).to(PerChannelPubSubStrategy::class.java).`in`(Singleton::class.java)
        bind(LateOperationHandler::class.java).to(DelayLateOperations::class.java)

        // Providers
        bind(AppState::class.java).toProvider(AppStateProvider::class.java).`in`(Singleton::class.java)

        // String variables
        bind(String::class.java)
            .annotatedWith(Names.named("mongoConnectionString"))
            .toInstance(uri)

        bind(String::class.java).annotatedWith(Names.named("channels")).toInstance("channels")
        bind(String::class.java).annotatedWith(Names.named("contexts")).toInstance("contexts")
        bind(String::class.java).annotatedWith(Names.named("entities")).toInstance("entities")
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

    /**
     * Provides the MinareVerticleFactory
     */
    @Provides
    @Singleton
    fun provideMinareVerticleFactory(injector: Injector): MinareVerticleFactory {
        return MinareVerticleFactory(injector)
    }

    /**
     * Direct EntityFactory binding - no wrapper needed since Entity has no dependencies.
     * Applications provide their EntityFactory implementation with @Named("userEntityFactory").
     */
    @Provides
    @Singleton
    fun provideEntityFactory(
        @Named("userEntityFactory") userEntityFactory: EntityFactory
    ): EntityFactory {
        return userEntityFactory
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

    override fun getDatabaseName(): String = "minare"
}