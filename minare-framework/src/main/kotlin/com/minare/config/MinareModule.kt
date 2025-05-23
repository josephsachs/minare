package com.minare.config

import com.google.inject.*
import com.google.inject.name.Names
import com.minare.cache.ConnectionCache
import com.minare.cache.InMemoryConnectionCache
import com.minare.core.entity.ReflectionCache
import com.minare.worker.ChangeStreamWorkerVerticle
import com.minare.worker.CleanupVerticle
import com.minare.worker.MutationVerticle
import com.minare.worker.MinareVerticleFactory
import com.minare.persistence.*
import com.minare.utils.VerticleLogger
import io.vertx.core.Vertx
import io.vertx.core.impl.logging.LoggerFactory
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClient
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import javax.inject.Named
import kotlin.coroutines.CoroutineContext

/**
 * Core framework Guice module that provides default bindings.
 * Applications can override these bindings by using a child injector.
 */
class MinareModule : AbstractModule(), DatabaseNameProvider {
    private val log = LoggerFactory.getLogger(MinareModule::class.java)

    val uri = System.getenv("MONGO_URI") ?: "mongodb://localhost:27017"

    override fun configure() {
        bind(EntityStore::class.java).to(MongoEntityStore::class.java).`in`(Singleton::class.java)
        bind(ConnectionStore::class.java).to(MongoConnectionStore::class.java).`in`(Singleton::class.java)
        bind(ChannelStore::class.java).to(MongoChannelStore::class.java).`in`(Singleton::class.java)
        bind(ContextStore::class.java).to(MongoContextStore::class.java).`in`(Singleton::class.java)

        bind(ConnectionCache::class.java).to(InMemoryConnectionCache::class.java).`in`(Singleton::class.java)
        bind(ReflectionCache::class.java).`in`(Singleton::class.java)

        bind(String::class.java)
            .annotatedWith(Names.named("mongoConnectionString"))
            .toInstance(uri)


        bind(String::class.java).annotatedWith(Names.named("channels")).toInstance("channels")
        bind(String::class.java).annotatedWith(Names.named("contexts")).toInstance("contexts")
        bind(String::class.java).annotatedWith(Names.named("entities")).toInstance("entities")
        bind(String::class.java).annotatedWith(Names.named("connections")).toInstance("connections")

        bind(Boolean::class.java)
            .annotatedWith(Names.named("clusteringEnabled"))
            .toInstance(false)


        bind(Int::class.java)
            .annotatedWith(Names.named("frameIntervalMs"))
            .toInstance(100)

        bind(VerticleLogger::class.java).`in`(Singleton::class.java)


        bind(ChangeStreamWorkerVerticle::class.java)
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

    override fun getDatabaseName(): String = "minare"
}