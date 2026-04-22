# Controllers

Controllers are the primary extension points through which applications integrate with the Minare framework. Each has a specific responsibility in the request-processing pipeline. All five are bound in the application's Guice module and injected throughout the framework.

---

## MessageController

`MessageController` is the first handler for all incoming WebSocket messages on the UpSocket. Its job is to parse and classify each message, then dispatch it as a typed command object.

**Abstract — must be implemented.**

```kotlin
@Singleton
class MyMessageController @Inject constructor() : MessageController() {
    override suspend fun handle(connection: Connection, message: JsonObject) {
        when {
            message.getString("type") == "heartbeat_response" ->
                dispatch(HeartbeatResponse(
                    connection,
                    message.getLong("timestamp"),
                    message.getLong("clientTimestamp")
                ))

            message.getString("command") == "sync" ->
                dispatch(SyncCommand(connection, SyncCommandType.CHANNEL, null))

            else ->
                dispatch(OperationCommand(message))
        }
    }
}
```

`dispatch()` accepts three command types:

| Type | Purpose |
|---|---|
| `HeartbeatResponse` | Client acknowledgement of a server heartbeat. Required to keep connections alive. |
| `SyncCommand` | Client requesting a full state resync for their subscribed channels. |
| `OperationCommand` | Any mutation request — forwarded to `OperationController`. |

The framework handles `SyncCommand` and `HeartbeatResponse` internally after dispatch. `OperationCommand` payloads are passed to `OperationController.process()`.

---

## OperationController

`OperationController` bridges raw client messages to typed operations. It owns the translation and queuing lifecycle.

**Abstract — must be implemented.** The application must extend it and implement `preQueue()`.

```kotlin
@Singleton
class MyOperationController @Inject constructor() : OperationController() {

    override suspend fun preQueue(message: JsonObject): Any? {
        return when (message.getString("command")) {
            "create" -> Operation()
                .entityType(MyEntity::class)
                .action(OperationType.CREATE)
                .delta(message.getJsonObject("state") ?: JsonObject())

            "mutate" -> Operation()
                .entity(message.getString("entityId"))
                .entityType(MyEntity::class)
                .action(OperationType.MUTATE)
                .delta(message.getJsonObject("delta") ?: JsonObject())
                .version(message.getLong("version") ?: 0L)

            "delete" -> Operation()
                .entity(message.getString("entityId"))
                .entityType(MyEntity::class)
                .action(OperationType.DELETE)

            else -> null  // returning null skips queuing silently
        }
    }
}
```

`preQueue` returns an `Operation`, an `OperationSet`, or `null`. Returning `null` silently skips queuing — use it to ignore unknown or invalid messages.

### Post-execution hooks

These are called by the worker after operations complete. Override them to react to confirmed state changes:

```kotlin
override suspend fun afterCreateOperation(operation: JsonObject, entity: Entity) {
    // entity is fully hydrated; typical use is adding it to a channel
    channelController.addEntity(entity, defaultChannelId)
}

override suspend fun afterDeleteOperation(operation: JsonObject, entity: Entity) {
    channelController.removeEntity(entity, defaultChannelId)
}

override suspend fun afterMutateOperation(operation: JsonObject, before: JsonObject, after: JsonObject) {
    // before and after are serialized entity state snapshots
}
```

`postQueue(operations: JsonArray)` is also available if you need to act immediately after Kafka send, before worker execution:

```kotlin
override suspend fun postQueue(operations: JsonArray) {
    // called after Kafka send, before the worker processes the operation
}
```

---

## ConnectionController

`ConnectionController` manages client connection lifecycle. The base implementation handles the mechanics; override specific hooks for auth and application-specific connection logic.

**Open — override as needed.** Bind the base class directly in your module if no custom logic is required.

```kotlin
override suspend fun onConnectionAttempt(message: String): Boolean {
    // Receives the raw handshake message string.
    // Return false to reject the connection before a Connection record is created.
    return validateToken(message)
}

override suspend fun onConnected(connection: Connection) {
    // Called after the handshake completes and the Connection is stored.
    // connection.id, connection.upSocketId, connection.downSocketId, connection.meta are all available.
}
```

Utility methods available for use in other controllers:

| Method | Purpose |
|---|---|
| `createConnection(meta)` | Create a connection record with optional metadata |
| `getConnection(connectionId)` | Retrieve a stored connection by ID |
| `hasConnection(connectionId)` | Check whether a connection exists |
| `sendToUpSocket(connectionId, message)` | Send a message to a specific connection's command socket |
| `cleanupConnection(connectionId)` | Remove a connection from all channels and delete it |

---

## ChannelController

`ChannelController` manages pub/sub channels, their entity memberships, and their client subscriptions. Entities added to a channel have their state updates pushed to all clients subscribed to that channel.

**Open — override as needed.** The base implementation is complete; most applications use it directly.

```kotlin
// Create a new channel; returns the channel ID
val channelId = channelController.createChannel()

// Subscribe a client connection to a channel
channelController.addClient(connectionId, channelId)

// Add an entity to a channel
channelController.addEntity(entity, channelId)

// Add multiple entities at once
channelController.addEntitiesToChannel(entities, channelId)

// Remove an entity from a channel
channelController.removeEntity(entity, channelId)

// Push a message directly to all clients in a channel
channelController.broadcast(channelId, JsonObject().put("type", "announcement"))
```

Channels are typically created in `onCoordinatorStart()` and stored in `AppState` so workers can look up their IDs. Entities are added to channels in `OperationController.afterCreateOperation()`, so that newly created entities are immediately visible to subscribers.

---

## EntityController

`EntityController` handles direct server-side entity persistence. It manages the two-phase create flow (Redis primary store + optional MongoDB graph store) and exposes save, delete, and find operations.

**Open — override as needed.** The base implementation is complete. Extend it to intercept persistence operations — for example, to assign custom entity IDs or to transform state before saving.

```kotlin
// Override to use a custom ID format instead of UUID
override suspend fun setId(entity: Entity) {
    entity._id = "node-${UUID.randomUUID()}"
}
```

Available methods:

| Method | Purpose |
|---|---|
| `create(entity)` | Assign an ID, save to Redis, optionally write to MongoDB graph. Returns the saved entity. |
| `saveState(entityId, deltas)` | Save changed `@State` fields and update relationship graph |
| `saveProperties(entityId, deltas)` | Save changed `@Property` fields |
| `delete(entityId)` | Remove the entity from all stores |
| `findByIds(ids)` | Retrieve a map of `entityId → Entity` for a list of IDs |

`EntityController` is for server-side state management. Client-initiated mutations go through the operation queue, not directly through `EntityController`.

---

## Application lifecycle hooks

`MinareApplication` provides hooks that run at specific points during startup. Override them in your application class to perform initialization at the right moment.

```kotlin
class MyApplication : MinareApplication() {

    override suspend fun onApplicationBootStrap() {
        // Runs before the coordinator/worker split, on all instances.
        // Use for initialization that every node needs (e.g., configuring libraries).
    }

    override suspend fun onCoordinatorStart() {
        // Coordinator only. Runs before the frame loop starts.
        // Typical use: create channels, seed initial entities.
        val channelId = channelController.createChannel()
        appState.set("defaultChannel", channelId)
    }

    override suspend fun afterCoordinatorStart() {
        // Coordinator only. Runs after the frame loop has started.
    }

    override suspend fun onWorkerStart() {
        // All workers. Runs before HTTP routes are set up.
        // Typical use: load configuration or shared state that workers need.
    }

    override suspend fun setupApplicationRoutes() {
        // All workers. Add custom HTTP routes here.
    }
}
```

The startup sequence on each role:

**Coordinator:** `onApplicationBootStrap` → wait for all workers → `onCoordinatorStart` → start frame loop → `afterCoordinatorStart`

**Worker:** `onApplicationBootStrap` → deploy sockets and frame workers → `onWorkerStart` → `setupApplicationRoutes` → signal ready

---

## Wiring controllers in Guice

All five controllers must be bound in your application's Guice module. Use a `PrivateModule` to scope bindings to the application:

```kotlin
class MyAppModule : PrivateModule() {
    override fun configure() {
        bind(MessageController::class.java).to(MyMessageController::class.java).`in`(Singleton::class.java)
        bind(OperationController::class.java).to(MyOperationController::class.java).`in`(Singleton::class.java)
        bind(ConnectionController::class.java).to(MyConnectionController::class.java).`in`(Singleton::class.java)
        bind(ChannelController::class.java).to(MyChannelController::class.java).`in`(Singleton::class.java)
        bind(EntityController::class.java).to(MyEntityController::class.java).`in`(Singleton::class.java)

        expose(MessageController::class.java)
        expose(OperationController::class.java)
        expose(ConnectionController::class.java)
        expose(ChannelController::class.java)
        expose(EntityController::class.java)
    }
}
```

If you have no custom logic for a controller, bind it to the base class:

```kotlin
bind(EntityController::class.java).`in`(Singleton::class.java)
```

The module is referenced from your application class via a `getModule()` companion method:

```kotlin
class MyApplication : MinareApplication() {
    companion object {
        @JvmStatic
        fun getModule(): Module = MyAppModule()
    }
}
```
