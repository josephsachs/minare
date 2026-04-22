# Work Dispatch

The work dispatch system lets the coordinator distribute arbitrary work across all registered workers within a single coordinated operation. It handles serialization of work items into a Hazelcast manifest, event bus signalling, and barrier synchronisation — `dispatch()` suspends until every worker has reported completion.

`AppState` is the natural companion to the dispatch system: it provides a cluster-wide key-value store that coordinator and workers use to share identifiers and configuration that both sides need.

---

## AppState

`AppState` is a cluster-wide key-value store backed by Vert.x distributed shared data. It is available on all instances after `onApplicationBootStrap()` completes, and is the standard way to share values — channel IDs, feature flags, session identifiers — between the coordinator and workers.

Inject it directly:

```kotlin
@Inject private lateinit var appState: AppState
```

### Methods

| Method | Description |
|---|---|
| `get(key)` | Return the stored string, or `null` if absent |
| `set(key, value)` | Store a string |
| `getJson(key)` | Return the value parsed as `JsonObject`, or `null` |
| `setJson(key, value)` | Store a `JsonObject` (serialised as a string internally) |
| `remove(key)` | Delete the key, returning its previous value |
| `exists(key)` | Return `true` if the key is present |
| `keys()` | Return the full set of stored keys |

All methods are `suspend`.

### Typical pattern

The coordinator creates shared resources during `onCoordinatorStart()` and stores their IDs in `AppState`. Workers read those IDs during `onWorkerStart()`.

```kotlin
// In NodeGraphApplication (coordinator role)
override suspend fun onCoordinatorStart() {
    val channelId = channelController.createChannel()
    appState.set("defaultChannel", channelId)
}

// In NodeGraphApplication (worker role)
override suspend fun onWorkerStart() {
    val channelId = appState.get("defaultChannel")
        ?: throw IllegalStateException("Default channel not found in AppState")
    // store channelId locally for use in afterCreateOperation, etc.
}
```

---

## WorkUnit

`WorkUnit` is the interface applications implement to define a unit of distributed work. The split between `prepare()` and `process()` mirrors the coordinator/worker split: preparation runs once on the coordinator, processing runs in parallel across all workers on their assigned subsets.

```kotlin
interface WorkUnit {
    suspend fun prepare(): Collection<Any>
    suspend fun process(items: Collection<Any>): Any?
}
```

- `prepare()` — called on the coordinator. Returns the full collection of work items to be distributed.
- `process(items)` — called on each worker with its assigned subset of items. The return value is currently unused.

`WorkUnit` implementations are resolved via Guice, so injection works as normal:

```kotlin
@Singleton
class SeedNodesWorkUnit @Inject constructor(
    private val entityController: EntityController,
    private val channelController: ChannelController,
    private val appState: AppState
) : WorkUnit {

    override suspend fun prepare(): Collection<Any> {
        // Return the work items to distribute — here, a list of node configs
        return listOf(
            JsonObject().put("color", "#FF0000"),
            JsonObject().put("color", "#00FF00"),
            JsonObject().put("color", "#0000FF")
        )
    }

    override suspend fun process(items: Collection<Any>): Any? {
        val channelId = appState.get("defaultChannel") ?: return null

        for (item in items) {
            val config = item as JsonObject
            val node = Node().apply { color = config.getString("color") }
            val saved = entityController.create(node)
            channelController.addEntity(saved, channelId)
        }

        return null
    }
}
```

---

## WorkDispatchService

`WorkDispatchService` is injected into your application and called from coordinator context — typically inside `onCoordinatorStart()` or `afterCoordinatorStart()`.

```kotlin
@Inject private lateinit var workDispatchService: WorkDispatchService
```

### `dispatch()`

```kotlin
suspend fun dispatch(
    event: String,
    strategy: WorkDispatchStrategy,
    workUnit: WorkUnit
)
```

- `event` — a unique string label for this dispatch round. Used as the Hazelcast manifest key and event bus address. Must not collide with concurrent dispatches.
- `strategy` — controls how items from `prepare()` are divided among workers.
- `workUnit` — the `WorkUnit` instance to execute. Its class name is broadcast to workers, which resolve their own instance via Guice.

`dispatch()` suspends until all active workers have signalled completion, then cleans up the manifest and event consumers before returning.

```kotlin
override suspend fun onCoordinatorStart() {
    workDispatchService.dispatch(
        event = "seed-initial-nodes",
        strategy = WorkDispatchService.Companion.WorkDispatchStrategy.RANGE,
        workUnit = injector.getInstance(SeedNodesWorkUnit::class.java)
    )
}
```

### WorkDispatchStrategy

| Strategy | Behaviour | Status |
|---|---|---|
| `RANGE` | Divides items into equal-sized chunks, one chunk per worker | Implemented |
| `UNIFORM` | Sends the full item collection to every worker | Implemented |
| `CONSISTENT_HASH` | Routes items to workers by hash ring | Not yet implemented |
| `SCOPE` | Routes items by affinity scope | Not yet implemented |

`RANGE` is the standard choice for seeding or bulk-initialisation work where each item should be processed exactly once. `UNIFORM` is appropriate when all workers need to act on the same shared information — for example, loading a lookup table into local memory.
