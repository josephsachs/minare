# Utilities

Reusable types and helpers provided by the framework under `com.minare.core.utils`.

---

## PushVar

`com.minare.core.utils.types.PushVar`

A variable holder optimised for values that are written rarely but read very frequently within a single node. On write, the new value is published to a Vert.x event bus address so all local consumers receive it immediately; subsequent reads return the locally cached value without any async call.

Construct via the injected `PushVar.Factory`:

```kotlin
@Inject private lateinit var pushVarFactory: PushVar.Factory

val pauseState: PushVar<PauseState> = pushVarFactory.create(
    address = "myapp.pause-state",
    initialValue = PauseState.RUNNING,
    serializer = { it.name },
    deserializer = { PauseState.valueOf(it as String) }
)
```

- `address` — the event bus address used to broadcast updates. Must be unique within the application.
- `initialValue` — the value before any write has occurred.
- `serializer` / `deserializer` — convert between the typed value and the event bus message body. Defaults to identity cast; provide these for any type that isn't directly passable on the event bus.

```kotlin
pauseState.set(PauseState.PAUSED)   // broadcasts to all local consumers
val current = pauseState.get()       // returns local cached value, no suspension
```

`PushVar` is local only — updates do not cross node boundaries. Use `DistributedEnum` or `AppState` for cluster-wide state.

---

## DistributedEnum

`com.minare.core.utils.types.DistributedEnum`

An atomic enum value backed by the Hazelcast CP subsystem. Provides cluster-wide consistency for state that needs to be visible and writable from any node.

Construct via the injected `DistributedEnum.Factory`:

```kotlin
@Inject private lateinit var distributedEnumFactory: DistributedEnum.Factory

val clusterMode: DistributedEnum<ClusterMode> = distributedEnumFactory.create(
    name = "myapp.cluster-mode",
    enumClass = ClusterMode::class,
    initialValue = ClusterMode.NORMAL
)
```

- `name` — the Hazelcast CP atomic reference name. All instances using the same name share the same value.
- `initialValue` — set via `compareAndSet(null, initialValue)` on first construction; subsequent constructions on other nodes see the already-set value.

```kotlin
clusterMode.set(ClusterMode.MAINTENANCE)

val current: ClusterMode = clusterMode.get()

// Atomic conditional update
val swapped: Boolean = clusterMode.compareAndSet(
    expected = ClusterMode.NORMAL,
    update = ClusterMode.MAINTENANCE
)

// Read previous value while updating
val previous: ClusterMode? = clusterMode.getAndSet(ClusterMode.NORMAL)
```

`DistributedEnum` uses the Hazelcast CP subsystem, which requires at least three CP members in the cluster for consensus. In single-node development setups, the CP subsystem may not be available; prefer `AppState` for simpler coordination needs.

---

## EventStateFlow

`com.minare.core.utils.types.esf.EventStateFlow`

A lightweight state machine whose steps are coordinated over the Vert.x event bus. Each registered state is a suspend function that runs in the creating verticle's coroutine scope. A step calls `next()` on its `StateFlowContext` when it is done, triggering the transition to the following step.

```kotlin
val flow = EventStateFlow(
    eventKey = "myapp.startup-sequence",
    coroutineScope = this,   // the enclosing CoroutineVerticle's scope
    vertx = vertx,
    looping = false
)

flow.registerState("init") { ctx ->
    // first step
    initializeResources()
    ctx.next()
}

flow.registerState("seed") { ctx ->
    // second step, runs after "init" calls next()
    seedInitialData()
    ctx.next()
}

flow.registerState("open") { ctx ->
    // third step
    openForTraffic()
    // no next() needed for the last step in a non-looping flow
}

flow.start()  // kicks off the first step immediately
```

Steps execute in declaration order. If `looping = true`, the sequence restarts from the beginning after the last step calls `next()`.

### StateFlowContext

The argument passed to each step function. Provides two transition methods:

| Method | Behaviour |
|---|---|
| `next()` | Unconditionally trigger the next state transition |
| `tryNext()` | Trigger the next transition only if no state is currently executing; silently skips if busy |

Use `tryNext()` when the state machine is triggered by an external recurring source (a timer, a heartbeat) where it is normal for a trigger to arrive while the previous step is still running.

### Cleanup

```kotlin
flow.cleanup()  // unregisters all event bus consumers and resets the tracker
```

Call `cleanup()` if the flow needs to be torn down before it completes naturally.

---

## LoopingList

`com.minare.core.utils.types.LoopingList`

A list with a stateful iterator that optionally wraps around to the beginning when it reaches the end.

```kotlin
val workers = LoopingList(listOf("worker-1", "worker-2", "worker-3"), looping = true)

while (workers.hasNext()) {
    val worker = workers.next()   // cycles: 1, 2, 3, 1, 2, 3, ...
    assignWork(worker)
}

workers.reset()   // returns the iterator to the starting position
```

In non-looping mode, `hasNext()` returns `false` after the last element and `next()` throws `NoSuchElementException`.

---

## DebugLogger

`com.minare.core.utils.debug.DebugLogger`

The framework's internal structured debug log. Each framework subsystem has named `DebugType` entries that can be individually enabled or disabled. Most are off by default to avoid noise in production.

To enable detailed logging for a subsystem during development, locate the relevant `DebugType` entries in `DebugLogger` and set their values to `true` in the `isEnabled` map:

```kotlin
// In DebugLogger.kt
private val isEnabled: Map<DebugType, Boolean> = mapOf(
    ...
    DebugType.COORDINATOR_MANIFEST_BUILDER_WROTE_WORKER to true,  // was false
    DebugType.COORDINATOR_MANIFEST_BUILDER_WROTE_ALL to true,     // was false
    ...
)
```

`DebugLogger` is injected by the framework into its own controllers and verticles; application code does not call it directly. It is documented here because enabling specific entries is the primary way to get detailed visibility into frame coordination, affinity resolution, and operation routing during development.
