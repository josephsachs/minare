# Operations

Operations are the mechanism through which state changes are requested. They express intent: create this entity, change this field, delete that entity. The framework queues them, assigns them to frames, routes them to workers, and executes them in order.

## Operation

An `Operation` is a single mutation request, built with a fluent API.

### Creating an entity

```kotlin
val op = Operation()
    .entityType(Node::class)
    .action(OperationType.CREATE)
    .delta(JsonObject().put("color", "#FF0000"))
```

`entityType` accepts a `KClass`, `Class`, or `String`. For `CREATE`, no entity ID is required — the framework assigns one. The `delta` sets the initial field values on the new entity.

### Mutating an entity

```kotlin
val op = Operation()
    .entity(entityId)
    .entityType(Node::class)
    .action(OperationType.MUTATE)
    .delta(JsonObject().put("color", "#00FF00"))
    .version(currentVersion)
```

Only fields annotated `@Mutable` on the target entity will be accepted. The `version` parameter is optional and used for optimistic locking when the entity has a `@VersionPolicy` set.

### Deleting an entity

```kotlin
val op = Operation()
    .entity(entityId)
    .entityType(Node::class)
    .action(OperationType.DELETE)
```

### Queuing an operation

Pass an `Operation` to `OperationController.queue()`:

```kotlin
operationController.queue(operation)
```

In practice most applications do this inside `OperationController.preQueue()`, where the returned value is picked up and queued automatically by the framework. Calling `queue()` directly is available for server-side operations that originate outside the message handler.

---

## OperationSet

An `OperationSet` groups related operations and other step types into a unit that is routed and processed together within a single frame. All members share a `operationSetId`, a normalized timestamp, and a declaration-order index that controls execution order.

```kotlin
val set = OperationSet()
    .failurePolicy(FailurePolicy.ABORT)

set.add(
    Operation()
        .entity(nodeAId)
        .entityType(Node::class)
        .action(OperationType.MUTATE)
        .delta(JsonObject().put("color", "#FF0000"))
)
set.add(
    Operation()
        .entity(nodeBId)
        .entityType(Node::class)
        .action(OperationType.MUTATE)
        .delta(JsonObject().put("color", "#0000FF"))
)

operationController.queue(set)
```

Steps execute in the order they were added. The set is sealed and serialized when `queue()` is called; do not call `build()` directly.

### FailurePolicy

Controls what happens when any step in the set fails:

| Policy | Behavior |
|---|---|
| `CONTINUE` | Proceed regardless of failure. All completed deltas stand. |
| `ABORT` (default) | Halt remaining steps on failure. Completed deltas stand. |
| `ROLLBACK` | Halt remaining steps on failure. Reverse completed mutations through the normal operation handler path. |

```kotlin
val set = OperationSet().failurePolicy(FailurePolicy.ROLLBACK)
```

---

## OperationSet step types

Beyond `Operation`, three additional step types can be added to an `OperationSet`.

### FunctionCall

Invokes a `@FunctionCall`-annotated method on a hydrated instance of the target entity. The call suspends to completion and its return value is passed as context to the next step.

```kotlin
set.add(
    FunctionCall()
        .entity(nodeId)
        .entityType(Node::class)
        .function("computeNewColor")
        .args(JsonObject().put("color", "#FF0000"))
)
```

The matching entity method:

```kotlin
@FunctionCall
suspend fun computeNewColor(args: JsonObject): String {
    return args.getString("color", color)
}
```

### Assert

Invokes a `@Assert`-annotated boolean predicate on the target entity. A `false` return triggers the set's `FailurePolicy`.

```kotlin
set.add(
    Assert()
        .entity(nodeId)
        .entityType(Node::class)
        .function("isColorValid")
)
```

The matching entity method:

```kotlin
@Assert
fun isColorValid(): Boolean = color.matches(Regex("^#[0-9A-Fa-f]{6}$"))
```

`Assert` is useful as a guard before subsequent mutations — verify a precondition, then let the set apply changes only if it holds.

### Trigger

Invokes a `@Trigger`-annotated method on the target entity without suspending. The framework fires it and immediately advances to the next step. Use for side effects that should not block set execution.

```kotlin
set.add(
    Trigger()
        .entity(nodeId)
        .entityType(Node::class)
        .function("notifyNeighbors")
)
```

The matching entity method:

```kotlin
@Trigger
suspend fun notifyNeighbors() {
    // launched without awaiting
}
```

---

## How operations flow

1. A client sends a message over the UpSocket, or server-side code calls `queue()` directly.
2. `MessageController.handle()` classifies the message and passes it to `OperationController.process()`.
3. `OperationController.preQueue()` (your implementation) converts the raw message into an `Operation` or `OperationSet`, or returns `null` to skip processing.
4. The operation is serialized and sent to Kafka.
5. The `FrameCoordinatorVerticle` consumes operations from Kafka, buckets them into logical frames, and distributes work manifests to workers via Hazelcast.
6. A `FrameWorkerVerticle` processes its manifest for the current frame, routing each operation to the handler.
7. After execution, `OperationController.afterCreateOperation()`, `afterMutateOperation()`, or `afterDeleteOperation()` is called.
8. State changes are collected and pushed to subscribed clients over the DownSocket.
