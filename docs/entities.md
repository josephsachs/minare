# Entities

Every persistent object in a Minare application is an entity. Entities are the primary unit of state: they are created, mutated, and deleted via operations, change-tracked by the framework, and pushed as updates to subscribed clients.

## Defining an entity

Entities extend the `Entity` base class and are annotated with `@EntityType`.

```kotlin
@EntityType("Node")
class Node : Entity() {
    init { type = "Node" }
}
```

The string passed to `@EntityType` is the type name used throughout the system — in operations, in Redis storage, and in the entity factory registry. It must be unique within an application.

The `type` field inherited from `Entity` must be set manually in `init` to match the annotation value.

`Entity` provides three base fields that are always present:

| Field | Type | Description |
|---|---|---|
| `_id` | `String` | Unique identifier, assigned by the framework on creation |
| `version` | `Long` | Monotonically incrementing; updated on each mutation |
| `type` | `String` | The entity type name |

## Field annotations

### `@State`

Fields annotated `@State` are included in managed entity state. The framework tracks changes to these fields across frame processing and includes them in the delta store and client update pushes.

```kotlin
@State
var label: String = ""

@State
var count: Int = 0
```

An optional `fieldName` parameter overrides the serialized key name:

```kotlin
@State(fieldName = "node_color")
var color: String = "#CCCCCC"
```

### `@Property`

`@Property` marks a field as persistent but not change-tracked. Use it for data that should be stored without triggering state updates when it changes.

```kotlin
@Property
var metadata: String = ""
```

### `@Mutable`

`@Mutable` marks a `@State` field as one that can be changed via a `MUTATE` operation from a client. Without this annotation, the framework rejects attempts to mutate the field through an operation.

```kotlin
@State
@Mutable
var color: String = "#CCCCCC"
```

Fields that should only change through server-side logic — task functions, `@FunctionCall` steps — should carry `@State` but not `@Mutable`.

#### Validation policies

`@Mutable` accepts a `validationPolicy` parameter controlling what happens when a mutation fails validation on that field:

| Policy | Behavior |
|---|---|
| `NONE` (default) | No validation applied; all well-formed changes are accepted |
| `FIELD` | Reject only the failing field; apply remaining valid fields in the delta |
| `ENTITY` | Reject the entire entity delta if any field fails |
| `OPERATION` | Reject the entire operation if any field fails |

```kotlin
@State
@Mutable(validationPolicy = Mutable.Companion.ValidationPolicy.ENTITY)
var color: String = "#CCCCCC"
```

## Relationship annotations

Three annotations describe relationships between entities. They serve two purposes: they inform the MongoDB entity graph store (if configured), and they are used by the affinity system to co-locate related entities on the same worker during frame processing.

### `@Parent`

Marks a field as a reference to this entity's parent entity ID.

```kotlin
@State
@Parent
var parentId: String? = null
```

### `@Child`

Marks a field as a reference to the IDs of this entity's child entities.

```kotlin
@State
@Child
var childIds: MutableList<String> = mutableListOf()
```

### `@Peer`

Marks a field as a reference to peer entity IDs (non-hierarchical relationships).

```kotlin
@State
@Peer
var linkedIds: MutableList<String> = mutableListOf()
```

Relationship annotations interact with the `frames.group_operations_by` configuration setting to control which operations the coordinator routes to the same worker. See [Configuration](configuration.md) for details on `FIELD_PARENT`, `FIELD_CHILD`, and `FIELD_PEER` affinity scopes.

## Version policy

`@VersionPolicy` controls how the framework validates the `version` field on incoming operations. Applied at the class level.

| Rule | Behavior |
|---|---|
| `NONE` (default) | Version is ignored; all operations accepted |
| `MUST_MATCH` | The operation's `version` must exactly match the entity's current version |
| `ONLY_NEXT` | The operation's `version` must be exactly `current + 1` |
| `ALLOW_NEWER` | The operation's `version` must be ≥ the entity's current version |

```kotlin
@EntityType("Node")
@VersionPolicy(VersionPolicy.Companion.VersionPolicyType.MUST_MATCH)
class Node : Entity() { ... }
```

## Task annotations

Task annotations mark entity member functions that run automatically during frame processing.

### `@Task`

Runs on a configurable tick interval (set via `tasks.tick_interval` in config). All entities with a `@Task` function will have it called on each tick, regardless of whether they have pending operations.

```kotlin
@Task
suspend fun tick() {
    // called on every tick
}
```

Task functions must be `suspend`.

### `@FixedTask`

Frame-bound — runs once per frame for the entity. Use when you need behavior synchronized to the frame loop rather than a wall-clock interval.

```kotlin
@FixedTask
suspend fun onFrame() {
    // called once per frame
}
```

## OperationSet function annotations

These annotations mark functions that can be invoked as steps within an `OperationSet`. They are called on a hydrated instance of the entity at execution time. See [Operations](operations.md) for how to construct sets that call them.

### `@FunctionCall`

A suspending function whose return value is passed as context to the next step in the set.

```kotlin
@FunctionCall
suspend fun computeNewColor(args: JsonObject): String {
    return args.getString("color", color)
}
```

### `@Trigger`

A fire-and-forget function. The framework launches it and immediately advances to the next step without waiting for completion. Use for side effects that should not block set execution.

```kotlin
@Trigger
suspend fun notifyNeighbors() {
    // launched without awaiting
}
```

### `@Assert`

A boolean predicate. If it returns `false`, the set's `FailurePolicy` is applied to the remaining steps.

```kotlin
@Assert
fun isColorValid(): Boolean = color.matches(Regex("^#[0-9A-Fa-f]{6}$"))
```

## EntityFactory

The framework needs to know how to instantiate each registered entity type. Applications provide this via a class that extends `EntityFactory`:

```kotlin
class MyEntityFactory : EntityFactory() {
    override val entityTypes = mapOf(
        "Node" to Node::class.java,
        "Edge" to Edge::class.java
    )
}
```

The fully-qualified class name of the factory is set in config under `entity.factory`:

```yaml
entity:
  factory: com.example.myapp.MyEntityFactory
```

Entities are instantiated by Guice, so `@Inject` works inside entity classes.
