# Configuration

Minare loads its configuration from a YAML file at `resources/config/${ENVIRONMENT}.yml`. The `ENVIRONMENT` environment variable selects which file to load; if unset, it defaults to `default`, loading `resources/config/default.yml`.

The file is parsed once at startup by `FrameworkConfigBuilder`. Missing required fields cause a `ConfigurationException` that halts the application before any verticles are deployed. Optional fields log an informational message and fall back to documented defaults.

## Environment variables

Three environment variables are required at runtime and are not part of the config file:

| Variable | Required | Description |
|---|---|---|
| `INSTANCE_ROLE` | Yes | `COORDINATOR` or `WORKER`. Determines which verticles this instance deploys. |
| `HOSTNAME` | Yes (workers) | Worker node identity. Used as the worker ID in the distributed registry. |
| `ENVIRONMENT` | No | Config file selector. Defaults to `default`. |

---

## `sockets`

**Required.**

### `sockets.up`

The UpSocket is the client-to-server command channel.

| Key | Type | Required | Description |
|---|---|---|---|
| `host` | string | Yes | Bind address (e.g. `"0.0.0.0"`) |
| `port` | integer | Yes | Port to listen on |
| `base_path` | string | Yes | WebSocket endpoint path (e.g. `"/command"`) |
| `handshake_timeout` | integer (ms) | Yes | How long to wait for a client to complete the connection handshake before closing the socket |
| `heartbeat_interval` | integer (ms) | Yes | How frequently the server sends heartbeat pings to connected clients |
| `ack` | boolean | No | Whether the server sends acknowledgement messages for received operations. Defaults to `true`. |
| `threads` | integer | No | Number of UpSocket verticle instances to deploy. Defaults to `1`. |

### `sockets.down`

The DownSocket is the server-to-client update push channel.

| Key | Type | Required | Description |
|---|---|---|---|
| `host` | string | Yes | Bind address |
| `port` | integer | Yes | Port to listen on |
| `heartbeat_interval` | integer (ms) | Yes | How frequently the server sends heartbeat pings to connected clients |
| `cache_ttl` | integer (ms) | Yes | How long update payloads are cached before being flushed to subscribers |
| `threads` | integer | No | Number of DownSocket verticle instances to deploy. Defaults to `1`. |

### `sockets.connection`

Controls the lifecycle of `Connection` records stored in Redis.

| Key | Type | Required | Description |
|---|---|---|---|
| `connection_expiry` | integer (ms) | Yes | How long before an idle connection is considered expired |
| `cleanup_interval` | integer (ms) | Yes | How often the cleanup job runs to remove expired connections |
| `reconnect_timeout` | integer (ms) | Yes | How long a disconnected client has to reconnect before its connection record is deleted |
| `aggressive_cleanup` | boolean | No | When `true`, expired connections are removed immediately rather than waiting for the next cleanup interval. Defaults to `false`. |

---

## `entity`

**Required.**

| Key | Type | Required | Description |
|---|---|---|---|
| `factory` | string | Yes | Fully-qualified class name of the application's `EntityFactory` implementation (e.g. `com.example.myapp.MyEntityFactory`) |

### `entity.update`

Controls how entity state changes are collected and pushed to clients.

| Key | Type | Required | Description |
|---|---|---|---|
| `collect_changes` | boolean | Yes | Whether the framework tracks field-level deltas and includes them in update pushes |
| `interval` | integer (frames) | No | How many frames between update collections. `0` or `1` means every frame. Defaults to `0`. |

### `entity.graphing`

**Optional.** Omitting this section disables the entity graph store.

| Key | Type | Required | Description |
|---|---|---|---|
| `store` | string | Yes (if section present) | `"mongo"` to enable MongoDB graph storage, `"none"` to disable |

When `store: mongo` is set, `@Parent`, `@Child`, and `@Peer` field relationships are written to MongoDB after each entity create or mutation. The `mongo` section must also be configured.

---

## `frames`

**Required.**

| Key | Type | Required | Description |
|---|---|---|---|
| `frame_duration` | integer (ms) | Yes | Target wall-clock duration of each logical frame. Operations received within this window are bucketed into the same frame. |
| `lookahead` | integer (frames) | Yes | How many frames ahead the coordinator pre-allocates in the manifest queue |
| `threads` | integer | No | Number of frame worker pairs (FrameWorkerVerticle + WorkerOperationHandlerVerticle) to deploy per worker instance. Defaults to `1`. |
| `group_operations_by` | array of strings | No | Affinity scopes that determine which operations the coordinator routes to the same worker. Defaults to none (no affinity). |

#### `group_operations_by` values

Each value in the array adds a co-location rule. The effects are cumulative — more scopes mean more operations are pinned to the same worker, increasing ordering guarantees at the cost of parallelism.

| Value | Co-location rule |
|---|---|
| `ENTITY` | Operations targeting the same entity are routed to the same worker |
| `TARGETS` | Entity IDs found in an operation's delta are co-located with the operation's primary entity |
| `OPERATION_SET` | All members of an `OperationSet` are routed to the same worker |
| `FIELD_PARENT` | An entity and entities referenced by its `@Parent` fields are co-located |
| `FIELD_CHILD` | An entity and entities referenced by its `@Child` fields are co-located |
| `FIELD_PEER` | An entity and entities referenced by its `@Peer` fields are co-located |

Example: `["ENTITY", "FIELD_PARENT", "FIELD_CHILD"]` ensures that an entity and its immediate parent and children always process on the same worker within a frame.

### `frames.session`

Controls automatic session boundaries in the frame timeline.

| Key | Type | Required | Description |
|---|---|---|---|
| `auto_session` | string | Yes | `"frames_per_session"` to automatically close sessions after a fixed frame count, `"never"` to disable automatic session boundaries |
| `frames_per_session` | integer | Yes (if `auto_session: frames_per_session`) | Number of frames per session |

### `frames.timeline`

**Optional.** Controls behavior when the coordinator detaches from the active timeline (e.g. during replay or recovery). If omitted, safe defaults are used.

#### `frames.timeline.detach`

| Key | Type | Required | Description |
|---|---|---|---|
| `flush_on_detach` | boolean | Yes (if section present) | Whether to flush pending operations before detaching. Default: `true`. |
| `buffer_when_detached` | boolean | Yes (if section present) | Whether to buffer incoming operations while detached rather than dropping them. Default: `true`. |
| `assign_on_resume` | boolean | No | Whether to assign buffered operations to new frames on resume. Default: `false`. |
| `replay_on_resume` | boolean | No | Whether to replay buffered operations from the delta store on resume. Default: `false`. |

#### `frames.timeline.replay`

| Key | Type | Required | Description |
|---|---|---|---|
| `buffer_while_replay` | boolean | Yes (if section present) | Whether to buffer new incoming operations while a replay is in progress. Default: `true`. |

### `frames.snapshot`

**Optional.** Omitting this section disables snapshots.

| Key | Type | Required | Description |
|---|---|---|---|
| `store` | string | Yes (if section present) | `"mongo"` to store snapshots in MongoDB, `"json"` to store as files on the filesystem |

When `store: mongo`, the `mongo` section must be configured. When `store: json`, the `filesystem.storage_path` is used as the output directory.

---

## `tasks`

**Required.**

| Key | Type | Required | Description |
|---|---|---|---|
| `tick_interval` | integer (ms) | Yes | How frequently `@Task`-annotated entity functions are called |

---

## `filesystem`

**Required.**

| Key | Type | Required | Description |
|---|---|---|---|
| `storage_path` | string | Yes | Base path for filesystem writes (used by the JSON snapshot store) |

---

## `redis`

**Required.** Redis is the primary entity state store.

| Key | Type | Required | Description |
|---|---|---|---|
| `host` | string | Yes | Redis hostname |
| `port` | integer | Yes | Redis port |
| `pool` | integer | No | Connection pool size. Defaults to `6`. |
| `max_waiting` | integer | No | Maximum number of operations queued waiting for a pool connection. Defaults to `24`. |

---

## `kafka`

**Required.** Kafka is the durable operation message queue between clients and the frame coordinator.

| Key | Type | Required | Description |
|---|---|---|---|
| `host` | string | Yes | Kafka broker hostname |
| `port` | integer | Yes | Kafka broker port |
| `group_id` | string | No | Kafka consumer group ID. Defaults to `"minare-coordinator"`. |

---

## `mongo`

**Optional.** Required only if any feature is configured to use the MongoDB adapter (`entity.graphing.store: mongo` or `frames.snapshot.store: mongo`). If those features are enabled but this section is absent, the application will fail to start.

| Key | Type | Required | Description |
|---|---|---|---|
| `host` | string | Yes (if section present) | MongoDB hostname |
| `port` | integer | Yes (if section present) | MongoDB port |
| `database` | string | Yes (if section present) | Database name |

---

## `hazelcast`

**Optional.** Hazelcast manages distributed data structures and cluster membership. If omitted, defaults are used.

| Key | Type | Required | Description |
|---|---|---|---|
| `cluster_name` | string | No | Cluster name used to scope this application's Hazelcast members. Defaults to `"minare-application"`. All instances that should form a cluster must share the same value. |

---

## `development`

**Optional.** Development-only settings. Omit entirely in production.

| Key | Type | Required | Description |
|---|---|---|---|
| `reset_data` | boolean | No | **Deletes all data in all configured data stores on startup.** Never set to `true` in production. Defaults to `false`. |

---

## Full example

```yaml
sockets:
  up:
    host: "0.0.0.0"
    port: 4225
    base_path: "/command"
    handshake_timeout: 3000
    heartbeat_interval: 15000
    ack: true
    threads: 3
  down:
    host: "0.0.0.0"
    port: 4226
    cache_ttl: 1000
    heartbeat_interval: 15000
    threads: 3
  connection:
    connection_expiry: 180000
    cleanup_interval: 60000
    reconnect_timeout: 60000
    aggressive_cleanup: true

entity:
  factory: com.example.myapp.MyEntityFactory
  update:
    collect_changes: true
    interval: 1
  graphing:
    store: mongo

frames:
  frame_duration: 125
  lookahead: 40
  threads: 10
  group_operations_by: ["ENTITY", "FIELD_PARENT", "FIELD_CHILD"]
  session:
    auto_session: frames_per_session
    frames_per_session: 600
  timeline:
    detach:
      flush_on_detach: true
      buffer_when_detached: true
      assign_on_resume: false
      replay_on_resume: false
    replay:
      buffer_while_replay: true
  snapshot:
    store: mongo

tasks:
  tick_interval: 5000

filesystem:
  storage_path: /var/log

redis:
  host: redis
  port: 6379
  pool: 5
  max_waiting: 500

kafka:
  host: kafka
  port: 29092
  group_id: minare-coordinator

mongo:
  host: mongodb-rs
  port: 27017
  database: my_app

hazelcast:
  cluster_name: my-application

development:
  reset_data: false
```
