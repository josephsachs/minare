# Docker Setup

The Docker environment runs the complete Minare stack locally: one coordinator, one or more scalable workers, MongoDB, Redis, Kafka, HAProxy, and a development Kafka UI. Everything is defined in `docker/docker-compose.yaml`.

---

## Quick start

From the project root:

```bash
./run.sh [WORKER_COUNT] -n    # build framework + nodegraph, then start
./run.sh [WORKER_COUNT] -i    # build framework + integration tests, then start
./run.sh -b                   # build framework only, no Docker
./run.sh -x -n                # skip framework build, start nodegraph with existing JARs
```

`WORKER_COUNT` defaults to `1`. Example with three workers:

```bash
./run.sh 3 -n
```

`run.sh` always does a full teardown (`docker compose down -v`), clears data directories and logs, rebuilds images from scratch, and follows the logs after starting. It is destructive by design.

To stop without destroying data:

```bash
./stop.sh    # runs docker compose stop (graceful stop, volumes preserved)
```

To rebuild only the framework library (e.g. after changing framework code):

```bash
./provide.sh    # mvn clean install in framework/
```

Then re-run `run.sh -x -n` to skip the framework build and use the updated installed artifact.

---

## Services

### `app-coordinator`

Runs the application JAR with `INSTANCE_ROLE=COORDINATOR`. Deploys the frame loop and waits for all workers to register before starting it. Exposes port `9090` for the coordinator admin endpoint (used by the `infra` container to register workers).

### `worker`

Runs the application JAR with `INSTANCE_ROLE=WORKER`. Scalable — Docker Compose generates the container name, which becomes each worker's `HOSTNAME` and therefore its worker ID in the distributed registry. Deploys UpSocket (4225), DownSocket (4226), and HTTP (8080) servers.

### `infra`

A lightweight Alpine container that acts as the bridge between external cloud orchestration and the coordinator's worker registry. Its job is to translate whatever the cloud config system knows about the worker fleet into coordinator API calls — registering workers as they come up and deregistering them as they go down. The coordinator waits for all registered workers to signal readiness before starting the frame loop.

The specific entrypoint (`infra-entrypoint-dynamic.sh`, mounted from `./scripts/infra/`) is deployment-provided and not part of the repository. `docker/scripts/infra-entrypoint.sh` is a development reference implementation that does static worker discovery by name. The infra container's configuration — coordinator host, worker prefix, expected count, retry parameters — is driven by environment variables in the compose file.

### `mongodb`

MongoDB in replica set mode (`rs0`). Replica set is required by Minare even in single-instance deployments because Vert.x's MongoDB client uses change streams, which require an oplog. The `docker/scripts/init.sh` entrypoint starts `mongod` in the background, waits for it to be ready, then initialises the replica set if not already done. The hostname alias `mongodb-rs` matches what the config file expects.

### `redis`

Uses `redis/redis-stack-server` (Redis + RedisJSON module). Persistence is enabled: RDB snapshots every 60 seconds and AOF append-only log. Data is stored in `docker/data/redis`.

### `kafka`

Confluent Platform Kafka in KRaft mode (no Zookeeper). Single-broker, single-partition development configuration. The framework uses the `minare.operations` topic; the `infra` container is meant to create this topic on startup (see flagged issues below).

### `kafka-ui`

Provectus Kafka UI, available at `http://localhost:8090` during development. Useful for inspecting message flow through `minare.operations`. Not needed for production.

### `haproxy`

Fronts all worker traffic. Three frontends:

| Frontend | Port | Purpose |
|---|---|---|
| `http_frontend` | 8080 | HTTP health checks and application HTTP routes |
| `up_socket_frontend` | 4225 | Client command WebSocket (UpSocket) |
| `down_socket_frontend` | 4226 | Client update WebSocket (DownSocket) |

HAProxy stats page is available at `http://localhost:8404/stats`.

Worker backends use Docker's internal DNS resolver (`127.0.0.11`) with `server-template` to dynamically discover all containers registered under the `worker` service name. Up to 10 worker slots are pre-allocated in the template.

---

## Ports exposed to the host

| Port | Service | Purpose |
|---|---|---|
| 4225 | HAProxy → workers | UpSocket (client commands) |
| 4226 | HAProxy → workers | DownSocket (client updates) |
| 8080 | HAProxy → workers | HTTP |
| 8090 | kafka-ui | Kafka browser |
| 8404 | HAProxy | Stats page |
| 9090 | app-coordinator | Coordinator admin endpoint |
| 9092 | kafka | Kafka (host access) |
| 27017 | mongodb | MongoDB (host access) |
| 6379 | redis | Redis (host access) |
| 5005 | app-coordinator | JVM debug port (when enabled) |

---

## Remote debugging

Set environment variables before running to enable JVM debug mode on the coordinator:

```bash
JAVA_DEBUG=true JAVA_DEBUG_PORT=5005 JAVA_DEBUG_SUSPEND=n ./run.sh 1 -n
```

| Variable | Default | Description |
|---|---|---|
| `JAVA_DEBUG` | `false` | Enable JDWP debug agent |
| `JAVA_DEBUG_PORT` | `5005` | Port the debugger listens on |
| `JAVA_DEBUG_SUSPEND` | `n` | `y` = pause JVM until debugger attaches; `n` = start immediately |

Use `JAVA_DEBUG_SUSPEND=y` to debug startup or clustering issues — the JVM will not proceed until a debugger connects on the configured port.

---

## Diagnostic scripts

All scripts in `docker/` operate against running containers and must be run from the `docker/` directory.

### `read-redis-keys.sh`

Dumps all keys and their JSON values from Redis:

```bash
./read-redis-keys.sh
```

Useful for inspecting live entity state.

### `read-kafka-logs.sh`

Tails the `minare.operations` topic from the beginning:

```bash
./read-kafka-logs.sh
```

Shows every operation the coordinator has received, in order.

### `read-snapshots.sh`

Reads all session snapshot collections from a MongoDB database:

```bash
./read-snapshots.sh node_graph
```

Snapshots are stored as `snapshot_<timestamp>` collections, sorted chronologically.

### `scan-logs.sh`

Greps a pattern across coordinator and worker log files simultaneously and prints a match count:

```bash
./scan-logs.sh "ERROR"
./scan-logs.sh "logical frame"
```

Requires logs to be present at `docker/logs/COORDINATOR-app-coordinator-runtime.log` and `docker/logs/WORKER-*-runtime.log`.

### `ops-report.sh`

A collection of grep patterns for tracing operation flow through coordinator and worker logs. Run manually as a shell snippet rather than as a script (it has no shebang). Useful as a starting point for debugging frame processing issues.

---

## Flagged issues

### NTP infrastructure is dead code

The Dockerfile installs `chrony` and `libcap`. Both coordinator and worker containers have `cap_add: SYS_TIME` and `NTP_URL=ntp.local` set. `start.sh` contains a complete NTP sync implementation — all commented out. This predates the logical frame approach, which eliminates the need for clock synchronisation. The `cap_add`, `NTP_URL`, `chrony`, and `libcap` can all be removed.

### `infra` Kafka topic creation in the reference implementation cannot work

The development reference script (`infra-entrypoint.sh`) attempts `docker exec kafka kafka-topics ...` from inside the container. Without the Docker socket mounted, `docker` is not available there. The `minare.operations` topic is currently created by Kafka's auto-create behaviour. A deployment-specific entrypoint should use the Kafka broker's network address directly rather than `docker exec`.

### `init-replica.sh` and `init-replica.js` are superseded

Two older MongoDB initialisation scripts sit alongside the one that's actually used (`init.sh`). `init-replica.sh` uses `mongo:27017` as the replica set host (the old service name) rather than `mongodb-rs:27017`. `init-replica.js` attempts to create an admin user that nothing else references. Neither is referenced by the compose file. Both can be deleted.

### `docker/setup.sh` is stale

References `minare-example/target/minare-app-fat.jar` (a path and name that don't exist), uses the old `docker-compose` v1 CLI, and tries to build a service named `app` which doesn't exist in the compose file. The actual workflow is handled by `run.sh`. `setup.sh` can be deleted or replaced with a note pointing to `run.sh`.

### HAProxy UpSocket backend uses round-robin

The `up_socket_backend` has `balance source` (sticky sessions by client IP) commented out with a note saying "USE THIS FOR PRODUCTION", and currently uses `balance roundrobin`. UpSocket connections are stateful — a client mid-session landing on a different worker will still work because connection state is in Redis and messages are routed over the event bus, but the added hop introduces latency. Source-based stickiness is the right default once you have more than one worker.

### `start.sh` dependency wait does nothing

`wait_for_dependencies()` only acts if `MONGO_URI` is set, which the compose file never sets. The Redis check is commented out. The function prints "Dependency checks complete" immediately without waiting for anything. This is harmless since Docker's `depends_on` with `condition: service_healthy` already handles ordering, but the function is misleading.
