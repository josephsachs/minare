# Minare Framework
Current version: 0.6.0

## Concept
Multiplayer games and collaborative simulations need consistency, determinism, and the ability to recover quickly
from failures affecting the integrity of mutable state. 

Minare explores logical frame coordination as a solution to such challenges. If the concept proves sound, 
Minare will provide a rich back-end framework for developers building multiplayer games, or any distributed application 
requiring strong consistency guarantees.

### Why Logical Frames?
Imagine you have to distribute work across a cluster of workers. Each operation might have implications for subsequent 
operations, so you need them to complete in order.

One approach might be to timestamp incoming work and pass that to workers. This is intuitively appealing: time is already
an ordering abstraction, and while clocks do drift there are external authorities to synchronize with. 

However, wall-clock time is not the *only* type of time, and workers are not abstractions but individual VMs: 
sometimes running on different metal, sometimes over different pipes. Variances in network latency, CPU speed, and 
GC throughput create time variances you cannot synchronize away. 

You can build in grace periods, have a leader make periodic progress checks and so on—but as you're doing this, more 
work is coming in.

Computing addresses this by abstracting process time as **cycles.** Game engines like Unity and Godot extend the idea 
with the concept of **frames**: discrete units of logical time. A frame might complete more or less quickly 
depending on the actual speed of work (impacted by the weight of the simulation, the user's hardware and, in lockstep
netcode, the speed of the slowest connection), but variances do not affect the integrity of the simulated gameworld.

Minare adopts the same approach, in theory protecting temporal ordering from clock drift and other variances by 
assigning operations to abstract frames, then coordinating the completion of these frames.

### Consistency First
The CAP theorem states that in distributed systems, consistency, availability and partition tolerance are competing 
aims: strong guarantees in one area imply weaker guarantees in another area.

Minare chooses consistency early and often. Work is stored in a durable message queue. There is one cluster coordinator 
and no leader election. Worker topology is fixed per session: when you replay a session, the hash ring will distribute 
work across the cluster in the same subsets. At the first sign of trouble, the entire cluster pauses; it could be 
for a few milliseconds to address a lagging worker, or for longer if frame replay is necessary to reconstruct state.

Snapshots and message queues can be stored as long as necessary to meet the needs of the application: for the 
length of a match, for a week, or indefinitely (e.g. for regulatory compliance). These two pieces should enable
reconstruction of the state.

One benefit of this is "time-travel debugging": a developer can locate the exact time when something went 
wrong and be certain they are addressing the true cause. 

### Caveats
~~"Parallel interleaving" remains an unsolved problem. While you can guarantee operations are replayed in the same frames,
in the same subsets, ordered the same, how do you ensure that closely-grouped, intra-frame operations complete in the same 
order across the cluster? Right now, your best bet is to configure a very fast frame (that is, one containing
fewer operations per); future efforts may include configurable per-frame work limits, single-thread pipelines for 
designated operations, and worker/scope affinities.~~ **\[MIN-201] Implement Operation affinity scope**

## Technical Details

### Architecture
Because Minare is intended to be not just a solution but tooling, the choice of technical stack requires a careful balance 
between my goals and the developer's experience. I chose 
- **JVM** because it performs better than Node.js or Python, is polyglot, doesn't require the developer to learn novel patterns like Rust or Go, and has a mature ecosystem;
- **Vert.x** because it scales up to handle massive socket pools without blinking; 
- **Redis** because it's a proven cache with good Json support and pub-sub for free; 
- **Hazelcast** because it has already solved numerous distributed consensus challenges that are outside my domain; 
- and **MongoDB** because it is schemaless and Json-native, has good graph lookups and, with replica sets, is quite durable. A graphing database was perhaps due a consideration here; it would not, in any case, be difficult to implement in-place. 

### History and Roadmap
0.1.0 - Transport layer, Entity, datastores, publish events, NodeGraph application

0.2.0 - Clustering, logical frames

0.3.0 - Pause, snapshot, timeline head

0.4.0 - Task/FixedTask, WorkUnit

0.5.0 - Serialization, application hooks, common interfaces

**0.6.0** - Config builder, modular infra, schema validation, more test coverage

0.7.0 - Affinity scope

0.8.0 - UpdateController, views

1.0.0 - Quick start

1.1.0 - Replay, improved session snapshot behavior, NodeGraph v2

1.2.0 - Operation recovery, monitoring tools

1.3.0 - Worker health monitoring

1.4.0 - OperationSet 

1.5.0 - Session branching, graph visitor builder

# Utilities
The framework repository contains several testing and debugging utilities. To use these, 
ensure you have installed the prerequisites:
   1. A Docker version that includes `docker compose`
   2. Java 24.0.1
   2. Maven 3.9.10
   3. Node.js 24.3.0
   4. artillery 2.0.21

## Integration Tests
The framework integration test suite creates a simulated docker environment against which to run assertion-based test cases. 
To test it,

1. Run `bash run.sh 1 -i`. 
2. Docker will build a container and run the tests. Results will be written to `docker/logs`. 
3. When they are complete, run `bash stop.sh`

## NodeGraph Application
The framework includes an example application demonstrating a simple graph of nodes which initialize at start-up. 
Clients can then submit operations which change the colors of nodes. Several smoke tests exist which demonstrate
this. 
1. Run `bash run.sh 2 -n`. (You can replace `2` with the number of workers you want to spin up.)
2. In a browser, navigate to `http://localhost:8080/index.html` and click the `Connect` button at the top of the screen. 
3. After a connection is established, on the upper-right, click `Show Grid View`. 
4. In your terminal, find the `/minare-example/artillery` path and run `artillery run color-wave-test.yml`. 
5. Watch the nodes change color in waves. After the final wave, each node should be colored black and have its version incremented to 40.
6. Run `bash stop.sh`

## License
© 2025 Joseph Sachs. All rights reserved. License TBD.