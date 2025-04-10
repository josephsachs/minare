# Minare Framework

### Author's Note
I worked on this for two intense weeks to produce the prototype, after which I took a break. My goal was to gain some practical experience implementing a realtime data feed at scale.

I made it a framework for two reasons:
1) I have more than one idea how to use the finished product.
2) Portfolios are the place for willful perversity in the name of challenge.

I chose Vert.x because:
1) I wanted to learn Java.
2) Vert.x's reputation for high throughput.

In retrospect, Node.js or plain Java (if I wanted active memory management) would have been fine. While MongoDB suits the data structure well, it is slower; in other words, it's unlikely that server throughput would ever become the bottleneck.

In its current state, the pipeline works but is not efficient. States stay synchronized under moderate load but rapid updates quickly overwhelm the distributor, resulting in inconsistency. The solution to this likely includes some combination of
1) batching updates on the stream consumer's side,
2) using a message queue (to ensure delivery) and
3) a more efficient publisher.

If list that makes you think of a couple of product names, well, me too but let's not say them out loud.

Finally, we get to the "at scale" part. I will need to handle this carefully as costs add up when you start deploying autoscale groups.

This remains a project I plan to revisit when time allows. I estimate the timeline for the above at 14-30 more long days.

### **NOTE: Minare is currently unreleased. Core systems require improvement and many planned changes await.**

## Architecture Overview

Minare facilitates decoupled unidirectional data flow by leveraging DB as the source of truth and relying on event-driven behavior. Vert.x's requirement of thread-safety and clustering features should aid with horizontal scaling.

## Core Components

### Entity System

The foundation of Minare is the extensible entity system:

**Entity**: Entity is the base class for objects on the state graph. Fields must be marked for persistence with the `@State` annotation. 

Entities can have parent (`@Parent`) and child (`@Child`) relationships with other entities, including other types. These are stored as references when persisted. The chain of parent-child relationships will affect version propogation (`@BubbleUp`, `@BubbleDown`), consistency guarantees (`@Mutate(rule=)`) and will be included an Entity's full context is considered.
 
## Near term roadmap

- Optimize the updater for load
- Continue to assess where processes can be isolated
- Audit all caches for thread- and instance- safety requirements
- Implement a message queue, particularly on the Update side
- New integration tests that better simulate expected user pattern
- Deploy to AWS with the infra stack

## Example Application

The framework includes an example application demonstrating a simple graph of nodes:

- **Node**: Custom entity in the example application with parent/child relationships and an integer value
- **ExampleApplication**: Extends MinareApplication and initializes a sample node graph
- **ExampleEntityFactory**: Extends EntityFactory and creates the entities for the test application

This stuff is used in the development of the framework and is as subject to change as it.

## Testing

The framework uses JUnit 5 with Vert.x extensions and Mockito for unit tests. I'm focusing on Entity as this is where most of the complexity lives. We could use mocks to cover the DBAL, socket management and so on, but integration tests will do during early development. 

Artillery used for integration and performance testing.

## Getting Started

**Local Development**

Use `docker-compose` to raise the container in the `docker/` path.

Setup the IntelliJ project/module. The module source root should be `main`.

**Deployment**

Create the artifact bucket in your AWS account. Place your application `jar`. Use `minare-infrastructure.yaml` to create a CloudFormation stack.
