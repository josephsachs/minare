# Minare Framework
### **NOTE: Minare is currently unreleased. Core systems require improvement and many planned changes await.**

Minare is a scalable, real-time state synchronization framework for applications requiring eventually consistent object graphs across many clients. Built with Vert.x, Kotlin, and MongoDB.

The implementer hooks into the framework by defining Entity types and extending MinareApplication. They may also use their own Guice module to override framework services as needed.

## Architecture Overview

Minare facilitates decoupled unidirectional data flow by leveraging DB as the source of truth and relying on event-driven behavior. Vert.x's requirement of thread-safety and clustering features should aid with horizontal scaling.

## Core Components

### Entity System

The foundation of Minare is the extensible entity system:

**Entity**: Entity is the base class for objects on the state graph. Fields must be marked for persistence with the `@State` annotation. 

Entities can have parent (`@Parent`) and child (`@Child`) relationships with other entities, including other types. These are stored as references when persisted. The chain of parent-child relationships will affect version propogation (`@BubbleUp`, `@BubbleDown`), consistency guarantees (`@Mutate(rule=)`) and will be included an Entity's full context is considered.
 
## Near term roadmap

- **Bug:** Thread unsafe code in Update causing dropped updates
- Continue to assess where processes can be isolated
- Determine whether it is necessary to refactor the datastore
- Message queues, particularly on the Update side
- New integration tests that better simulate expected user pattern
- Deploy to AWS with the infra stack

## Example Application

The framework includes an example application demonstrating a simple graph of nodes:

- **Node**: Custom entity in the example application with parent/child relationships and an integer value
- **ExampleApplication**: Extends MinareApplication and initializes a sample node graph
- **ExampleEntityFactory**: Extends EntityFactory and creates the entities for the test application

This stuff is used in the development of the framework and is as subject to change as it.

## Testing Methodology

The framework uses JUnit 5 with Vert.x extensions and Mockito for testing entity functions and version bubbling**

Artillery used for integration and performance testing

## Getting Started

**Local Development**

Use `docker-compose` to raise the container in the `docker/` path.

Setup the IntelliJ project/module. The module source root should be `main`.

**Deployment**

Create the artifact bucket in your AWS account. Place your application `jar`. Use `minare-infrastructure.yaml` to create a CloudFormation stack.

## Endnotes

\** I'm focusing on Entity as this is where most of the complexity lives. We could use mocks to cover the DBAL, socket management and so on, but integration tests will do during early development. 