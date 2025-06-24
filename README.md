# Minare Framework
Current version: 0.2.0

### Author's Note
Major pre-release version update is underway and the contents of this document will likely invalidate frequently.

### **NOTE: Minare is currently unreleased. Core systems require improvement and many planned changes await.**

## Architecture Overview

Minare provides a back-end framework for management of real-time data with many clients. The aim is to solve distributed state problems for game developers.

## Core Components

### Entity System

The foundation of Minare is the extensible entity system:

**Entity**: Entity is the base class for objects on the state graph. Fields must be marked for persistence with the `@State` annotation. 

Entities can have parent (`@Parent`), child (`@Child`) and peer (`@Peer`) relationships with other entities, including other types. These are stored as references when persisted. The chain of parent-child relationships will affect version propogation (`@BubbleUp`, `@BubbleDown`), consistency guarantees (`@Mutate(rule=)`) and will be included an Entity's full context is considered.
 
## Near term roadmap

- Implement Kafka for ordering guarantees and replay
- Frame control + cluster orchestration for consistency guarantee
- New integration tests that better simulate expected user pattern

## Example Application

The framework includes an example application demonstrating a simple graph of nodes:

- **Node**: Custom entity in the example application with parent/child relationships and an integer value
- **ExampleApplication**: Extends MinareApplication and initializes a sample node graph
- **ExampleEntityFactory**: Extends EntityFactory and creates the entities for the test application

This stuff is used in the development of the framework and is as subject to change as it.

## Testing

Artillery is used for integration and performance testing.

## Getting Started

**Local Development**

Use `docker-compose` to raise the container in the `docker/` path.

Setup the IntelliJ project/module. The module source root should be `main`.

**Deployment**

Create the artifact bucket in your AWS account. Place your application `jar`. Use `minare-infrastructure.yaml` to create a CloudFormation stack.
