# Minare Framework
Current version: 0.2.0

### Author's Note
Major pre-release version update is underway and the contents of this document will likely invalidate frequently.

### **NOTE: Minare is currently unreleased. Core systems require improvement and many planned changes await.**

## Architecture Overview

Minare provides a back-end framework for management of real-time data with many clients.

## Core Components

### Entity System

The foundation of Minare is the extensible entity system:

**Entity**: Entity is the base class for objects on the state graph. Fields must be marked for persistence with the `@State` annotation. 

Entities can have parent (`@Parent`) and child (`@Child`) relationships with other entities, including other types. These are stored as references when persisted. The chain of parent-child relationships will affect version propogation (`@BubbleUp`, `@BubbleDown`), consistency guarantees (`@Mutate(rule=)`) and will be included an Entity's full context is considered.
 
## Near term roadmap

- Finetune batch updater
- Implement Kafka + frame control + cluster orchestration
- New integration tests that better simulate expected user pattern

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
