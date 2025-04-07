# Minare Framework

Minare is a scalable, real-time state synchronization framework for applications requiring consistent object graphs across multiple clients. Built with Vert.x, Kotlin, and MongoDB.

The extension application defines an object graph. Minare handles persistence, consistency, events and eventually frames. The application server is stateless; Mongo is the source of truth. The data flow is unidirectional. 

The implementer hooks into the framework by extending MinareApplication. They may also use their own Guice module to override framework services as needed.

**NOTE: Minare is currently a work-in-progress. Many planned changes await implementation.** 

## Architecture Overview

Minare facilitates bidirectional communication between clients and a stateless server layer, with MongoDB serving as the source of truth. This creates a unidirectional data flow:

1. Clients send mutations via Command Socket
2. Server validates and processes mutations
3. MongoDB persists changes
4. MongoDB change streams notify server of updates
5. Server broadcasts changes to clients via Update Socket

The result is eventual consistency across all clients with configurable mutation behavior at the field level.

## Core Components

### Entity System

The foundation of Minare is the extensible entity system:

- **Entity**: Base class for all application-defined objects in the state graph
    - Versioning with incremental counters
    - Ancestor/descendant relationship traversal
    - Serialization/deserialization

- **Annotation System**:
    - `@EntityType`: Marks a class as an entity with a specified type name
    - `@State`: Marks fields for persistence and synchronization
    - `@Parent`/`@ParentReference`: Defines references to parent entities
    - `@Child`/`@ChildReference`: Defines references to child entities
    - `@Mutable`: Defines fields that can be mutated with specified consistency level

- **Consistency Levels**:
    - `OPTIMISTIC`: Allow changes, resolve conflicts later if needed
    - `PESSIMISTIC`: Verify version before allowing changes
    - `STRICT`: Most restrictive, require exact version match

## Scaling Strategy

- Stateless server design for horizontal scaling
- MongoDB as the centralized state store
- Future DB shard architecture for large-scale deployments
- Future Kafka integration

## Example Application

The framework includes an example application demonstrating a simple graph of nodes:

- **Node**: Custom entity in the example application with parent/child relationships and an integer value
- **ExampleApplication**: Extends MinareApplication and initializes a sample node graph
- **ExampleEntityFactory**: Extends EntityFactory and creates the entities for the test application

This example serves as both a demonstration and a performance testing ground for the framework.

## Testing Methodology

The framework uses JUnit 5 with Vert.x extensions and Mockito for testing:

- Unit tests for entity operations and version bubbling
- Integration tests for socket communication
- Performance tests for concurrency and scaling

## Future Development

- Enhanced authorization and user management
- Channels for segmented entity updates
- Performance optimization based on test results
- Kafka integration for advanced messaging patterns
- UDP support for high-frequency, low-importance updates

## Getting Started

**Local Development**

Use `docker-compose` to raise the container in the `docker/` path.
Setup the IntelliJ project/module. The module source root should be `main`.

**Deployment**

Create the artifact bucket in your AWS account. Place your application `jar`. Use `minare-infrastructure.yaml` to create a CloudFormation stack.
