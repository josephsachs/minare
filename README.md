# Minare Framework

Minare is a scalable, real-time state synchronization framework for applications requiring consistent object graphs across multiple clients. Built with Vert.x, Kotlin, and MongoDB.

The extension application defines an object graph. The application server is stateless; Mongo is the source of truth. The data flow is unidirectional. 

The implementer hooks into the framework by extending MinareApplication. They may also use their own Guice module to override framework services as needed.

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

### Network Layer

- **CommandSocketManager**: Handles client connections for receiving mutations
- **UpdateSocketManager**: Manages sockets for broadcasting updates to clients
- **ConnectionManager**: Associates command and update sockets with client connections
- **WebSocketRoutes**: Exposes endpoints for clients to establish connections

### State Management

- **MongoChangeStreamConsumer**: Listens for MongoDB changes and propagates updates
- **EntityStore**: Persists entities and manages their lifecycle
- **ConnectionStore**: Tracks client connections and their associated sockets

### Reflection & Metadata

- **EntityReflector**: Analyzes entity classes using reflection
- **ReflectionCache**: Stores metadata about entity structure for efficiency
- **EntityFactory**: Creates entity instances based on type

## Synchronization Mechanisms

### Version Bubbling

When an entity changes, its version is incremented. This change may propagate to ancestors based on annotation configuration:

1. An entity is mutated, triggering version increment
2. JGraphT is used to build an ancestor graph
3. Version changes bubble up to parents according to bubble_version rules

### Change Stream Processing

MongoDB change streams notify the server of database changes:

1. Server subscribes to entity collection changes
2. Changes are transformed into update messages
3. Messages are broadcast to relevant clients

## Client Communication

### Command Socket

- Clients connect to receive a unique connection ID
- Clients send mutation requests over this socket
- Server validates and processes these requests

### Update Socket

- Clients establish a second socket using their connection ID
- Server pushes entity updates to clients through this socket
- Updates include entity ID, version, and changed state

## Performance Considerations

### Optimizations

- Reflection caching for efficient entity metadata access
- Field-level mutation control for fine-grained consistency
- Ancestor graph calculation with minimal database queries
- Version bubbling control to reduce unnecessary updates

### Planned Features

- Command batching for reduced MongoDB write operations
- State transition rules to validate mutations at the framework level
- Frame loop for server-initiated updates and command queues
- Distributed synchronization for horizontal scaling

## Scaling Strategy

- Stateless server design for horizontal scaling
- MongoDB as the centralized state store
- Possible key-sharded architecture for large-scale deployments
- Future Kafka integration for cross-shard communication

## Example Application

The framework includes an example application demonstrating a simple graph of nodes:

- **Node**: Entity with parent/child relationships and an integer value
- **ExampleApplication**: Initializes a sample node graph
- **ExampleEntityFactory**: Creates entities for the test application

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

[Instructions for building, deploying, and using the framework will be added as development progresses]