# Minare Framework

A framework for testing WebSocket-based real-time state synchronization at scale, with emphasis on state validation and connection management.

## Core Features

- Real-time state synchronization using WebSockets
- State validation using rolling checksums with 12-second window
- Distributed state caching with Hazelcast
- Channel-based subscriptions with user filtering capability
- DynamoDB-based persistence for connections and checksums
- Flexible entity state management

## Architecture

The framework is built around several key components:

### State Management
- `EntityStateManager`: Handles state updates and distribution
- `StateCache`: Provides distributed caching via Hazelcast
- `ChecksumManager`: Manages rolling window state validation

### Connection Management  
- `WebSocketManager`: Handles WebSocket connections and message routing
- `ConnectionManager`: Manages connection lifecycle and metadata
- `WebSocketRoutes`: Configures WebSocket endpoints

### Data Persistence
- `DynamoDBManager`: Manages DynamoDB tables for persistence
- Uses tables for subscriptions and checksums with TTL

### Implementation Support
- `AbstractEntityController`: Base class for implementing custom entity logic
- `ExampleEntityController`: Reference implementation showing basic usage

## Getting Started

1. Add dependencies in your pom.xml
2. Implement your own EntityController extending AbstractEntityController
3. Configure DynamoDB (local or AWS)
4. Start the server

## Example Implementation

The framework includes an example implementation showing:
- Basic entity state management
- WebSocket client/server communication
- Checksum validation
- Connection management

See the `example` package for reference implementation.

## Load Testing

The framework is designed to support load testing scenarios:
- High-frequency state updates
- Large numbers of concurrent connections
- State validation under load
- Connection cleanup and resource management

## Development

### Prerequisites
- Java 11+
- Maven
- DynamoDB Local or AWS Account
- Hazelcast

### Building
bash
mvn clean install


### Structure
```com.minare
├── config
│   ├── GuiceModule.java         // DI configuration
│   └── HazelcastConfig.java     // Distributed cache setup
│
├── core
│   ├── state
│   │   ├── EntityStateManager.java     
│   │   ├── StateCache.java            
│   │   └── ChecksumManager.java       
│   │
│   ├── websocket
│   │   ├── WebSocketManager.java      
│   │   └── ConnectionManager.java     
│   │
│   └── models
│       ├── StateUpdate.java           
│       └── Subscription.java          
│
├── controllers
│   ├── AbstractEntityController.java  
│   └── EntityController.java          
│
├── persistence
│   └── DynamoDBManager.java          
│
├── example
│   └── ExampleEntityController.java   // Stub implementation
│
└── Main.java```
