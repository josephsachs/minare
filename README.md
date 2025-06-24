# Minare Framework
Current version: 0.2.0

### Author's Note
Major pre-release version update is underway and the contents of this document will likely invalidate frequently.

### **NOTE: Minare is currently unreleased. Core systems require improvement and many planned changes await.**

## Architecture Overview

Minare provides a back-end framework for management of real-time data with many clients. The aim is to solve distributed state problems for game developers.

## Core Components

### Entity System

The foundation of Minare is Entity, the base class for objects on the state graph. Fields must be marked for persistence with the `@State` annotation. 

Entities can be defined with hierarchical and peer relationships, including to other types. The application developer can then leverage efficient relationship queries to support their own logic. 
 
## Near term roadmap

- Implement Kafka for ordering guarantees and replay
- Frame control + cluster orchestration for consistency
- New integration tests that better simulate expected user pattern

## Example Application

The framework includes an example application demonstrating a simple graph of nodes:

This application is used in the development of the framework and is as subject to change as it.

## Testing

Artillery is used for integration and performance testing.

## Getting Started

Coming soon