# Minare Framework
Current version: 0.2.0

### Author's Note
Major pre-release version update is underway and the contents of this document will likely invalidate frequently.

### **NOTE: Minare is currently unreleased. Core systems require improvement and many planned changes await.**

## Overview

Minare provides a back-end framework for management of real-time data with many clients. The aim is to solve distributed state problems for game developers.

## Stack

- **Vert.x**: cluster orchestration, socket management
- **Redis**: source of truth for entity state, publication
- **MongoDB**: secondary datastore for entity relationships, snapshots
- **Kafka**: delivery guarantee, audit and replay

## Principles

- Domain agnostic
- Consistency above all
- Unidirectional flow

## Example Application

The framework includes an example application demonstrating a simple graph of nodes:

This application is used in the development of the framework and is as subject to change as it.

## Testing

Artillery is used for integration and performance testing.

## Getting Started

TBD