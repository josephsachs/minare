/**
 * Direct WebSocket connection functions for Artillery
 * These bypass Artillery's WebSocket mechanisms entirely
 */
const WebSocket = require('ws');

// Store for maintaining state between function calls
// Using a Map instead of a plain object for better performance with many entries
const userStores = new Map();

// Metrics for performance monitoring
const metrics = {
  connections: {
    attempted: 0,
    successful: 0,
    failed: 0,
    active: 0
  },
  operations: {
    mutations: {
      attempted: 0,
      successful: 0,
      failed: 0,
      responseTime: []
    },
    entityUpdates: 0
  }
};

/**
 * Create an isolated state object for a user
 */
function createUserState(userId) {
  return {
    id: userId,
    connected: false,
    connectionId: null,
    commandSocket: null,
    updateSocket: null,
    connectionTimeout: null,
    entities: {},
    pendingMutations: {},
    lastActivity: Date.now(),
    sessionStart: Date.now(),
    stats: {
      mutationsAttempted: 0,
      mutationsSucceeded: 0,
      entityUpdatesReceived: 0
    }
  };
}

/**
 * Set up WebSocket connections manually
 */
function setupWebSockets(requestParams, context, events, done) {
  // Initialize context.vars if it doesn't exist
  if (!context.vars) {
    context.vars = {};
  }

  const userId = `user-${Date.now()}-${Math.floor(Math.random() * 10000)}`;
  console.log(`[${userId}] Setting up WebSocket connections`);

  // Initialize user state with isolated state
  const userState = createUserState(userId);
  userStores.set(userId, userState);

  // Store userId in context for later steps
  context.vars.userId = userId;

  // Determine WebSocket URL from HTTP target
  const target = context.vars.target || 'http://localhost:8080';
  const wsTarget = target.replace('http://', 'ws://').replace('https://', 'wss://');

  // Set connection timeout
  userState.connectionTimeout = setTimeout(() => {
    if (userStores.has(userId) && !userState.connected) {
      console.error(`[${userId}] Connection establishment timed out`);
      cleanupUser(userId);
    }
  }, 30000); // 30 second timeout for connection establishment

  // Create command socket
  try {
    metrics.connections.attempted++;

    const commandSocket = new WebSocket(`${wsTarget}/ws`);

    commandSocket.on('open', () => {
      console.log(`[${userId}] Command socket connected`);
      if (userStores.has(userId)) {
        userState.commandSocket = commandSocket;
        userState.lastActivity = Date.now();
        metrics.connections.successful++;
        metrics.connections.active++;
      } else {
        // User was cleaned up before socket opened
        try { commandSocket.close(); } catch (err) {}
      }
    });

    commandSocket.on('message', (data) => {
      try {
        if (!userStores.has(userId)) return;
        userState.lastActivity = Date.now();

        const message = JSON.parse(data.toString());
        console.log(`[${userId}] Received command message: ${message.type}`);

        if (message.type === 'connection_confirm') {
          // Store connection ID
          const connectionId = message.connectionId;
          userState.connectionId = connectionId;
          console.log(`[${userId}] Received connection ID: ${connectionId}`);

          // Connect update socket
          connectUpdateSocket(userId, wsTarget, connectionId);
        } else if (message.type === 'sync') {
          if (message.data && message.data.entities) {
            console.log(`[${userId}] Received ${message.data.entities.length} entities from sync`);

            // Process entities
            message.data.entities.forEach(entity => {
              if (entity._id) {
                userState.entities[entity._id] = {
                  id: entity._id,
                  version: entity.version,
                  state: entity.state,
                  type: entity.type
                };
                userState.stats.entityUpdatesReceived++;
                metrics.operations.entityUpdates++;
              }
            });
          }
        } else if (message.type === 'mutation_response' || message.type === 'mutation_success') {
          console.log(`[${userId}] Received mutation response: ${message.type}`);

          // Handle mutation response
          handleMutationResponse(userId, message);
        }
      } catch (error) {
        console.error(`[${userId}] Error processing command message:`, error);
      }
    });

    commandSocket.on('error', (error) => {
      console.error(`[${userId}] Command socket error:`, error);
      metrics.connections.failed++;
    });

    commandSocket.on('close', (code, reason) => {
      console.log(`[${userId}] Command socket closed: ${code} ${reason}`);

      // Clean up user if their command socket closed
      if (userStores.has(userId)) {
        cleanupUser(userId);
      }
    });

    done();
  } catch (error) {
    console.error(`[${userId}] Failed to setup WebSockets:`, error);
    metrics.connections.failed++;
    cleanupUser(userId);
    done(error);
  }
}

/**
 * Connect the update socket with the connection ID
 */
function connectUpdateSocket(userId, wsTarget, connectionId) {
  if (!userStores.has(userId)) {
    console.error(`[${userId}] User not found for update socket connection`);
    return;
  }

  const userState = userStores.get(userId);
  console.log(`[${userId}] Connecting update socket with connection ID: ${connectionId}`);

  try {
    metrics.connections.attempted++;

    // Create update socket
    const updateSocket = new WebSocket(`${wsTarget}/ws/updates`);

    updateSocket.on('open', () => {
      console.log(`[${userId}] Update socket connected, sending association message`);

      // Send connection ID to associate the update socket
      const associationMessage = {
        connectionId: connectionId
      };
      updateSocket.send(JSON.stringify(associationMessage));

      // Store the socket only if the user still exists
      if (userStores.has(userId)) {
        userState.updateSocket = updateSocket;
        userState.lastActivity = Date.now();
      } else {
        // User was cleaned up before socket opened
        try { updateSocket.close(); } catch (err) {}
      }
    });

    updateSocket.on('message', (data) => {
      try {
        if (!userStores.has(userId)) return;
        userState.lastActivity = Date.now();

        const message = JSON.parse(data.toString());

        if (message.type === 'update_socket_confirm') {
          console.log(`[${userId}] Update socket confirmed, connection established`);
          userState.connected = true;
          metrics.connections.successful++;

          // Clear the connection timeout as we're now fully connected
          if (userState.connectionTimeout) {
            clearTimeout(userState.connectionTimeout);
            userState.connectionTimeout = null;
          }
        } else if (message.type === 'sync') {
          if (message.data && message.data.entities) {
            console.log(`[${userId}] Received ${message.data.entities.length} entities from update socket`);

            message.data.entities.forEach(entity => {
              if (entity._id) {
                // Skip if we have a pending mutation for this entity
                if (userState.pendingMutations[entity._id]) return;

                userState.entities[entity._id] = {
                  id: entity._id,
                  version: entity.version,
                  state: entity.state,
                  type: entity.type
                };
                userState.stats.entityUpdatesReceived++;
                metrics.operations.entityUpdates++;
              }
            });
          }
        } else if (message.update && message.update.entities) {
          console.log(`[${userId}] Received ${message.update.entities.length} entity updates`);

          message.update.entities.forEach(entity => {
            const entityId = entity._id || entity.id;
            if (entityId) {
              // Skip if we have a pending mutation for this entity
              if (userState.pendingMutations[entityId]) return;

              userState.entities[entityId] = {
                id: entityId,
                version: entity.version,
                state: entity.state,
                type: entity.type
              };
              userState.stats.entityUpdatesReceived++;
              metrics.operations.entityUpdates++;
            }
          });
        }
      } catch (error) {
        console.error(`[${userId}] Error processing update message:`, error);
      }
    });

    updateSocket.on('error', (error) => {
      console.error(`[${userId}] Update socket error:`, error);
      metrics.connections.failed++;
    });

    updateSocket.on('close', (code, reason) => {
      console.log(`[${userId}] Update socket closed: ${code} ${reason}`);

      // Mark user as disconnected and clean up
      if (userStores.has(userId)) {
        userState.connected = false;
        metrics.connections.active--;

        // Since update socket closed, clean up the whole user
        cleanupUser(userId);
      }
    });
  } catch (error) {
    console.error(`[${userId}] Failed to connect update socket:`, error);
    metrics.connections.failed++;
  }
}

/**
 * Check the status of connections
 */
function checkConnections(requestParams, context, events, done) {
  if (!context.vars) {
    context.vars = {};
  }

  const userId = context.vars.userId;

  if (!userStores.has(userId)) {
    console.error(`[${userId}] User not found for connection check`);
    done();
    return;
  }

  const userState = userStores.get(userId);

  // Both sockets must be connected and the connection ID must be confirmed
  const isFullyConnected = userState.commandSocket &&
                           userState.updateSocket &&
                           userState.connectionId &&
                           userState.connected;

  console.log(`[${userId}] Connection status check:
    Command socket: ${userState.commandSocket ? 'Connected' : 'Not connected'}
    Connection ID: ${userState.connectionId || 'Not received'}
    Update socket: ${userState.updateSocket ? 'Connected' : 'Not connected'}
    Fully connected: ${userState.connected ? 'Yes' : 'No'}
    Overall status: ${isFullyConnected ? 'READY' : 'NOT READY'}
    Entities: ${Object.keys(userState.entities).length}
  `);

  // Don't proceed with test actions until fully connected
  if (!isFullyConnected) {
    console.log(`[${userId}] Waiting for connection establishment...`);
  }

  done();
}

/**
 * Handle a mutation response from the server
 */
function handleMutationResponse(userId, message) {
  if (!userStores.has(userId)) return;

  const userState = userStores.get(userId);

  const entityId = message.entity?._id;
  if (!entityId || !userState.pendingMutations[entityId]) {
    return;
  }

  const pendingMutation = userState.pendingMutations[entityId];
  const newVersion = message.entity.version;

  // Update our local entity store with the new version
  if (userState.entities[entityId]) {
    userState.entities[entityId].version = newVersion;

    // Update the color in our local state
    if (userState.entities[entityId].state) {
      userState.entities[entityId].state.color = pendingMutation.newColor;
    }
  }

  // Calculate latency
  const latency = Date.now() - pendingMutation.timestamp;
  metrics.operations.mutations.responseTime.push(latency);

  // Update statistics
  userState.stats.mutationsSucceeded++;
  metrics.operations.mutations.successful++;

  // Log success
  console.log(`[${userId}] Mutation succeeded for entity ${entityId}, latency: ${latency}ms`);

  // Remove from pending mutations
  delete userState.pendingMutations[entityId];
}

/**
 * Perform test operations (send a mutation command)
 */
function performTest(requestParams, context, events, done) {
  if (!context.vars) {
    context.vars = {};
  }

  const userId = context.vars.userId;

  if (!userStores.has(userId) || !userStores.get(userId).connected) {
    console.log(`[${userId}] Not fully connected, skipping test operations`);
    done();
    return;
  }

  const userState = userStores.get(userId);
  userState.lastActivity = Date.now();

  // Get all node entities
  const nodeEntities = Object.values(userState.entities).filter(entity =>
    (entity.state && entity.state.label) ||
    (entity.type === 'Node') ||
    (entity.type && typeof entity.type === 'string' && entity.type.toLowerCase().includes('node')) ||
    (entity.state && entity.state.color)
  );

  if (nodeEntities.length === 0) {
    console.log(`[${userId}] No node entities found, skipping mutation`);
    done();
    return;
  }

  // Select a random node
  const randomIndex = Math.floor(Math.random() * nodeEntities.length);
  const targetNode = nodeEntities[randomIndex];

  // Generate a new color
  const currentColor = targetNode.state?.color || '#CCCCCC';
  const newColor = generateRandomColor(currentColor);

  // Prepare mutation command
  const command = {
    command: 'mutate',
    entity: {
      _id: targetNode.id,
      version: targetNode.version,
      state: {
        color: newColor
      }
    }
  };

  // Track this pending mutation
  userState.pendingMutations[targetNode.id] = {
    timestamp: Date.now(),
    oldVersion: targetNode.version,
    newColor: newColor
  };

  // Update statistics
  userState.stats.mutationsAttempted++;
  metrics.operations.mutations.attempted++;

  // Send the command
  try {
    userState.commandSocket.send(JSON.stringify(command));
    console.log(`[${userId}] Sent mutation for node ${targetNode.id} color to ${newColor}`);
  } catch (error) {
    console.error(`[${userId}] Error sending mutation:`, error);
    metrics.operations.mutations.failed++;
    delete userState.pendingMutations[targetNode.id];
  }

  done();
}

/**
 * Generate a random color variation
 */
function generateRandomColor(baseColor = '#CCCCCC') {
  // Parse the base color
  let r = parseInt(baseColor.slice(1, 3), 16);
  let g = parseInt(baseColor.slice(3, 5), 16);
  let b = parseInt(baseColor.slice(5, 7), 16);

  // Apply small random changes to each component
  const variation = 30;
  r = Math.max(0, Math.min(255, r + Math.floor(Math.random() * variation * 2) - variation));
  g = Math.max(0, Math.min(255, g + Math.floor(Math.random() * variation * 2) - variation));
  b = Math.max(0, Math.min(255, b + Math.floor(Math.random() * variation * 2) - variation));

  // Convert back to hex
  return `#${r.toString(16).padStart(2, '0')}${g.toString(16).padStart(2, '0')}${b.toString(16).padStart(2, '0')}`;
}

// Set up periodic metrics logging
const metricsInterval = setInterval(logMetrics, 60000); // Log every minute

/**
 * Clean up stale connections
 */
function cleanupStaleConnections() {
  const now = Date.now();
  const staleThreshold = 300000; // 5 minutes

  let staleCount = 0;

  userStores.forEach((userState, userId) => {
    // Check if the user has been inactive
    if (now - userState.lastActivity > staleThreshold) {
      console.log(`[${userId}] Cleaning up stale connection (inactive for ${(now - userState.lastActivity)/1000}s)`);
      cleanupUser(userId);
      staleCount++;
    }
  });

  if (staleCount > 0) {
    console.log(`Cleaned up ${staleCount} stale connections`);
  }
}

// Run stale connection cleanup every 2 minutes
const staleCleanupInterval = setInterval(cleanupStaleConnections, 120000);

/**
 * Log performance metrics
 */
function logMetrics() {
  console.log('===== PERFORMANCE METRICS =====');
  console.log('Time:', new Date().toISOString());

  console.log('\nConnection Metrics:');
  console.log(`  Attempted: ${metrics.connections.attempted}`);
  console.log(`  Successful: ${metrics.connections.successful}`);
  console.log(`  Failed: ${metrics.connections.failed}`);
  console.log(`  Currently Active: ${metrics.connections.active}`);
  console.log(`  Success Rate: ${metrics.connections.attempted > 0 ?
    ((metrics.connections.successful / metrics.connections.attempted) * 100).toFixed(2) : 0}%`);

  console.log('\nOperation Metrics:');
  console.log('  Mutations:');
  console.log(`    Attempted: ${metrics.operations.mutations.attempted}`);
  console.log(`    Successful: ${metrics.operations.mutations.successful}`);
  console.log(`    Failed: ${metrics.operations.mutations.failed}`);

  const successRate = metrics.operations.mutations.attempted > 0 ?
    ((metrics.operations.mutations.successful / metrics.operations.mutations.attempted) * 100).toFixed(2) : 0;
  console.log(`    Success Rate: ${successRate}%`);

  if (metrics.operations.mutations.responseTime.length > 0) {
    const times = metrics.operations.mutations.responseTime;
    const avgTime = times.reduce((sum, time) => sum + time, 0) / times.length;
    const minTime = Math.min(...times);
    const maxTime = Math.max(...times);

    // Calculate percentiles
    const sortedTimes = [...times].sort((a, b) => a - b);
    const p50Index = Math.floor(sortedTimes.length * 0.5);
    const p95Index = Math.floor(sortedTimes.length * 0.95);
    const p99Index = Math.floor(sortedTimes.length * 0.99);

    console.log(`    Response Time (ms):`);
    console.log(`      Average: ${avgTime.toFixed(2)}`);
    console.log(`      Min: ${minTime}`);
    console.log(`      Max: ${maxTime}`);
    console.log(`      p50: ${sortedTimes[p50Index]}`);
    console.log(`      p95: ${sortedTimes[p95Index]}`);
    console.log(`      p99: ${sortedTimes[p99Index]}`);
  }

  console.log(`  Entity Updates Received: ${metrics.operations.entityUpdates}`);

  console.log('\nActive Users:', userStores.size);
  console.log('================================\n');
}

/**
 * Clean up connections for a user
 */
function cleanupUser(userId) {
  if (!userStores.has(userId)) return;

  const userState = userStores.get(userId);

  // Clear any pending timeouts
  if (userState.connectionTimeout) {
    clearTimeout(userState.connectionTimeout);
    userState.connectionTimeout = null;
  }

  if (userState.updateSocket) {
    try {
      userState.updateSocket.close();
    } catch (err) {
      // Ignore errors when closing
    }
    userState.updateSocket = null;
  }

  if (userState.commandSocket) {
    try {
      userState.commandSocket.close();
    } catch (err) {
      // Ignore errors when closing
    }
    userState.commandSocket = null;
  }

  if (userState.connected) {
    userState.connected = false;
    metrics.connections.active--;
  }

  // Remove from store to free memory
  userStores.delete(userId);

  console.log(`[${userId}] User cleaned up, current active users: ${userStores.size}`);
}

/**
 * Clean up all connections for the current user
 */
function cleanupConnections(requestParams, context, events, done) {
  if (!context.vars) {
    context.vars = {};
  }

  const userId = context.vars.userId;

  if (userId && userStores.has(userId)) {
    console.log(`[${userId}] Cleaning up connections at test end`);
    cleanupUser(userId);

    // Force a metrics log at cleanup
    logMetrics();
  }

  done();
}

/**
 * Clean up all intervals when the process exits
 */
process.on('SIGINT', () => {
  clearInterval(metricsInterval);
  clearInterval(staleCleanupInterval);

  console.log('Cleaned up intervals before exit');
  console.log('Final metrics:');
  logMetrics();

  process.exit(0);
});

/**
 * Handle unexpected errors at the process level
 */
process.on('uncaughtException', (error) => {
  console.error('Uncaught exception:', error);

  // Log the current state
  console.log(`Active users: ${userStores.size}`);
  logMetrics();

  // Continue execution - don't crash the process
  // In a real-world scenario, you might want different behavior
});

module.exports = {
  setupWebSockets,
  checkConnections,
  performTest,
  cleanupConnections
};