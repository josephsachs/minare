/**
 * Complex simulation with multiple user types for Artillery load testing
 *
 * User types:
 * - Lurker: Mutates one entity every minute, mostly listens
 * - Twitchy: Mutates 1-3 linked entities every 0.3-1 sec, for ~2 minutes
 * - Heavy-Handed: Mutates 5-10 entities every ~15 sec (prefers blue hues)
 * - Big Tick: Mutates 10-16 entities every minute
 */
const WebSocket = require('ws');

// Use a Map instead of a plain object for better performance with many entries
const userStores = new Map();

// Separate aggregator for metrics by user type
const metricsByUserType = {
  lurker: { mutations: 0, successRate: [], responseTime: [] },
  twitchy: { mutations: 0, successRate: [], responseTime: [] },
  heavyHanded: { mutations: 0, successRate: [], responseTime: [] },
  bigTick: { mutations: 0, successRate: [], responseTime: [] }
};

// Global stats that don't depend on specific users
const globalStats = {
  totalConnections: 0,
  activeConnections: 0,
  totalMutations: 0,
  successfulMutations: 0,
  failedMutations: 0,
  userTypes: {
    lurker: 0,
    twitchy: 0,
    heavyHanded: 0,
    bigTick: 0
  }
};

// User type definitions
const userTypes = {
  lurker: {
    name: 'Lurker',
    mutationInterval: { min: 55, max: 65 },
    entitiesPerMutation: { min: 1, max: 1 },
    abortChance: 0.25,
    sessionDuration: { min: 1800, max: 3600 },
    colorPreference: 'increment',
    activityPattern: 'constant'
  },
  twitchy: {
    name: 'Twitchy',
    mutationInterval: { min: 0.3, max: 1 },
    entitiesPerMutation: { min: 1, max: 3 },
    abortChance: 0.1,
    sessionDuration: { min: 110, max: 130 },
    colorPreference: 'green',
    activityPattern: 'burst'
  },
  heavyHanded: {
    name: 'Heavy-Handed',
    mutationInterval: { min: 13, max: 17 },
    entitiesPerMutation: { min: 5, max: 10 },
    abortChance: 0.05,
    sessionDuration: { min: 600, max: 1200 },
    colorPreference: 'blue',
    activityPattern: 'constant'
  },
  bigTick: {
    name: 'Big Tick',
    mutationInterval: { min: 55, max: 65 },
    entitiesPerMutation: { min: 10, max: 16 },
    abortChance: 0.15,
    sessionDuration: { min: 900, max: 1800 },
    colorPreference: 'random',
    activityPattern: 'constant'
  }
};

/**
 * Create isolated state for each user
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
    linkedEntities: {},
    simulationRunning: false,
    userType: null,
    lastActivity: Date.now(),
    sessionStart: Date.now(),
    nextMutationTime: null,
    stats: {
      mutationsAttempted: 0,
      mutationsSucceeded: 0,
      entityUpdatesReceived: 0,
      entitiesMutated: 0,
      lastMutationTime: 0
    }
  };
}

// Setup WebSockets using promises
function setupWebSockets(requestParams, context, events) {
  return new Promise((resolve, reject) => {
    if (!context.vars) {
      context.vars = {};
    }

    const userId = `user-${Date.now()}-${Math.floor(Math.random() * 10000)}`;
    console.log(`[${userId}] Setting up WebSocket connections`);

    // Initialize user state
    const userState = createUserState(userId);
    userStores.set(userId, userState);

    // Store userId in context for later steps
    context.vars.userId = userId;

    // Determine WebSocket URL from HTTP target
    const target = context.vars.target || 'http://localhost:8080';
    const wsTarget = target.replace('http://', 'ws://').replace('https://', 'wss://');
    context.vars.wsTarget = wsTarget;

    // Read config variables
    context.vars.minMutationInterval = context.vars.minMutationInterval || 10;
    context.vars.maxMutationInterval = context.vars.maxMutationInterval || 20;
    context.vars.mutationProbability = context.vars.mutationProbability || 0.25;

    // Set connection timeout
    userState.connectionTimeout = setTimeout(() => {
      if (userStores.has(userId) && !userState.connected) {
        console.error(`[${userId}] Connection establishment timed out`);
        cleanupUser(userId);
      }
    }, 30000); // 30 second timeout for connection establishment

    // Create command socket
    try {
      const commandSocket = new WebSocket(`${wsTarget}/ws`);

      commandSocket.on('open', () => {
        console.log(`[${userId}] Command socket connected`);

        if (userStores.has(userId)) {
          userState.commandSocket = commandSocket;
          globalStats.totalConnections++;
          globalStats.activeConnections++;
        } else {
          // User was cleaned up before socket opened
          try { commandSocket.close(); } catch (err) {}
        }
      });

      commandSocket.on('message', (data) => {
        try {
          if (!userStores.has(userId)) return;

          const message = JSON.parse(data.toString());

          if (message.type === 'connection_confirm') {
            // Store connection ID
            const connectionId = message.connectionId;
            userState.connectionId = connectionId;
            console.log(`[${userId}] Received connection ID: ${connectionId}`);

            // Connect update socket
            connectUpdateSocket(userId, wsTarget, connectionId);
          } else if (message.type === 'sync') {
            if (message.data && message.data.entities) {
              const entityCount = message.data.entities.length;
              console.log(`[${userId}] Received ${entityCount} entities from sync`);

              // Process entities
              message.data.entities.forEach(entity => {
                if (entity._id) {
                  userState.entities[entity._id] = {
                    id: entity._id,
                    version: entity.version,
                    state: entity.state,
                    type: entity.type
                  };

                  // Track links between entities
                  trackEntityRelationships(userId, entity);
                }
              });

              userState.stats.entityUpdatesReceived += entityCount;
            }
          } else if (message.type === 'mutation_response' || message.type === 'mutation_success') {
            handleMutationResponse(userId, message);
          }
        } catch (error) {
          console.error(`[${userId}] Error processing command message:`, error);
        }
      });

      commandSocket.on('error', (error) => {
        console.error(`[${userId}] Command socket error:`, error);
      });

      commandSocket.on('close', (code, reason) => {
        console.log(`[${userId}] Command socket closed: ${code} ${reason}`);

        // Clean up user if their command socket closed
        if (userStores.has(userId)) {
          cleanupUser(userId);
        }
      });

      // We resolve immediately rather than waiting for the connection
      // Artillery will continue to the next step in the scenario
      resolve();
    } catch (error) {
      console.error(`[${userId}] Failed to setup WebSockets:`, error);
      cleanupUser(userId);
      reject(error);
    }
  });
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
      } else {
        // User was cleaned up before socket opened
        try { updateSocket.close(); } catch (err) {}
      }
    });

    updateSocket.on('message', (data) => {
      try {
        if (!userStores.has(userId)) return;

        const message = JSON.parse(data.toString());

        if (message.type === 'update_socket_confirm') {
          console.log(`[${userId}] Update socket confirmed, connection established`);
          userState.connected = true;

          // Clear the connection timeout as we're now fully connected
          if (userState.connectionTimeout) {
            clearTimeout(userState.connectionTimeout);
            userState.connectionTimeout = null;
          }
        } else if (message.type === 'sync') {
          if (message.data && message.data.entities) {
            const entityCount = message.data.entities.length;

            // Process entities
            message.data.entities.forEach(entity => {
              if (entity._id) {
                // Skip if we have a pending mutation for this entity
                if (userState.pendingMutations[entity._id]) {
                  return;
                }

                userState.entities[entity._id] = {
                  id: entity._id,
                  version: entity.version,
                  state: entity.state,
                  type: entity.type
                };

                // Track links between entities
                trackEntityRelationships(userId, entity);
              }
            });

            userState.stats.entityUpdatesReceived += entityCount;
          }
        } else if (message.update && message.update.entities) {
          const entityCount = message.update.entities.length;

          // Process entity updates
          message.update.entities.forEach(entity => {
            const entityId = entity._id || entity.id;
            if (entityId) {
              // Skip if we have a pending mutation for this entity
              if (userState.pendingMutations[entityId]) {
                return;
              }

              userState.entities[entityId] = {
                id: entityId,
                version: entity.version,
                state: entity.state,
                type: entity.type
              };

              // Track links between entities
              trackEntityRelationships(userId, entity);
            }
          });

          userState.stats.entityUpdatesReceived += entityCount;
        }
      } catch (error) {
        console.error(`[${userId}] Error processing update message:`, error);
      }
    });

    updateSocket.on('error', (error) => {
      console.error(`[${userId}] Update socket error:`, error);
    });

    updateSocket.on('close', (code, reason) => {
      console.log(`[${userId}] Update socket closed: ${code} ${reason}`);

      // Mark user as disconnected and update active connections count
      if (userStores.has(userId)) {
        userState.connected = false;
        globalStats.activeConnections--;

        // Since update socket closed, clean up the whole user
        cleanupUser(userId);
      }
    });
  } catch (error) {
    console.error(`[${userId}] Failed to connect update socket:`, error);
  }
}

/**
 * Track entity relationships for linked mutations
 */
function trackEntityRelationships(userId, entity) {
  if (!userStores.has(userId)) return;
  const userState = userStores.get(userId);

  const entityId = entity._id || entity.id;
  if (!entityId) return;

  // Initialize linked entities for this entity if not exist
  if (!userState.linkedEntities[entityId]) {
    userState.linkedEntities[entityId] = new Set();
  }

  // Track parent-child relationships
  if (entity.state) {
    // Add parent to linked entities
    if (entity.state.parentId) {
      userState.linkedEntities[entityId].add(entity.state.parentId);

      // Ensure the parent also links to this entity
      if (!userState.linkedEntities[entity.state.parentId]) {
        userState.linkedEntities[entity.state.parentId] = new Set();
      }
      userState.linkedEntities[entity.state.parentId].add(entityId);
    }

    // Add children to linked entities
    if (entity.state.childIds && Array.isArray(entity.state.childIds)) {
      entity.state.childIds.forEach(childId => {
        userState.linkedEntities[entityId].add(childId);

        // Ensure the child also links to this entity
        if (!userState.linkedEntities[childId]) {
          userState.linkedEntities[childId] = new Set();
        }
        userState.linkedEntities[childId].add(entityId);
      });
    }
  }
}

function assignUserType(requestParams, context, events) {
  return new Promise((resolve, reject) => {
    if (!context.vars) {
      context.vars = {};
    }

    const userId = context.vars.userId;

    if (!userStores.has(userId)) {
      console.error(`[${userId}] User not found for type assignment`);
      resolve(); // We resolve instead of rejecting to continue the test
      return;
    }

    const userState = userStores.get(userId);

    // Get probabilities from context or use defaults
    const lurkerChance = context.vars.lurkerChance || 40;
    const twitchyChance = context.vars.twitchyChance || 30;
    const heavyHandedChance = context.vars.heavyHandedChance || 20;
    const bigTickChance = context.vars.bigTickChance || 10;

    // Generate a random number for type selection
    const random = Math.random() * 100;
    let userType;

    if (random < lurkerChance) {
      userType = 'lurker';
    } else if (random < lurkerChance + twitchyChance) {
      userType = 'twitchy';
    } else if (random < lurkerChance + twitchyChance + heavyHandedChance) {
      userType = 'heavyHanded';
    } else {
      userType = 'bigTick';
    }

    // Assign the user type
    userState.userType = userType;
    context.vars.userType = userType;
    globalStats.userTypes[userType]++;

    console.log(`[${userId}] Assigned user type: ${userTypes[userType].name}`);

    resolve();
  });
}

/**
 * Check the status of connections
 */
function checkConnections(requestParams, context, events) {
  return new Promise((resolve) => {
    if (!context.vars) {
      context.vars = {};
    }

    const userId = context.vars.userId;

    if (!userStores.has(userId)) {
      console.error(`[${userId}] User not found for connection check`);
      resolve();
      return;
    }

    const userState = userStores.get(userId);

    console.log(`[${userId}] Connection status check:
      User type: ${userState.userType ? userTypes[userState.userType].name : 'Not assigned'}
      Command socket: ${userState.commandSocket ? 'Connected' : 'Not connected'}
      Connection ID: ${userState.connectionId || 'Not received'}
      Update socket: ${userState.updateSocket ? 'Connected' : 'Not connected'}
      Fully connected: ${userState.connected ? 'Yes' : 'No'}
      Entities: ${Object.keys(userState.entities).length}
    `);

    resolve();
  });
}

/**
 * Simulate user behavior based on assigned type
 */
function simulateUserBehavior(requestParams, context, events) {
  return new Promise((resolve) => {
    if (!context.vars) {
      context.vars = {};
    }

    const userId = context.vars.userId;

    if (!userStores.has(userId) || !userStores.get(userId).connected || !userStores.get(userId).userType) {
      console.log(`[${userId}] Not ready for simulation, skipping`);
      resolve();
      return;
    }

    const userState = userStores.get(userId);
    const type = userState.userType;
    const userProfile = userTypes[type];

    console.log(`[${userId}] Starting ${userProfile.name} behavior simulation`);

    // Set simulation running flag
    userState.simulationRunning = true;

    // Calculate session duration
    const sessionDurationSec = Math.floor(
      Math.random() * (userProfile.sessionDuration.max - userProfile.sessionDuration.min) +
      userProfile.sessionDuration.min
    );

    // Schedule the first mutation
    scheduleNextActivity(userId);

    // Note: We resolve immediately rather than waiting for the simulation to complete
    // This allows Artillery to continue with the scenario
    resolve();

    // Set timeout to end the session
    setTimeout(() => {
      if (userStores.has(userId)) {
        console.log(`[${userId}] Ending ${userProfile.name} behavior session after ${sessionDurationSec} seconds`);
        userState.simulationRunning = false;
      }
    }, sessionDurationSec * 1000);
  });
}

/**
 * Schedule the next activity for a user
 */
function scheduleNextActivity(userId) {
  if (!userStores.has(userId)) return;

  const userState = userStores.get(userId);
  if (!userState.simulationRunning) return;

  const userProfile = userTypes[userState.userType];

  // Determine next mutation interval based on user type
  let nextIntervalSec;

  if (userProfile.activityPattern === 'burst') {
    // Check if we're in a burst or rest period
    const timeSinceLastActivity = (Date.now() - userState.lastActivity) / 1000;

    if (timeSinceLastActivity > 120) {
      // Start a new burst after 2 minutes of inactivity
      nextIntervalSec = 0.5; // Start burst immediately
      console.log(`[${userId}] ${userProfile.name} starting a new burst`);
    } else if (timeSinceLastActivity < userProfile.sessionDuration.min) {
      // During burst period, use the normal interval
      nextIntervalSec = Math.random() *
        (userProfile.mutationInterval.max - userProfile.mutationInterval.min) +
        userProfile.mutationInterval.min;
    } else {
      // Rest period between bursts
      nextIntervalSec = 30 + Math.random() * 60; // 30-90 second rest
      console.log(`[${userId}] ${userProfile.name} resting between bursts`);
    }
  } else {
    // Constant pattern - use normal interval
    nextIntervalSec = Math.random() *
      (userProfile.mutationInterval.max - userProfile.mutationInterval.min) +
      userProfile.mutationInterval.min;
  }

  // Convert to milliseconds and schedule
  const nextIntervalMs = nextIntervalSec * 1000;
  userState.nextMutationTime = Date.now() + nextIntervalMs;

  setTimeout(() => {
    if (userStores.has(userId) && userState.simulationRunning) {
      performUserAction(userId);
      scheduleNextActivity(userId);
    }
  }, nextIntervalMs);
}

/**
 * Perform a user action based on their type
 */
function performUserAction(userId) {
  if (!userStores.has(userId)) return;

  const userState = userStores.get(userId);
  if (!userState.simulationRunning) return;

  // Log all entities first
  const allEntities = Object.values(userState.entities);
  console.log(`[${userId}] Total entities in store: ${allEntities.length}`);

  if (allEntities.length > 0) {
    // Log 1-2 sample entities to check their structure
    const sampleEntities = allEntities.slice(0, Math.min(2, allEntities.length));
    console.log(`[${userId}] Entity sample:`, JSON.stringify(sampleEntities));
  }

  // Now apply the filter and see what happens
  const nodeEntities = allEntities.filter(entity =>
    (entity.state && entity.state.label) ||
    (entity.type === 'Node') ||
    (entity.type && typeof entity.type === 'string' && entity.type.toLowerCase().includes('node')) ||
    (entity.state && entity.state.color)
  );

  console.log(`[${userId}] Filtered down to ${nodeEntities.length} node entities`);

  if (nodeEntities.length === 0) {
    // Debug: Check each filter condition separately to see which ones fail
    const hasLabel = allEntities.filter(e => e.state && e.state.label).length;
    const isNodeType = allEntities.filter(e => e.type === 'Node').length;
    const includesNodeInType = allEntities.filter(e => e.type && typeof e.type === 'string' && e.type.toLowerCase().includes('node')).length;
    const hasColor = allEntities.filter(e => e.state && e.state.color).length;

    console.log(`[${userId}] Filter breakdown - entities with:`);
    console.log(`  - state.label: ${hasLabel}`);
    console.log(`  - type === 'Node': ${isNodeType}`);
    console.log(`  - 'node' in type: ${includesNodeInType}`);
    console.log(`  - state.color: ${hasColor}`);

    return;  // Exit early if no entities found
  }

  // Determine how many entities to mutate
  const entityCount = Math.floor(
    Math.random() *
    (userProfile.entitiesPerMutation.max - userProfile.entitiesPerMutation.min + 1) +
    userProfile.entitiesPerMutation.min
  );

  // Tracking successfully sent mutations
  let mutationsSent = 0;

  // If the user type prefers linked entities, try to find a group
  if (userProfile.entitiesPerMutation.max > 1) {
    // First, pick a random starting entity
    const startingIndex = Math.floor(Math.random() * nodeEntities.length);
    const startingEntity = nodeEntities[startingIndex];

    // Get linked entities
    const targetEntities = getLinkedEntities(userId, startingEntity.id, entityCount);

    if (targetEntities.length > 0) {
      console.log(`[${userId}] ${userProfile.name} mutating ${targetEntities.length} linked entities`);

      // Mutate each entity in the linked group
      targetEntities.forEach(entityId => {
        if (userState.entities[entityId]) {
          const success = mutateSingleEntity(userId, userState.entities[entityId], userProfile.colorPreference);
          if (success) mutationsSent++;
        }
      });
    } else {
      // If no linked entities, just pick random ones
      console.log(`[${userId}] ${userProfile.name} couldn't find linked entities, using random selection`);
      mutationsSent = mutateRandomEntities(userId, nodeEntities, entityCount, userProfile.colorPreference);
    }
  } else {
    // For users that mutate single entities (like lurkers), just pick random ones
    mutationsSent = mutateRandomEntities(userId, nodeEntities, entityCount, userProfile.colorPreference);
  }

  userState.stats.mutationsAttempted += entityCount;
  globalStats.totalMutations += mutationsSent;
}

/**
 * Get a set of linked entities for group mutations
 */
function getLinkedEntities(userId, startEntityId, maxCount) {
  if (!userStores.has(userId)) return [];

  const userState = userStores.get(userId);
  if (!userState.linkedEntities[startEntityId]) return [];

  // Use breadth-first search to find connected entities
  const result = [startEntityId];
  const visited = new Set([startEntityId]);
  const queue = [startEntityId];

  while (queue.length > 0 && result.length < maxCount) {
    const currentId = queue.shift();

    // Get immediate neighbors
    const neighbors = userState.linkedEntities[currentId] || new Set();

    for (const neighborId of neighbors) {
      if (!visited.has(neighborId)) {
        visited.add(neighborId);
        result.push(neighborId);
        queue.push(neighborId);

        if (result.length >= maxCount) break;
      }
    }
  }

  return result;
}

/**
 * Mutate a set of random entities
 */
function mutateRandomEntities(userId, nodeEntities, count, colorPreference) {
  if (!userStores.has(userId)) return 0;

  count = Math.min(count, nodeEntities.length);
  let mutationsSent = 0;

  // Create a copy of the array to avoid modifying the original
  const availableEntities = [...nodeEntities];

  // Randomly select and mutate entities
  for (let i = 0; i < count; i++) {
    if (availableEntities.length === 0) break;

    // Pick a random index
    const index = Math.floor(Math.random() * availableEntities.length);

    // Get the entity and remove it from the available list
    const entity = availableEntities.splice(index, 1)[0];

    // Mutate the entity
    const success = mutateSingleEntity(userId, entity, colorPreference);
    if (success) mutationsSent++;
  }

  return mutationsSent;
}

/**
 * Mutate a single entity with the appropriate color preference
 */
function mutateSingleEntity(userId, entity, colorPreference) {
  if (!userStores.has(userId)) return false;

  const userState = userStores.get(userId);
  if (!userState.commandSocket) return false;

  try {
    // Generate a new color based on preference
    const currentColor = entity.state?.color || '#CCCCCC';
    let newColor;

    switch (colorPreference) {
      case 'increment':
        newColor = incrementColor(currentColor);
        break;
      case 'green':
        newColor = generateColorInHue(120, 50); // Green hue
        break;
      case 'blue':
        newColor = generateColorInHue(240, 50); // Blue hue
        break;
      case 'random':
      default:
        newColor = generateRandomColor();
        break;
    }

    // Prepare mutation command
    const command = {
      command: 'mutate',
      entity: {
        _id: entity.id,
        version: entity.version,
        state: {
          color: newColor
        }
      }
    };

    // Track this pending mutation
    userState.pendingMutations[entity.id] = {
      timestamp: Date.now(),
      oldVersion: entity.version,
      newColor: newColor
    };

    // Update statistics
    userState.stats.lastMutationTime = Date.now();

    // Send the command
    userState.commandSocket.send(JSON.stringify(command));

    return true;
  } catch (error) {
    console.error(`[${userId}] Error sending mutation for entity ${entity.id}:`, error);
    globalStats.failedMutations++;
    return false;
  }
}

/**
 * Increment a color slightly (used by lurkers)
 */
function incrementColor(baseColor = '#CCCCCC') {
  // Parse the base color
  let r = parseInt(baseColor.slice(1, 3), 16);
  let g = parseInt(baseColor.slice(3, 5), 16);
  let b = parseInt(baseColor.slice(5, 7), 16);

  // Apply small increments (max 10 units)
  const increment = Math.floor(Math.random() * 10) + 5;

  // Choose a random component to increment
  const component = Math.floor(Math.random() * 3);

  if (component === 0) {
    r = Math.min(255, r + increment);
  } else if (component === 1) {
    g = Math.min(255, g + increment);
  } else {
    b = Math.min(255, b + increment);
  }

  // Convert back to hex
  return `#${r.toString(16).padStart(2, '0')}${g.toString(16).padStart(2, '0')}${b.toString(16).padStart(2, '0')}`;
}

/**
 * Generate a color in a specific hue range
 */
function generateColorInHue(hue, saturation) {
  // Use HSL color model for easier hue control
  // hue: 0-360, saturation: 0-100, lightness: 0-100
  const h = (hue + Math.random() * 60 - 30) % 360; // Hue with ±30 variation
  const s = saturation + Math.random() * 30; // Saturation 50-80%
  const l = 30 + Math.random() * 40; // Lightness 30-70%

  // Convert HSL to RGB
  const c = (1 - Math.abs(2 * l / 100 - 1)) * s / 100;
  const x = c * (1 - Math.abs((h / 60) % 2 - 1));
  const m = l / 100 - c / 2;

  let r, g, b;
  if (h < 60) {
    [r, g, b] = [c, x, 0];
  } else if (h < 120) {
    [r, g, b] = [x, c, 0];
  } else if (h < 180) {
    [r, g, b] = [0, c, x];
  } else if (h < 240) {
    [r, g, b] = [0, x, c];
  } else if (h < 300) {
    [r, g, b] = [x, 0, c];
  } else {
    [r, g, b] = [c, 0, x];
  }

  // Convert back to RGB
  const red = Math.round((r + m) * 255);
  const green = Math.round((g + m) * 255);
  const blue = Math.round((b + m) * 255);

  // Convert to hex
  return `#${red.toString(16).padStart(2, '0')}${green.toString(16).padStart(2, '0')}${blue.toString(16).padStart(2, '0')}`;
}

/**
 * Generate a completely random color
 */
function generateRandomColor() {
  const r = Math.floor(Math.random() * 256);
  const g = Math.floor(Math.random() * 256);
  const b = Math.floor(Math.random() * 256);

  return `#${r.toString(16).padStart(2, '0')}${g.toString(16).padStart(2, '0')}${b.toString(16).padStart(2, '0')}`;
}

/**
 * Log metrics at regular intervals
 */
function logMetrics() {
  console.log('===== METRICS REPORT =====');
  console.log('Time:', new Date().toISOString());
  console.log('\nGlobal Statistics:');
  console.log(`  Total Connections: ${globalStats.totalConnections}`);
  console.log(`  Active Connections: ${globalStats.activeConnections}`);
  console.log(`  Total Mutations: ${globalStats.totalMutations}`);
  console.log(`  Successful Mutations: ${globalStats.successfulMutations}`);
  console.log(`  Failed Mutations: ${globalStats.failedMutations}`);

  console.log('\nUser Type Distribution:');
  Object.keys(globalStats.userTypes).forEach(type => {
    console.log(`  ${userTypes[type].name}: ${globalStats.userTypes[type]}`);
  });

  console.log('\nUser Type Performance:');
  Object.keys(metricsByUserType).forEach(type => {
    const metrics = metricsByUserType[type];
    if (metrics.mutations === 0) return;

    const avgResponseTime = metrics.responseTime.length > 0
      ? metrics.responseTime.reduce((a, b) => a + b, 0) / metrics.responseTime.length
      : 0;

    const successRate = metrics.successRate.length > 0
      ? (metrics.successRate.reduce((a, b) => a + b, 0) / metrics.successRate.length * 100)
      : 0;

    console.log(`  ${userTypes[type].name}:`);
    console.log(`    Mutations: ${metrics.mutations}`);
    console.log(`    Avg Response Time: ${avgResponseTime.toFixed(2)}ms`);
    console.log(`    Success Rate: ${successRate.toFixed(2)}%`);
  });

  console.log('\nActive Users:', userStores.size);
  console.log('========================\n');
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

  // Update user-specific statistics
  userState.stats.mutationsSucceeded++;
  userState.stats.entitiesMutated++;

  // Update global statistics
  globalStats.successfulMutations++;

  // Update metrics by user type
  if (userState.userType) {
    metricsByUserType[userState.userType].mutations++;
    metricsByUserType[userState.userType].responseTime.push(latency);

    if (message.type === 'mutation_success') {
      metricsByUserType[userState.userType].successRate.push(1);
    } else {
      metricsByUserType[userState.userType].successRate.push(0);
    }
  }

  // Remove from pending mutations
  delete userState.pendingMutations[entityId];
}

/**
 * Report statistics from the simulation
 */
function reportStatistics(requestParams, context, events) {
  return new Promise((resolve) => {
    if (!context.vars) {
      context.vars = {};
    }

    const userId = context.vars.userId;

    if (!userStores.has(userId)) {
      console.error(`[${userId}] User not found for statistics reporting`);
      resolve();
      return;
    }

    const userState = userStores.get(userId);
    console.log(`[${userId}] Reporting statistics`);
    console.log(`  User type: ${userState.userType ? userTypes[userState.userType].name : 'Not assigned'}`);
    console.log(`  Mutations attempted: ${userState.stats.mutationsAttempted}`);
    console.log(`  Mutations succeeded: ${userState.stats.mutationsSucceeded}`);
    console.log(`  Entity updates received: ${userState.stats.entityUpdatesReceived}`);
    console.log(`  Entities mutated: ${userState.stats.entitiesMutated || 0}`);

    resolve();
  });
}

/**
 * Clean up user connections
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
    globalStats.activeConnections--;
  }

  // Remove from store to free memory
  userStores.delete(userId);

  console.log(`[${userId}] User cleaned up, current active users: ${userStores.size}`);
}

/**
 * Clean up all connections for the current user
 */
function cleanupConnections(requestParams, context, events) {
  return new Promise((resolve) => {
    if (!context.vars) {
      context.vars = {};
    }

    const userId = context.vars.userId;

    if (userId && userStores.has(userId)) {
      console.log(`[${userId}] Cleaning up connections at test end`);
      cleanupUser(userId);
    }

    resolve();
  });
}

module.exports = {
  setupWebSockets,
  assignUserType,
  checkConnections,
  simulateUserBehavior,
  reportStatistics,
  cleanupConnections
};