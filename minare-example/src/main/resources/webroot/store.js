/**
 * State management for the Minare client
 * Implements a simple store pattern
 * Optimized for high-frequency updates
 */

// Import event emitter functionality
import createEventEmitter from './events.js';
import config from './config.js';

// Create a state store with event emission
const createStore = () => {
  // Private state object
  const _state = {
    // Connection state
    connection: {
      id: null,
      connected: false,
      commandSocket: null,
      updateSocket: null
    },

    // Entities received from server
    entities: {},

    // Visualization state
    visualization: {
      type: 'grid',  // 'grid' or 'd3'
      instance: null
    },

    // Simulation state
    simulation: {
      lurkers: []
    },

    // Performance metrics
    performance: {
      lastUpdateTime: 0,
      updateCount: 0,
      throttled: true
    }
  };

  // Create event emitter
  const events = createEventEmitter();

  // Update emission throttling
  let updateThrottleTimer = null;
  const UPDATE_THROTTLE_INTERVAL = 150; // ms

  // Methods to interact with the store
  return {
    // Get entire state (for debugging)
    getState: () => ({..._state}),

    // Connection state methods
    getConnectionId: () => _state.connection.id,
    setConnectionId: (id) => {
      _state.connection.id = id;
      events.emit('connection.id.changed', id);
    },

    isConnected: () => _state.connection.connected,
    setConnected: (connected) => {
      _state.connection.connected = connected;
      events.emit('connection.status.changed', connected);
    },

    getCommandSocket: () => _state.connection.commandSocket,
    setCommandSocket: (socket) => {
      _state.connection.commandSocket = socket;
    },

    getUpdateSocket: () => _state.connection.updateSocket,
    setUpdateSocket: (socket) => {
      _state.connection.updateSocket = socket;
    },

    // Entity methods
    getEntities: () => Object.values(_state.entities),
    getEntityById: (id) => _state.entities[id],

    /**
     * Update entities with optimized processing and throttled events
     * @param {Array} entities - Array of entity objects to update
     */
    updateEntities: (entities) => {
      // Skip if no entities to update
      if (!entities || entities.length === 0) return;

      // Track changes for logging
      const startTime = performance.now();
      let changed = 0;
      let unchanged = 0;
      let added = 0;

      // Process entities efficiently
      for (const entity of entities) {
        if (!entity.id) {
          // Skip entities without proper ID
          continue;
        }

        const existing = _state.entities[entity.id];
        if (!existing) {
          // New entity
          _state.entities[entity.id] = {
            id: entity.id,
            version: entity.version,
            state: entity.state,
            type: entity.type
          };
          added++;
        } else if (existing.version !== entity.version) {
          // Entity exists but version changed - update it
          _state.entities[entity.id] = {
            id: entity.id,
            version: entity.version,
            state: entity.state,
            type: entity.type
          };
          changed++;
        } else {
          // Entity exists with same version - no change
          unchanged++;
        }
      }

      const endTime = performance.now();
      const processingTime = endTime - startTime;

      // Log only if significant changes or significant processing time
      if ((added > 0 || changed > 0) && (config.logging?.verbose || processingTime > 20)) {
        logger.info(`Entity processing: Added=${added}, Changed=${changed}, Time=${processingTime.toFixed(2)}ms`);
      }

      // Track update metrics
      _state.performance.updateCount++;
      _state.performance.lastUpdateTime = Date.now();

      // Always emit update event regardless of changes
      // This ensures visualization gets updated even if the logic determining
      // whether to update is incorrect
      events.emit('entities.updated', Object.values(_state.entities));
    },

    clearEntities: () => {
      _state.entities = {};
      events.emit('entities.updated', []);
    },

    // Visualization methods
    getVisualizationType: () => _state.visualization.type,
    setVisualizationType: (type) => {
      _state.visualization.type = type;
      events.emit('visualization.type.changed', type);
    },

    getVisualizationInstance: () => _state.visualization.instance,
    setVisualizationInstance: (instance) => {
      _state.visualization.instance = instance;
    },

    // Performance methods
    setUpdateThrottling: (enabled) => {
      _state.performance.throttled = enabled;
      return enabled;
    },

    getPerformanceMetrics: () => ({..._state.performance}),

    // Simulation methods
    //getLurkers: () => _state.simulation.lurkers,
    //addLurker: (lurker) => {
    //  _state.simulation.lurkers.push(lurker);
    //  events.emit('simulation.lurkers.updated', _state.simulation.lurkers);
    //  return _state.simulation.lurkers.length - 1;  // Return index
    //},

    //removeLurker: (index) => {
    //  if (index >= 0 && index < _state.simulation.lurkers.length) {
    //    _state.simulation.lurkers.splice(index, 1);
    //    events.emit('simulation.lurkers.updated', _state.simulation.lurkers);
    //    return true;
    //  }
    //  return false;
    //},

    // Event subscription
    on: events.on,
    off: events.off
  };
};

// Create and export a singleton store instance
const store = createStore();

export default store;