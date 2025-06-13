/**
 * State management for the Minare client
 * Implements a simple store pattern
 * Optimized for high-frequency updates
 * Enhanced with activity tracking and delta merging
 */

import createEventEmitter from './events.js';
import config from './config.js';
import logger from './logger.js';

const createStore = () => {
  const _state = {
    connection: {
      id: null,
      connected: false,
      commandSocket: null,
      updateSocket: null,
      lastActivity: Date.now()
    },

    entities: {},

    visualization: {
      type: 'grid',  // 'grid' or 'd3'
      instance: null
    },

    simulation: {
      lurkers: []
    },

    performance: {
      lastUpdateTime: 0,
      updateCount: 0,
      throttled: true
    }
  };

  const events = createEventEmitter();

  let updateThrottleTimer = null;
  const UPDATE_THROTTLE_INTERVAL = 150;

  return {
    getState: () => ({..._state}),

    getConnectionId: () => _state.connection.id,
    setConnectionId: (id) => {
      _state.connection.id = id;
      _state.connection.lastActivity = Date.now();
      events.emit('connection.id.changed', id);
    },

    isConnected: () => _state.connection.connected,
    setConnected: (connected) => {
      _state.connection.connected = connected;
      _state.connection.lastActivity = Date.now();
      events.emit('connection.status.changed', connected);
    },

    getCommandSocket: () => _state.connection.commandSocket,
    setCommandSocket: (socket) => {
      _state.connection.commandSocket = socket;
      _state.connection.lastActivity = Date.now();
    },

    getUpdateSocket: () => _state.connection.updateSocket,
    setUpdateSocket: (socket) => {
      _state.connection.updateSocket = socket;
      _state.connection.lastActivity = Date.now();
    },

    getLastActivity: () => _state.connection.lastActivity,
    updateLastActivity: () => {
      _state.connection.lastActivity = Date.now();
    },

    isConnectionStale: () => {
      const now = Date.now();
      const staleThreshold = 40000;
      return (now - _state.connection.lastActivity) > staleThreshold;
    },

    getEntities: () => Object.values(_state.entities),
    getEntityById: (id) => _state.entities[id],

    /**
     * Merge entity delta with existing state
     * @param {string} entityId - ID of the entity to update
     * @param {Object} delta - Delta object containing changed fields
     * @param {number} version - New version number
     * @param {string} type - Entity type
     * @returns {boolean} Whether the entity was updated
     */
    mergeEntityDelta: (entityId, delta, version, type) => {
      if (!entityId || !delta || version == null) {
        if (config.logging?.verbose) {
          logger.error(`Invalid delta merge parameters: entityId=${entityId}, delta=${!!delta}, version=${version}`);
        }
        return false;
      }

      const existing = _state.entities[entityId];
      let wasUpdated = false;

      if (!existing) {
        // Entity doesn't exist - create new with delta as initial state
        _state.entities[entityId] = {
          id: entityId,
          version: version,
          state: { ...delta },
          type: type
        };
        wasUpdated = true;

        if (config.logging?.verbose) {
          logger.info(`Created new entity ${entityId} from delta with ${Object.keys(delta).join(', ')}`);
        }
      } else if (existing.version !== version) {
        // Entity exists but version changed - merge delta with existing state
        const mergedState = { ...existing.state };

        // Apply delta changes
        Object.keys(delta).forEach(key => {
          mergedState[key] = delta[key];
        });

        _state.entities[entityId] = {
          id: entityId,
          version: version,
          state: mergedState,
          type: type || existing.type
        };
        wasUpdated = true;

        if (config.logging?.verbose) {
          logger.info(`Merged delta for entity ${entityId}: ${Object.keys(delta).join(', ')} (v${existing.version} -> v${version})`);
        }
      } else {
        // Same version - no change needed
        if (config.logging?.verbose) {
          logger.info(`Skipped delta merge for entity ${entityId}: same version ${version}`);
        }
      }

      if (wasUpdated) {
        // Track update metrics
        _state.performance.updateCount++;
        _state.performance.lastUpdateTime = Date.now();

        // Emit update event for this specific entity change
        events.emit('entities.updated', Object.values(_state.entities));
      }

      return wasUpdated;
    },

    /**
     * Update a single entity with full state
     * @param {Object} entity - Complete entity object
     * @returns {boolean} Whether the entity was updated
     */
    updateEntity: (entity) => {
      if (!entity || !entity.id) {
        if (config.logging?.verbose) {
          logger.error(`Invalid entity update: missing id`);
        }
        return false;
      }

      const existing = _state.entities[entity.id];
      let wasUpdated = false;

      if (!existing) {
        // New entity
        _state.entities[entity.id] = {
          id: entity.id,
          version: entity.version,
          state: entity.state,
          type: entity.type
        };
        wasUpdated = true;

        if (config.logging?.verbose) {
          logger.info(`Added new entity ${entity.id} v${entity.version}`);
        }
      } else if (existing.version !== entity.version) {
        // Entity exists but version changed - full state update
        _state.entities[entity.id] = {
          id: entity.id,
          version: entity.version,
          state: entity.state,
          type: entity.type
        };
        wasUpdated = true;

        if (config.logging?.verbose) {
          logger.info(`Updated entity ${entity.id} (v${existing.version} -> v${entity.version})`);
        }
      }

      if (wasUpdated) {
        // Track update metrics
        _state.performance.updateCount++;
        _state.performance.lastUpdateTime = Date.now();

        // Emit update event
        events.emit('entities.updated', Object.values(_state.entities));
      }

      return wasUpdated;
    },

    /**
     * Update entities with optimized processing and throttled events
     * @param {Array} entities - Array of entity objects to update
     */
    updateEntities: (entities) => {
      if (!entities || entities.length === 0) return;

      const startTime = performance.now();
      let changed = 0;
      let unchanged = 0;
      let added = 0;

      for (const entity of entities) {
        if (!entity.id) {
          continue;
        }

        const existing = _state.entities[entity.id];
        if (!existing) {
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

    // Event subscription
    on: events.on,
    off: events.off
  };
};

// Create and export a singleton store instance
const store = createStore();

export default store;