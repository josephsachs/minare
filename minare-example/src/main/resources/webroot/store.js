/**
 * State management for the Minare client
 * Implements a simple store pattern
 */

// Import event emitter functionality
import createEventEmitter from './events.js';

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
    }
  };

  // Create event emitter
  const events = createEventEmitter();

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

    updateEntities: (entities) => {
      console.log('Store updating entities:', JSON.stringify(entities));

      for (const entity of entities) {
        if (entity.id) {
          _state.entities[entity.id] = {
            id: entity.id,
            version: entity.version,
            state: entity.state,
            type: entity.type  // Make sure we're storing the type property
          };
        }
      }

      const allEntities = Object.values(_state.entities);
      console.log(`Store now has ${allEntities.length} entities`);

      // Debug: Check if any entities would be recognized as nodes
      const potentialNodes = allEntities.filter(entity =>
        (entity.state && entity.state.label) ||
        (entity.type === 'Node')
      );
      console.log(`Of these, ${potentialNodes.length} would be recognized as nodes`);

      events.emit('entities.updated', allEntities);
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

    // Simulation methods
    getLurkers: () => _state.simulation.lurkers,
    addLurker: (lurker) => {
      _state.simulation.lurkers.push(lurker);
      events.emit('simulation.lurkers.updated', _state.simulation.lurkers);
      return _state.simulation.lurkers.length - 1;  // Return index
    },

    removeLurker: (index) => {
      if (index >= 0 && index < _state.simulation.lurkers.length) {
        _state.simulation.lurkers.splice(index, 1);
        events.emit('simulation.lurkers.updated', _state.simulation.lurkers);
        return true;
      }
      return false;
    },

    // Event subscription
    on: events.on,
    off: events.off
  };
};

// Create and export a singleton store instance
const store = createStore();

export default store;