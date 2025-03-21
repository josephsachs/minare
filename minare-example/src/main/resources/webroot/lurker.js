/**
 * Lurker simulated user
 *
 * This simulated user selects a random node from the graph and mutates its color
 * at random intervals between 1-5 seconds.
 */
import config from './config.js';
import store from './store.js';
import logger from './logger.js';
import { sendCommand } from './connection.js';
import createEventEmitter from './events.js';

export class Lurker {
  /**
   * Create a lurker simulated user
   * @param {string} id - Unique identifier for this lurker
   */
  constructor(id) {
    this.id = id || `lurker-${Date.now()}`;
    this.active = false;
    this.timer = null;
    this.targetedNodes = new Set();
    this.pendingMutations = new Map();

    // The lurker's own view of the entity graph
    this.entityStore = {
      entities: {},
      getEntities: () => Object.values(this.entityStore.entities),
      getEntityById: (id) => this.entityStore.entities[id],
      updateEntity: (entity) => {
        if (entity.id) {
          this.entityStore.entities[entity.id] = { ...entity };
        }
      },
      updateEntities: (entities) => {
        for (const entity of entities) {
          if (entity.id) {
            this.entityStore.entities[entity.id] = { ...entity };
          }
        }
      }
    };

    // Event emitter for this lurker
    this.events = createEventEmitter();

    // Configuration
    this.minInterval = config.simulation?.lurker?.minMutationInterval || 1000;
    this.maxInterval = config.simulation?.lurker?.maxMutationInterval || 5000;

    // Initialize by copying the current entity graph from the main store
    this.syncWithMainStore();

    // Set up subscription to entity updates
    this.storeSubscription = store.on('entities.updated', this.handleEntityUpdates.bind(this));

    logger.info(`Created lurker: ${this.id}`);
  }

  /**
   * Initialize entity store from main store
   */
  syncWithMainStore() {
    const entities = store.getEntities();

    if (entities.length > 0) {
      logger.info(`Lurker ${this.id}: Syncing ${entities.length} entities from main store`);
      this.entityStore.updateEntities(entities);
    } else {
      logger.info(`Lurker ${this.id}: No entities available in main store for initial sync`);
    }
  }

  /**
   * Handle entity updates from the main store
   * @param {Array} entities - Updated entities
   */
  handleEntityUpdates(entities) {
    if (!this.active) return;

    const updatedEntities = [];

    for (const entity of entities) {
      // Skip entities we have pending mutations for to avoid race conditions
      if (this.pendingMutations.has(entity.id)) {
        continue;
      }

      updatedEntities.push(entity);
    }

    if (updatedEntities.length > 0) {
      logger.info(`Lurker ${this.id}: Updating ${updatedEntities.length} entities in local store`);
      this.entityStore.updateEntities(updatedEntities);
    }
  }

  /**
   * Start the lurker
   * @returns {boolean} Success indicator
   */
  start() {
    if (this.active) return false;

    this.active = true;
    logger.info(`Starting lurker: ${this.id}`);

    // Ensure we have the latest entity graph
    this.syncWithMainStore();

    // Schedule the first mutation
    this.scheduleNextMutation();

    return true;
  }

  /**
   * Stop the lurker
   */
  stop() {
    if (!this.active) return;

    this.active = false;
    logger.info(`Stopping lurker: ${this.id}`);

    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }

    // Clean up store subscription
    if (this.storeSubscription) {
      store.off('entities.updated', this.storeSubscription);
    }
  }

  /**
   * Schedule the next mutation
   */
  scheduleNextMutation() {
    if (!this.active) return;

    // Random interval between min and max
    const interval = Math.floor(
      Math.random() * (this.maxInterval - this.minInterval) + this.minInterval
    );

    this.timer = setTimeout(() => this.performMutation(), interval);
  }

  /**
   * Perform a random color mutation on a node
   */
  performMutation() {
    if (!this.active) return;

    // Get all nodes from our entity store
    const entities = this.entityStore.getEntities();

    // Filter based on the type Node either:
    // 1. Node entities have state.label field (that's our best indicator without type)
    // 2. If the type property exists and equals "Node"
    const nodes = entities.filter(entity =>
      (entity.state && entity.state.label) ||
      (entity.type === 'Node')
    );

    if (nodes.length === 0) {
      logger.info(`Lurker ${this.id}: No nodes available for mutation (entities: ${entities.length})`);
      this.scheduleNextMutation();
      return;
    }

    // Select a random node
    const randomIndex = Math.floor(Math.random() * nodes.length);
    const targetNode = nodes[randomIndex];

    // Keep track of nodes this lurker has interacted with
    this.targetedNodes.add(targetNode.id);

    // Generate a slightly modified color
    const currentColor = targetNode.state?.color || '#CCCCCC';
    const newColor = this.generateRandomColor(currentColor);

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
    this.pendingMutations.set(targetNode.id, {
      timestamp: Date.now(),
      oldVersion: targetNode.version,
      newColor: newColor
    });

    // Send the command
    const success = sendCommand(command);

    if (success) {
      logger.info(`Lurker ${this.id}: Sent mutation for node ${targetNode.id} color to ${newColor}`);
    } else {
      logger.error(`Lurker ${this.id}: Failed to send mutation for node ${targetNode.id}`);
      // Remove from pending mutations since it failed to send
      this.pendingMutations.delete(targetNode.id);
    }

    // Schedule the next mutation
    this.scheduleNextMutation();
  }

  /**
   * Handle mutation response
   * @param {Object} response - Mutation response from server
   */
  handleMutationResponse(response) {
    const entityId = response.entity?._id;

    if (!entityId || !this.pendingMutations.has(entityId)) {
      return;
    }

    const pendingMutation = this.pendingMutations.get(entityId);
    const newVersion = response.entity.version;

    // Update our local entity store with the new version
    const entity = this.entityStore.getEntityById(entityId);
    if (entity) {
      entity.version = newVersion;

      // Update state if it exists
      if (entity.state) {
        entity.state.color = pendingMutation.newColor;
      }

      this.entityStore.updateEntity(entity);
    }

    // Calculate latency
    const latency = Date.now() - pendingMutation.timestamp;
    logger.info(`Lurker ${this.id}: Mutation for node ${entityId} confirmed (latency: ${latency}ms)`);

    // Remove from pending mutations
    this.pendingMutations.delete(entityId);
  }

  /**
   * Generate a random color variation
   * @param {string} baseColor - Base color in hex format (#RRGGBB)
   * @returns {string} New color in hex format
   */
  generateRandomColor(baseColor = '#CCCCCC') {
    // Parse the base color
    let r = parseInt(baseColor.slice(1, 3), 16);
    let g = parseInt(baseColor.slice(3, 5), 16);
    let b = parseInt(baseColor.slice(5, 7), 16);

    // Apply small random changes to each component
    const variation = 30; // Maximum color component change

    r = Math.max(0, Math.min(255, r + Math.floor(Math.random() * variation * 2) - variation));
    g = Math.max(0, Math.min(255, g + Math.floor(Math.random() * variation * 2) - variation));
    b = Math.max(0, Math.min(255, b + Math.floor(Math.random() * variation * 2) - variation));

    // Convert back to hex
    return `#${r.toString(16).padStart(2, '0')}${g.toString(16).padStart(2, '0')}${b.toString(16).padStart(2, '0')}`;
  }

  /**
   * Get lurker status information
   * @returns {Object} Status object
   */
  getStatus() {
    return {
      id: this.id,
      active: this.active,
      targetCount: this.targetedNodes.size,
      entityCount: this.entityStore.getEntities().length,
      pendingMutations: this.pendingMutations.size
    };
  }
}

export default Lurker;