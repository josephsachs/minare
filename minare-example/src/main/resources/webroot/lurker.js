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

    // Configuration
    this.minInterval = config.simulation?.lurker?.minMutationInterval || 1000;
    this.maxInterval = config.simulation?.lurker?.maxMutationInterval || 5000;

    logger.info(`Created lurker: ${this.id}`);
  }

  /**
   * Start the lurker
   * @returns {boolean} Success indicator
   */
  start() {
    if (this.active) return false;

    this.active = true;
    logger.info(`Starting lurker: ${this.id}`);

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

    // Get all nodes from the store
    const entities = store.getEntities();

    // Filter to only include nodes (either from state.type or entity.type)
    const nodes = entities.filter(entity =>
      (entity.state && entity.state.type === 'Node') || entity.type === 'Node'
    );

    if (nodes.length === 0) {
      logger.info(`Lurker ${this.id}: No nodes available for mutation`);
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

    // Send the command
    const success = sendCommand(command);

    if (success) {
      logger.info(`Lurker ${this.id}: Mutated node ${targetNode.id} color to ${newColor}`);
    } else {
      logger.error(`Lurker ${this.id}: Failed to mutate node ${targetNode.id}`);
    }

    // Schedule the next mutation
    this.scheduleNextMutation();
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
      targetCount: this.targetedNodes.size
    };
  }
}

export default Lurker;