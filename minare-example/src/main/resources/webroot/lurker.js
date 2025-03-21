/**
 * Lurker simulated user with independent WebSocket connections
 *
 * This simulated user maintains its own WebSocket connections to the server
 * and selects random nodes to mutate their colors at intervals of 1-5 seconds.
 */
import config from './config.js';
import logger from './logger.js';

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

    // Connection state
    this.connectionId = null;
    this.commandSocket = null;
    this.updateSocket = null;
    this.connected = false;

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

    // Configuration
    this.minInterval = config.simulation?.lurker?.minMutationInterval || 60000;
    this.maxInterval = config.simulation?.lurker?.maxMutationInterval || 120000;

    logger.info(`Created lurker: ${this.id}`);
  }

  /**
   * Connect to the server with independent WebSocket connections
   * @returns {Promise<boolean>} Success indicator
   */
  async connect() {
    if (this.connected) return true;

    try {
      // Create command socket
      const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const wsBaseUrl = `${wsProtocol}//${window.location.host}`;

      // Connect the command socket
      this.commandSocket = new WebSocket(`${wsBaseUrl}/ws`);

      // Wait for connection to establish and receive connection ID
      await new Promise((resolve, reject) => {
        this.commandSocket.onopen = () => {
          logger.info(`Lurker ${this.id}: Command socket connected`);
        };

        this.commandSocket.onmessage = (event) => {
          try {
            const message = JSON.parse(event.data);
            logger.info(`Lurker ${this.id}: Command socket received: ${JSON.stringify(message)}`);

            if (message.type === 'connection_confirm') {
              this.connectionId = message.connectionId;
              logger.info(`Lurker ${this.id}: Connection established with ID: ${this.connectionId}`);
              resolve();
            } else if (message.type === 'sync') {
              // Handle initial sync data
              if (message.data && message.data.entities) {
                logger.info(`Lurker ${this.id}: Received initial sync with ${message.data.entities.length} entities`);

                // Transform the data to match our entity store format
                const entities = message.data.entities.map(entity => ({
                  id: entity._id,
                  version: entity.version,
                  type: entity.type,
                  state: entity.state
                }));

                // Update our entity store
                this.entityStore.updateEntities(entities);
              }
            } else if (message.type === 'initial_sync_complete') {
              logger.info(`Lurker ${this.id}: Initial sync complete`);
            }
          } catch (error) {
            reject(error);
          }
        };

        this.commandSocket.onerror = (error) => {
          logger.error(`Lurker ${this.id}: Command socket error: ${error}`);
          reject(error);
        };

        this.commandSocket.onclose = () => {
          logger.info(`Lurker ${this.id}: Command socket closed`);
          if (this.connected) this.disconnect();
        };
      });

      // Connect the update socket using the connection ID
      this.updateSocket = new WebSocket(`${wsBaseUrl}/ws/updates`);

      await new Promise((resolve, reject) => {
        this.updateSocket.onopen = () => {
          logger.info(`Lurker ${this.id}: Update socket connected`);

          // Send connection ID to associate the update socket
          const associationMessage = {
            connectionId: this.connectionId
          };
          this.updateSocket.send(JSON.stringify(associationMessage));
        };

        this.updateSocket.onmessage = (event) => {
          try {
            const message = JSON.parse(event.data);
            logger.info(`Lurker ${this.id}: Update socket received: ${JSON.stringify(message)}`);

            if (message.type === 'update_socket_confirm') {
              logger.info(`Lurker ${this.id}: Update socket confirmed`);
              this.connected = true;
              resolve();
            } else if (message.type === 'sync') {
              // Handle sync data from update socket
              if (message.data && message.data.entities) {
                logger.info(`Lurker ${this.id}: Received sync with ${message.data.entities.length} entities`);

                // Transform the data to match our entity store format
                const entities = message.data.entities.map(entity => ({
                  id: entity._id,
                  version: entity.version,
                  type: entity.type,
                  state: entity.state
                }));

                // Update our entity store
                this.entityStore.updateEntities(entities);
              }
            } else if (message.update && message.update.entities) {
              // Process incremental entity updates
              logger.info(`Lurker ${this.id}: Received update with ${message.update.entities.length} entities`);
              this.handleEntityUpdates(message.update.entities);
            }
          } catch (error) {
            logger.error(`Lurker ${this.id}: Error processing update message: ${error.message}`);
          }
        };

        this.updateSocket.onerror = (error) => {
          logger.error(`Lurker ${this.id}: Update socket error: ${error}`);
          reject(error);
        };

        this.updateSocket.onclose = () => {
          logger.info(`Lurker ${this.id}: Update socket closed`);
          if (this.connected) this.disconnect();
        };
      });

      return true;
    } catch (error) {
      logger.error(`Lurker ${this.id}: Connection failed: ${error.message}`);
      this.disconnect(); // Clean up any partial connections
      return false;
    }
  }

  /**
   * Disconnect from the server
   */
  disconnect() {
    if (this.updateSocket) {
      this.updateSocket.close();
      this.updateSocket = null;
    }

    if (this.commandSocket) {
      this.commandSocket.close();
      this.commandSocket = null;
    }

    this.connectionId = null;
    this.connected = false;

    logger.info(`Lurker ${this.id}: Disconnected from server`);
  }

  /**
   * Handle entity updates from the server
   * @param {Array} entities - Updated entities
   */
  handleEntityUpdates(entities) {
    const updatedEntities = [];

    for (const entity of entities) {
      // Transform entity if needed (the format might be different from our internal format)
      const transformedEntity = {
        id: entity.id || entity._id,
        version: entity.version,
        type: entity.type,
        state: entity.state
      };

      // Skip entities we have pending mutations for to avoid race conditions
      if (this.pendingMutations.has(transformedEntity.id)) {
        continue;
      }

      updatedEntities.push(transformedEntity);
    }

    if (updatedEntities.length > 0) {
      logger.info(`Lurker ${this.id}: Updating ${updatedEntities.length} entities in local store`);
      this.entityStore.updateEntities(updatedEntities);
    }
  }

  /**
   * Start the lurker
   * @returns {Promise<boolean>} Success indicator
   */
  async start() {
    if (this.active) return false;

    // Connect to the server first
    const connected = await this.connect();
    if (!connected) {
      logger.error(`Lurker ${this.id}: Cannot start because connection failed`);
      return false;
    }

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

    // Disconnect from the server
    this.disconnect();
  }

  /**
   * Schedule the next mutation
   */
  scheduleNextMutation() {
    if (!this.active) {
      logger.info(`Lurker ${this.id}: Not scheduling next mutation because lurker is not active`);
      return;
    }

    // Random interval between min and max
    const interval = Math.floor(
      Math.random() * (this.maxInterval - this.minInterval) + this.minInterval
    );

    logger.info(`Lurker ${this.id}: Scheduling next mutation in ${interval}ms`);

    // Clear any existing timer first to avoid duplicate timers
    if (this.timer) {
      clearTimeout(this.timer);
    }

    this.timer = setTimeout(() => {
      logger.info(`Lurker ${this.id}: Executing scheduled mutation`);
      this.performMutation();
    }, interval);
  }

  /**
     * Perform a random color mutation on a node
     */
    performMutation() {
      if (!this.active) {
        logger.info(`Lurker ${this.id}: Not active, skipping mutation`);
        return;
      }

      if (!this.connected) {
        logger.info(`Lurker ${this.id}: Not connected, skipping mutation`);
        // Even if we're not connected, we should schedule the next attempt
        this.scheduleNextMutation();
        return;
      }

      // Random chance to bail (75%)
      const shouldBail = (Math.random() * (100 - 1) + 1) < 75;

      if (shouldBail) {
        logger.info(`Lurker ${this.id}: Randomly decided to skip this mutation opportunity`);
        // Always schedule the next mutation even when bailing
        this.scheduleNextMutation();
        return;
      }

      try {
        // Get all nodes from our entity store
        const entities = this.entityStore.getEntities();
        logger.info(`Lurker ${this.id}: Selecting from ${entities.length} entities for mutation`);

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

        // Send the command using this lurker's own command socket
        const success = this.sendCommand(command);

        if (!success) {
          logger.error(`Lurker ${this.id}: Failed to send mutation command`);
          // Clean up the pending mutation since it failed
          this.pendingMutations.delete(targetNode.id);
        }
      } catch (error) {
        logger.error(`Lurker ${this.id}: Error performing mutation: ${error.message}`);
      } finally {
        // Always schedule the next mutation, even if this one failed
        this.scheduleNextMutation();
      }
    }

  /**
   * Send a command to the server
   * @param {Object} command - Command object
   * @returns {boolean} Success indicator
   */
  sendCommand(command) {
    if (!this.commandSocket || this.commandSocket.readyState !== WebSocket.OPEN) {
      logger.error(`Lurker ${this.id}: Cannot send command: Command socket not connected`);
      return false;
    }

    try {
      const message = JSON.stringify(command);
      this.commandSocket.send(message);
      logger.info(`Lurker ${this.id}: Sent mutation for node ${command.entity._id} color to ${command.entity.state.color}`);
      return true;
    } catch (error) {
      logger.error(`Lurker ${this.id}: Failed to send command: ${error.message}`);
      return false;
    }
  }

  /**
   * Handle mutation response from server
   * @param {Object} response - Mutation response
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
      connected: this.connected,
      connectionId: this.connectionId,
      targetCount: this.targetedNodes.size,
      entityCount: this.entityStore.getEntities().length,
      pendingMutations: this.pendingMutations.size
    };
  }
}

export default Lurker;