/**
 * WebSocket message handlers
 */
import store from './store.js';
import logger from './logger.js';
import { connectUpdateSocket } from './connection.js';
import config from './config.js';

/**
 * Handle messages from the command socket
 * @param {MessageEvent} event - WebSocket message event
 */
export const handleCommandSocketMessage = (event) => {
  try {
    const message = JSON.parse(event.data);

    // Only log detailed message content in verbose mode to reduce noise
    if (config.logging?.verbose) {
      logger.command(`Received command message: ${JSON.stringify(message)}`);
    } else {
      logger.command(`Received command message type: ${message.type}`);
    }

    if (message.type === 'connection_confirm') {
      // Store connection ID
      store.setConnectionId(message.connectionId);
      logger.info(`Connection established with ID: ${message.connectionId}`);

      // Connect the update socket now that we have a connection ID
      connectUpdateSocket();
    } else if (message.type === 'initial_sync_complete') {
      logger.info('Initial sync complete');
    } else if (message.type === 'sync') {
      // Process entity updates from sync message
      if (message.data && message.data.entities) {
        logger.info(`Processing ${message.data.entities.length} entity updates from sync`);

        // Transform the entity data to match our expected format
        const transformedEntities = message.data.entities.map(entity => ({
          id: entity._id,
          version: entity.version,
          state: entity.state,
          type: entity.type
        }));

        store.updateEntities(transformedEntities);
      }
    } else if (message.type === 'mutation_response' || message.type === 'mutation_success') {
      // Process mutation response
      logger.info(`Received mutation response for entity: ${message.entity?._id}`);

      // Transform the entity data to our format
      if (message.entity) {
        const entityData = {
          id: message.entity._id,
          version: message.entity.version,
          state: message.entity.state,
          type: message.entity.type
        };

        // Update our store with the new version
        store.updateEntities([entityData]);
      }
    } else if (message.type === 'pong' || message.type === 'ping_response') {
      // Handle ping responses
      logger.info(`Received ping response: ${message.timestamp ? `latency=${Date.now() - message.timestamp}ms` : 'no timestamp'}`);
    } else {
      logger.info(`Unhandled command message type: ${message.type}`);
    }
  } catch (error) {
    logger.error(`Error processing command message: ${error.message}`);
  }
};

/**
 * Handle messages from the update socket
 * @param {MessageEvent} event - WebSocket message event
 */
export const handleUpdateSocketMessage = (event) => {
  try {
    const message = JSON.parse(event.data);

    // Only log detailed message content in verbose mode to reduce noise
    if (config.logging?.verbose) {
      logger.update(`Received update message: ${JSON.stringify(message)}`);
    } else {
      // Just log the type and entity count for normal mode
      let entityCount = 0;
      if (message.data && message.data.entities) {
        entityCount = message.data.entities.length;
      } else if (message.update && message.update.entities) {
        entityCount = message.update.entities.length;
      }

      if (entityCount > 0) {
        logger.update(`Received update message type: ${message.type} with ${entityCount} entities`);
      } else {
        logger.update(`Received update message type: ${message.type}`);
      }
    }

    if (message.type === 'update_socket_confirm') {
      // Update socket confirmed, we're fully connected
      store.setConnected(true);
      logger.info('Fully connected to server');
    } else if (message.type === 'sync') {
      // Process entity updates from sync message
      if (message.data && message.data.entities) {
        logger.info(`Processing ${message.data.entities.length} entity updates from update socket`);

        // Only log detailed entity data if configured
        if (config.logging?.logDetailedEntities && config.logging?.verbose) {
          console.log('Update socket sync example entity:', JSON.stringify(message.data.entities[0]));
        }

        // Transform the entity data to match our expected format
        const transformedEntities = message.data.entities.map(entity => ({
          id: entity._id,
          version: entity.version,
          state: entity.state,
          type: entity.type
        }));

        store.updateEntities(transformedEntities);
      }
    } else if (message.update && message.update.entities) {
      // Handle legacy format
      logger.info(`Processing ${message.update.entities.length} entity updates from update message`);

      // Only log detailed entity data if configured
      if (config.logging?.logDetailedEntities && config.logging?.verbose) {
        console.log('Legacy format entity example:', JSON.stringify(message.update.entities[0]));
      }

      // Check if these entities need transformation
      const needsTransform = message.update.entities[0] && message.update.entities[0]._id && !message.update.entities[0].id;

      if (needsTransform) {
        const transformedEntities = message.update.entities.map(entity => ({
          id: entity._id,
          version: entity.version,
          state: entity.state,
          type: entity.type
        }));
        store.updateEntities(transformedEntities);
      } else {
        store.updateEntities(message.update.entities);
      }
    } else {
      logger.info(`Unhandled update message type: ${message.type}`);
    }
  } catch (error) {
    logger.error(`Error processing update message: ${error.message}`);
  }
};

export default {
  handleCommandSocketMessage,
  handleUpdateSocketMessage
};