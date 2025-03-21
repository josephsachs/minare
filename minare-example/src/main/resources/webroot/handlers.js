/**
 * WebSocket message handlers
 */
import store from './store.js';
import logger from './logger.js';
import { connectUpdateSocket } from './connection.js';

/**
 * Handle messages from the command socket
 * @param {MessageEvent} event - WebSocket message event
 */
export const handleCommandSocketMessage = (event) => {
  try {
    const message = JSON.parse(event.data);
    logger.command(`Received command message: ${JSON.stringify(message)}`);

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
    logger.update(`Received update message: ${JSON.stringify(message)}`);

    if (message.type === 'update_socket_confirm') {
      // Update socket confirmed, we're fully connected
      store.setConnected(true);
      logger.info('Fully connected to server');
    } else if (message.type === 'sync') {
      // Process entity updates from sync message
      if (message.data && message.data.entities) {
        logger.info(`Processing ${message.data.entities.length} entity updates from update socket`);

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
      store.updateEntities(message.update.entities);
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