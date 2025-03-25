/**
 * WebSocket message handlers
 * Optimized for high-frequency updates
 * Enhanced with heartbeat support
 */
import store from './store.js';
import logger from './logger.js';
import { connectUpdateSocket } from './connection.js';
import config from './config.js';

// Global update queue with throttling
let pendingEntities = [];
let processingQueued = false;
const PROCESS_INTERVAL = 100; // ms

/**
 * Queue entity updates for batched processing
 * @param {Array} entities - Array of entities to process
 * @param {boolean} needsTransform - Whether entities need transformation
 */
function queueEntityUpdates(entities, needsTransform = true) {
  // Don't queue if we have no entities
  if (!entities || entities.length === 0) return;

  // Add to queue with transform flag
  pendingEntities.push({
    entities,
    needsTransform
  });

  // Schedule processing if not already scheduled
  if (!processingQueued) {
    processingQueued = true;
    setTimeout(processEntityQueue, PROCESS_INTERVAL);
  }
}

/**
 * Process queued entity updates in batch
 */
function processEntityQueue() {
  if (pendingEntities.length > 0) {
    // Find total entity count for logging
    let totalEntityCount = 0;
    pendingEntities.forEach(batch => {
      totalEntityCount += batch.entities.length;
    });

    // Only log if we have entities to process
    if (totalEntityCount > 0 && config.logging?.verbose) {
      logger.info(`Processing batch of ${totalEntityCount} entity updates`);
    }

    // Process each batch
    const allProcessedEntities = [];

    for (const batch of pendingEntities) {
      const { entities, needsTransform } = batch;

      if (needsTransform) {
        // Transform entities all at once
        const transformed = entities.map(entity => ({
          id: entity._id,
          version: entity.version,
          state: entity.state,
          type: entity.type
        }));
        allProcessedEntities.push(...transformed);
      } else {
        // No transformation needed
        allProcessedEntities.push(...entities);
      }
    }

    // Update store once with all entities
    if (allProcessedEntities.length > 0) {
      store.updateEntities(allProcessedEntities);
    }

    // Clear queue
    pendingEntities = [];
  }

  // Reset scheduling flag
  processingQueued = false;
}

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

    switch (message.type) {
      case 'connection_confirm':
          store.setConnectionId(message.connectionId);
          logger.info(`Connection established with ID: ${message.connectionId}`);
          connectUpdateSocket();
          break;

        case 'reconnect_response':
          if (message.success) {
            logger.info(`Reconnection successful with ID: ${store.getConnectionId()}`);
          } else {
            logger.error(`Reconnection failed: ${message.error || 'Unknown error'}`);
          }
          break;

        case 'heartbeat':
          const response = {
            type: 'heartbeat_response',
            timestamp: message.timestamp,
            clientTimestamp: Date.now()
          };

          const commandSocket = store.getCommandSocket();
          if (commandSocket && commandSocket.readyState === WebSocket.OPEN) {
            commandSocket.send(JSON.stringify(response));
            if (Math.random() < 0.05) { // Log roughly 5% of heartbeats
              logger.debug(`Received server heartbeat, responded with timestamp ${response.clientTimestamp}`);
            }
          }

          store.updateLastActivity();
          break;

        case 'initial_sync_complete':
          logger.info('Initial sync complete');
          break;

        case 'sync':
          if (message.data && message.data.entities) {
            logger.info(`Queueing ${message.data.entities.length} entity updates from command sync`);
            queueEntityUpdates(message.data.entities, true);
          }
          break;

        case 'mutation_response':
        case 'mutation_success':
          logger.info(`Received mutation response for entity: ${message.entity?._id}`);
          if (message.entity) {
            queueEntityUpdates([message.entity], true);
          }
          break;

        case 'pong':
        case 'ping_response':
          logger.info(`Received ping response: ${message.timestamp ? `latency=${Date.now() - message.timestamp}ms` : 'no timestamp'}`);
          break;

        case 'reconnect_update_socket':
          logger.info(`Server requested update socket reconnection at ${message.timestamp}`);
          const updateSocket = store.getUpdateSocket();
          if (updateSocket) {
            try {
              if (updateSocket.readyState === WebSocket.OPEN ||
                  updateSocket.readyState === WebSocket.CONNECTING) {
                logger.info('Closing existing update socket before reconnection');
                updateSocket.close();
              }
              store.setUpdateSocket(null);
            } catch (e) {
              logger.error(`Error closing existing update socket: ${e.message}`);
            }
          }

          // Reconnect the update socket
          connectUpdateSocket()
            .then(() => logger.info('Update socket reconnected successfully'))
            .catch(err => logger.error(`Failed to reconnect update socket: ${err.message}`));

          break;

        default:
          logger.info(`Unhandled command message type: ${message.type}`);
          break;
    }

  } catch (error) {
    logger.error(`Error processing command message: ${error.message}`);
  }
};

/**
 * Handle messages from the update socket - optimized for high frequency
 * @param {MessageEvent} event - WebSocket message event
 */
export const handleUpdateSocketMessage = (event) => {
  // Avoid try/catch in the hot path for better performance
  const message = JSON.parse(event.data);

  // Fast path for common message types to minimize processing overhead
  if (message.type === 'sync' && message.data?.entities) {
    // Skip logging for high-frequency updates except occasionally
    const shouldLog = config.logging?.verbose ||
                     (message.data.entities.length > 10 && Math.random() < 0.01); // Log ~1% of large updates

    if (shouldLog) {
      logger.info(`Queueing ${message.data.entities.length} entity updates from update socket`);
    }

    // Queue updates instead of processing immediately
    queueEntityUpdates(message.data.entities, true);
    return;
  }

  // Fast path for legacy format
  if (message.update?.entities) {
    // Queue updates with transform flag based on format
    const needsTransform = message.update.entities[0] &&
                          message.update.entities[0]._id &&
                          !message.update.entities[0].id;

    queueEntityUpdates(message.update.entities, needsTransform);
    return;
  }

  // Handle less common message types
  try {
    if (message.type === 'update_socket_confirm') {
      // Update socket confirmed, we're fully connected
      store.setConnected(true);
      logger.info('Fully connected to server');

      // Update last activity time
      store.updateLastActivity();
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