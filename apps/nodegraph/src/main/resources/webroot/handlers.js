/**
 * WebSocket message handlers
 * Optimized for high-frequency updates
 * Enhanced with heartbeat support and delta merging
 */
import store from './store.js';
import logger from './logger.js';
import { connectDownSocket } from './connection.js';
import config from './config.js';

let pendingEntities = [];
let processingQueued = false;
const PROCESS_INTERVAL = 100;

/**
 * Queue entity updates for batched processing
 * @param {Array} entities - Array of entities to process
 * @param {boolean} needsTransform - Whether entities need transformation
 */
function queueEntityUpdates(entities, needsTransform = true) {
  if (!entities || entities.length === 0) return;

  pendingEntities.push({
    entities,
    needsTransform
  });

  if (!processingQueued) {
    processingQueued = true;
    setTimeout(processEntityQueue, PROCESS_INTERVAL);
  }
}

/**
 * Process queued entity updates in batch with proper delta handling
 */
function processEntityQueue() {
  if (pendingEntities.length > 0) {
    let totalEntityCount = 0;
    pendingEntities.forEach(batch => {
      totalEntityCount += batch.entities.length;
    });

    if (totalEntityCount > 0 && config.logging?.verbose) {
      logger.info(`Processing batch of ${totalEntityCount} entity updates`);
    }

    let deltaUpdates = 0;
    let fullUpdates = 0;
    let transformedUpdates = 0;

    for (const batch of pendingEntities) {
      //console.log(batch);

      const { entities, needsTransform } = batch;

      console.log(entities);

      for (const entity of entities) {
        const entityId = entity._id || entity.id;

        if (!entityId) {
          if (config.logging?.verbose) {
            logger.error('Skipping entity update: missing ID');
          }
          continue;
        }

        // Detect delta vs full state updates
        if (entity.delta && entity.operation === 'update') {
          // This is a delta update - merge with existing state
          const updated = store.mergeEntityDelta(
            entityId,
            entity.delta,
            entity.version,
            entity.type
          );

          if (updated) {
            deltaUpdates++;
          }
        } else if (needsTransform) {
          // This needs transformation to standard format (legacy support)
          const updated = store.updateEntity({
            id: entityId,
            version: entity.version,
            state: entity.state,
            type: entity.type
          });

          if (updated) {
            transformedUpdates++;
          }
        } else {
          // Already in correct format - direct update
          const updated = store.updateEntity(entity);

          if (updated) {
            fullUpdates++;
          }
        }
      }
    }

    // Log processing summary if verbose or significant activity
    if (config.logging?.verbose || (deltaUpdates + fullUpdates + transformedUpdates) > 10) {
      logger.info(`Processed: ${deltaUpdates} deltas, ${fullUpdates} full updates, ${transformedUpdates} transformed`);
    }

    // Clear the queue
    pendingEntities = [];
  }

  processingQueued = false;
}

/**
 * Handle messages from the up socket
 * @param {MessageEvent} event - WebSocket message event
 */
export const handleUpSocketMessage = (event) => {
  try {
    const message = JSON.parse(event.data);

    // Only log detailed message content in verbose mode to reduce noise
    if (config.logging?.verbose) {
      logger.command(`Received up message: ${JSON.stringify(message)}`);
    } else {
      logger.command(`Received up message type: ${message.type}`);
    }

    switch (message.type) {
      case 'connection_confirm':
        store.setConnectionId(message.connectionId);
        logger.info(`Connection established with ID: ${message.connectionId}`);
        connectDownSocket();
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

        const upSocket = store.getUpSocket();
        if (upSocket && upSocket.readyState === WebSocket.OPEN) {
          upSocket.send(JSON.stringify(response));
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
          logger.info(`Queueing ${message.data.entities.length} entity updates from up sync`);
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

      case 'reconnect_down_socket':
        logger.info(`Server requested update socket reconnection at ${message.timestamp}`);
        const downSocket = store.getDownSocket();
        if (downSocket) {
          try {
            if (downSocket.readyState === WebSocket.OPEN ||
                downSocket.readyState === WebSocket.CONNECTING) {
              logger.info('Closing existing update socket before reconnection');
              downSocket.close();
            }
            store.setDownSocket(null);
          } catch (e) {
            logger.error(`Error closing existing update socket: ${e.message}`);
          }
        }

        // Reconnect the update socket
        connectDownSocket()
          .then(() => logger.info('Update socket reconnected successfully'))
          .catch(err => logger.error(`Failed to reconnect update socket: ${err.message}`));

        break;

      default:
        logger.info(`Unhandled up message type: ${message.type}`);
        break;
    }

  } catch (error) {
    logger.error(`Error processing up message: ${error.message}`);
  }
};

/**
 * Handle messages from the update socket - optimized for high frequency
 * @param {MessageEvent} event - WebSocket message event
 */
export const handleDownSocketMessage = (event) => {
  try {
    const message = JSON.parse(event.data);

    // Update last activity time
    store.updateLastActivity();

    // Handle entity updates — each message contains one entity in the updates map
    if (message.type === 'update' && message.updates) {
      const entityArray = Object.values(message.updates);

      if (entityArray.length > 0) {
        queueEntityUpdates(entityArray, false);
      }
      return;
    }

    // Legacy handlers for backward compatibility

    // Handle legacy sync message format
    if (message.type === 'sync' && message.data?.entities) {
      if (config.logging?.verbose) {
        logger.info(`Queueing ${message.data.entities.length} entity updates from sync`);
      }
      queueEntityUpdates(message.data.entities, true);
      return;
    }

    // Handle legacy update format
    if (message.update?.entities) {
      // Queue updates with transform flag based on format
      const needsTransform = message.update.entities[0] &&
                           message.update.entities[0]._id &&
                           !message.update.entities[0].id;

      queueEntityUpdates(message.update.entities, needsTransform);
      return;
    }

    // Handle direct entity updates
    if (message._id && message.state) {
      queueEntityUpdates([message], true);
      return;
    }

    // Other message types
    switch (message.type) {
      case 'down_socket_confirm':
        // Update socket confirmed, we're fully connected
        store.setConnected(true);
        logger.info('Fully connected to server');
        break;

      default:
        if (message.type !== 'heartbeat' && message.type !== 'heartbeat_response') {
          logger.info(`Unhandled update message type: ${message.type}`);
        }
        break;
    }
  } catch (error) {
    logger.error(`Error processing update message: ${error.message}`);
  }
};

export default {
  handleUpSocketMessage,
  handleDownSocketMessage
};