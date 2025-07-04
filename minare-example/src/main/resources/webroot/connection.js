/**
 * WebSocket connection management with reconnection support
 * Enhanced with improved heartbeat/reconnection support
 */
import config from './config.js';
import store from './store.js';
import logger from './logger.js';
import { handleUpSocketMessage, handleDownSocketMessage } from './handlers.js';

/**
 * Create a WebSocket connection
 * @param {string} url - WebSocket URL
 * @param {Function} onMessage - Message handler function
 * @param {Object} options - Additional options
 * @returns {Promise<WebSocket>} Websocket instance
 */
const createWebSocket = (url, onMessage, options = {}) => {
  return new Promise((resolve, reject) => {
    try {
      const socket = new WebSocket(url);

      socket.onopen = () => {
        logger.info(`WebSocket connected to ${url}`);
        resolve(socket);
      };

      socket.onmessage = onMessage;

      socket.onerror = (error) => {
        logger.error(`WebSocket error: ${error}`);
        if (options.onError) options.onError(error);
      };

      socket.onclose = (event) => {
        logger.info(`WebSocket closed: ${event.code} ${event.reason}`);
        if (options.onClose) options.onClose(event);
      };


      const timeout = setTimeout(() => {
        if (socket.readyState !== WebSocket.OPEN) {
          logger.error(`WebSocket connection timeout to ${url}`);
          socket.close();
          reject(new Error('Connection timeout'));
        }
      }, 10000);


      socket.addEventListener('open', () => clearTimeout(timeout));

    } catch (error) {
      logger.error(`Failed to create WebSocket: ${error.message}`);
      reject(error);
    }
  });
};

/**
 * Connect up socket with reconnection support
 * @param {boolean} isReconnect - If this is a reconnection attempt
 * @returns {Promise<WebSocket>} Up socket instance
 */
export const connectUpSocket = async (isReconnect = false) => {

  const url = `${config.websocket.wsProtocol}${config.websocket.host}:${config.websocket.upPort}${config.websocket.upEndpoint}`;

  logger.info(isReconnect ? 'Reconnecting up socket...' : 'Connecting up socket...');

  try {
    const socket = await createWebSocket(
      url,
      handleUpSocketMessage,
      {
        onClose: (event) => {

          if (store.isConnected()) {

            if (event.code !== 1000 && config.websocket.reconnect.enabled) {
              handleUpSocketDisconnect();
            } else {

              disconnect();
            }
          }
        }
      }
    );

    store.setUpSocket(socket);
    logger.command('Up socket connected');


    if (isReconnect) {
      const connectionId = store.getConnectionId();
      if (connectionId) {
        const reconnectMessage = {
          reconnect: true,
          connectionId: connectionId,
          timestamp: Date.now()
        };
        socket.send(JSON.stringify(reconnectMessage));
        logger.info(`Sent reconnection request for connection ${connectionId}`);
      }
    }

    return socket;
  } catch (error) {
    logger.error(`Up socket connection failed: ${error.message}`);
    throw error;
  }
};

/**
 * Connect down socket
 * @returns {Promise<WebSocket>} Down socket instance
 */
export const connectDownSocket = async () => {
  const connectionId = store.getConnectionId();
  if (!connectionId) {
    throw new Error('No connection ID available');
  }

  const url = `${config.websocket.wsProtocol}${config.websocket.host}:${config.websocket.downPort}${config.websocket.downEndpoint}`;

  logger.info('Connecting down socket...');

  try {
    const socket = await createWebSocket(
      url,
      handleDownSocketMessage,
      {
        onClose: () => {
          if (store.isConnected()) {
            handleDownSocketDisconnect();
          }
        }
      }
    );

    store.setDownSocket(socket);
    logger.update('Down socket connected');

    const associationMessage = {
      connectionId: connectionId
    };
    socket.send(JSON.stringify(associationMessage));

    return socket;
  } catch (error) {
    logger.error(`Down socket connection failed: ${error.message}`);
    throw error;
  }
};

/**
 * Connect to the server (both up and down sockets)
 * @param {boolean} isReconnect - Whether this is a reconnect attempt
 * @returns {Promise<boolean>} Success indicator
 */
export const connect = async (isReconnect = false) => {
  try {

    await connectUpSocket(isReconnect);


    return true;
  } catch (error) {
    logger.error(`Connection failed: ${error.message}`);
    return false;
  }
};

/**
 * Disconnect from the server
 */
export const disconnect = () => {
  const downSocket = store.getDownSocket();
  if (downSocket) {
    downSocket.close();
    store.setDownSocket(null);
  }

  const upSocket = store.getUpSocket();
  if (upSocket) {
    upSocket.close();
    store.setUpSocket(null);
  }


  store.setConnectionId(null);
  store.setConnected(false);
  store.clearEntities();

  logger.info('Disconnected from server');
};

/**
 * Handle up socket disconnect
 */
const handleUpSocketDisconnect = () => {
  logger.warn('Up socket disconnected unexpectedly, attempting reconnect...');
  store.setConnected(false);


  store.setUpSocket(null);

  // Try to reconnect with exponential backoff
  attemptReconnect(0);
};

/**
 * Handle update socket disconnect
 */
const handleDownSocketDisconnect = () => {
  logger.warn('Down socket disconnected, attempting to reconnect...');

  // Clear update socket reference
  store.setDownSocket(null);

  // Attempt to reconnect the update socket
  const attemptUpdateReconnect = async () => {
    try {
      await connectDownSocket();
      logger.info('Down socket reconnected successfully');
    } catch (error) {
      logger.error(`Failed to reconnect update socket: ${error.message}`);

      // If still connected (up socket is alive), try again after delay
      if (store.getUpSocket()) {
        setTimeout(attemptUpdateReconnect, 1000);
      }
    }
  };

  attemptUpdateReconnect();
};

/**
 * Attempt reconnection with exponential backoff
 * @param {number} attempt - Current attempt number
 */
const attemptReconnect = async (attempt) => {
  const maxAttempts = config.websocket.reconnect.maxAttempts;

  if (attempt >= maxAttempts) {
    logger.error(`Failed to reconnect after ${maxAttempts} attempts, giving up`);
    disconnect(); // Full disconnect
    return;
  }

  const delay = Math.min(30000, config.websocket.reconnect.delay * Math.pow(2, attempt));
  logger.info(`Reconnect attempt ${attempt + 1} in ${delay}ms...`);

  setTimeout(async () => {
    try {
      const success = await connect(true); // With reconnect flag

      if (success) {
        logger.info('Reconnected successfully');
      } else {
        // Try again with increased attempt count
        attemptReconnect(attempt + 1);
      }
    } catch (error) {
      logger.error(`Reconnection attempt failed: ${error.message}`);
      // Try again with increased attempt count
      attemptReconnect(attempt + 1);
    }
  }, delay);
};

/**
 * Check connection health
 * Detects if sockets are stale based on last activity
 * @returns {boolean} True if connection is healthy
 */
export const checkConnectionHealth = () => {
  // If we're not connected, no need to check
  if (!store.isConnected()) {
    return false;
  }

  // Check if the connection is stale (no activity for too long)
  if (store.isConnectionStale()) {
    logger.warn('Connection appears stale, no activity detected recently');

    // Attempt reconnection
    disconnect();
    connect(false);
    return false;
  }

  return true;
};

/**
 * Send a command to the server
 * @param {Object} command - Command object
 * @returns {boolean} Success indicator
 */
export const sendCommand = (command) => {
  const socket = store.getUpSocket();

  if (!socket || socket.readyState !== WebSocket.OPEN) {
    logger.error('Cannot send command: Up socket not connected');
    return false;
  }

  try {
    const message = JSON.stringify(command);
    socket.send(message);
    logger.command(`Sent command: ${message}`);

    // Update last activity time
    store.updateLastActivity();
    return true;
  } catch (error) {
    logger.error(`Failed to send command: ${error.message}`);
    return false;
  }
};

// Configuration example for websocket reconnection
export const reconnectConfig = {
  websocket: {
    // Base URL for WebSockets (derived from current location)
    getBaseUrl: () => {
      const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      return `${wsProtocol}//${window.location.host}`;
    },
    // Up socket endpoint
    upEndpoint: '/',
    upPort: 4225,
    // Update socket endpoint
    downEndpoint: '/',
    downPort: 4226,
    // Reconnection settings
    reconnect: {
      enabled: true,
      maxAttempts: 5,
      delay: 1000  // Base delay in ms, will be used with exponential backoff
    }
  }
};

// Start connection health check timer
setInterval(checkConnectionHealth, 30000); // Check every 30 seconds

export default {
  connect,
  disconnect,
  sendCommand,
  connectUpSocket,
  connectDownSocket,
  checkConnectionHealth
};