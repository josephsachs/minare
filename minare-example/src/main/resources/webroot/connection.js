/**
 * WebSocket connection management
 */
import config from './config.js';
import store from './store.js';
import logger from './logger.js';
import { handleCommandSocketMessage, handleUpdateSocketMessage } from './handlers.js';

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
    } catch (error) {
      logger.error(`Failed to create WebSocket: ${error.message}`);
      reject(error);
    }
  });
};

/**
 * Connect command socket
 * @returns {Promise<WebSocket>} Command socket instance
 */
export const connectCommandSocket = async () => {
  // Get WebSocket base URL
  const baseUrl = config.websocket.getBaseUrl();
  const url = `${baseUrl}${config.websocket.commandEndpoint}`;

  logger.info('Connecting command socket...');

  try {
    const socket = await createWebSocket(
      url,
      handleCommandSocketMessage,
      {
        onClose: () => {
          if (store.isConnected()) {
            disconnect();
          }
        }
      }
    );

    store.setCommandSocket(socket);
    logger.command('Command socket connected');

    return socket;
  } catch (error) {
    logger.error(`Command socket connection failed: ${error.message}`);
    throw error;
  }
};

/**
 * Connect update socket
 * @returns {Promise<WebSocket>} Update socket instance
 */
export const connectUpdateSocket = async () => {
  const connectionId = store.getConnectionId();
  if (!connectionId) {
    throw new Error('No connection ID available');
  }

  // Get WebSocket base URL
  const baseUrl = config.websocket.getBaseUrl();
  const url = `${baseUrl}${config.websocket.updateEndpoint}`;

  logger.info('Connecting update socket...');

  try {
    const socket = await createWebSocket(
      url,
      handleUpdateSocketMessage,
      {
        onClose: () => {
          if (store.isConnected()) {
            disconnect();
          }
        }
      }
    );

    store.setUpdateSocket(socket);
    logger.update('Update socket connected');

    // Send connection ID to associate the update socket
    const associationMessage = {
      connectionId: connectionId
    };
    socket.send(JSON.stringify(associationMessage));

    return socket;
  } catch (error) {
    logger.error(`Update socket connection failed: ${error.message}`);
    throw error;
  }
};

/**
 * Connect to the server (both command and update sockets)
 * @returns {Promise<boolean>} Success indicator
 */
export const connect = async () => {
  try {
    // Connect command socket first
    await connectCommandSocket();

    // Update socket will be connected after receiving connection ID
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
  const updateSocket = store.getUpdateSocket();
  if (updateSocket) {
    updateSocket.close();
    store.setUpdateSocket(null);
  }

  const commandSocket = store.getCommandSocket();
  if (commandSocket) {
    commandSocket.close();
    store.setCommandSocket(null);
  }

  // Reset state
  store.setConnectionId(null);
  store.setConnected(false);
  store.clearEntities();

  logger.info('Disconnected from server');
};

/**
 * Send a command to the server
 * @param {Object} command - Command object
 * @returns {boolean} Success indicator
 */
export const sendCommand = (command) => {
  const socket = store.getCommandSocket();

  if (!socket || socket.readyState !== WebSocket.OPEN) {
    logger.error('Cannot send command: Command socket not connected');
    return false;
  }

  try {
    const message = JSON.stringify(command);
    socket.send(message);
    logger.command(`Sent command: ${message}`);
    return true;
  } catch (error) {
    logger.error(`Failed to send command: ${error.message}`);
    return false;
  }
};

export default {
  connect,
  disconnect,
  sendCommand,
  connectCommandSocket,
  connectUpdateSocket
};