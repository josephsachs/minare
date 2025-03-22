/**
 * Configuration settings for the Minare client
 */
export const config = {
  // WebSocket connection settings
  websocket: {
    // Base URL for WebSockets (derived from current location)
    getBaseUrl: () => {
      const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      return `${wsProtocol}//${window.location.host}`;
    },
    // Command socket endpoint
    commandEndpoint: '/ws',
    // Update socket endpoint
    updateEndpoint: '/ws/updates',
    // Reconnection settings
    reconnect: {
      enabled: false,
      maxAttempts: 3,
      delay: 1000
    }
  },

  // Logging settings
  logging: {
    // Whether to log messages to the browser console
    console: true,
    // Maximum number of log entries to keep in the UI
    maxEntries: 20,
    // Whether to log verbose messages (like all updates and commands)
    verbose: false,
    // How often to flush the log buffer (in milliseconds)
    flushIntervalMs: 1000,
    // Maximum number of messages to buffer before forcing a flush
    maxBufferSize: 2,
    // Throttle update messages (only log 1 in X)
    updateLogThrottle: 10,
    // Enable detailed entity logging
    logDetailedEntities: false
  },

  // Visualization settings
  visualization: {
    // Default visualization type ('grid' or 'd3')
    defaultType: 'grid',
    // D3 force simulation settings
    d3: {
      // Force simulation parameters
      linkDistance: 100,
      chargeStrength: -300,
      collisionRadius: 40,
      // Colors
      nodeDefaultColor: '#CCCCCC',
      linkColor: '#999',
      linkOpacity: 0.6
    }
  }
};

export default config;