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
    maxEntries: 1000
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
  },

  // Simulation settings
  simulation: {
    // Lurker simulation settings
    lurker: {
      minMutationInterval: 1000,  // Minimum time between mutations (ms)
      maxMutationInterval: 5000   // Maximum time between mutations (ms)
    }
  }
};

export default config;