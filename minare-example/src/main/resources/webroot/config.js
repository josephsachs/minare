/**
 * Configuration settings for the Minare client
 */
export const config = {
  // WebSocket connection settings
  websocket: {
    wsProtocol: 'ws://',
    host: 'localhost',
    commandEndpoint: '/',
    commandPort: "4225",
    // Update socket endpoint
    updateEndpoint: '/',
    updatePort: "4226",
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
    // Default visualization type ('grid' or 'vis')
    // If vis.js isn't available, this will be forced to 'grid' at runtime
    defaultType: typeof vis !== 'undefined' ? 'vis' : 'grid',
    // D3 force simulation settings (kept for backward compatibility)
    d3: {
      // Force simulation parameters
      linkDistance: 100,
      chargeStrength: -300,
      collisionRadius: 40,
      // Colors
      nodeDefaultColor: '#CCCCCC',
      linkColor: '#999',
      linkOpacity: 0.6
    },
    // Vis.js network visualization settings
    vis: {
      // Node settings
      node: {
        size: 20,
        color: '#CCCCCC',
        borderWidth: 2,
        font: {
          size: 14
        }
      },
      // Edge settings
      edge: {
        width: 1,
        color: '#999',
        opacity: 0.6,
        smooth: true
      },
      // Physics settings
      physics: {
        enabled: true,
        barnesHut: {
          gravitationalConstant: -2000,
          centralGravity: 0.3
        },
        stabilization: {
          iterations: 100
        }
      }
    }
  }
};

export default config;