/**
 * Configuration settings for the Minare client
 */
export const config = {

  websocket: {
    wsProtocol: 'ws://',
    host: 'localhost',
    commandEndpoint: '/command',
    commandPort: "4225",

    updateEndpoint: '/update',
    updatePort: "4226",

    reconnect: {
      enabled: false,
      maxAttempts: 3,
      delay: 1000
    }
  },


  logging: {

    console: true,

    maxEntries: 20,

    verbose: false,

    flushIntervalMs: 1000,

    maxBufferSize: 2,

    updateLogThrottle: 10,

    logDetailedEntities: false
  },


  visualization: {
    defaultType: typeof vis !== 'undefined' ? 'vis' : 'grid',

    d3: {

      linkDistance: 100,
      chargeStrength: -300,
      collisionRadius: 40,

      nodeDefaultColor: '#CCCCCC',
      linkColor: '#999',
      linkOpacity: 0.6
    },

    vis: {

      node: {
        size: 20,
        color: '#CCCCCC',
        borderWidth: 2,
        font: {
          size: 14
        }
      },

      edge: {
        width: 1,
        color: '#999',
        opacity: 0.6,
        smooth: true
      },

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