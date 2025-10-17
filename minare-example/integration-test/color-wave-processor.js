'use strict';

const WebSocket = require('ws');

// Store active user sessions
const userSessions = new Map();

// Flag to ensure only one user runs the color waves
let colorWaveStarted = false;

// Color progression - from beige through the spectrum to red/orange
const COLORS = [
  '#F5F5DC', // beige
  '#FFE4B5', // pale gold
  '#FFFF00', // yellow
  '#ADFF2F', // yellow-green
  '#00FF00', // lime
  '#2E8B57', // sea green
  '#008000', // green
  '#40E0D0', // turquoise
  '#008080', // teal
  '#00CED1', // blue green
  '#0000FF', // blue
  '#4B0082', // indigo
  '#722F37', // wine
  '#800080', // purple
  '#8B008B', // dark magenta
  '#C71585', // medium violet red
  '#FF1493', // deep pink
  '#FF69B4', // hot pink
  '#FFC0CB', // pink
  '#FFB6C1', // light pink
  '#FFA07A', // light salmon
  '#FA8072', // salmon
  '#FF6347', // tomato
  '#FF4500', // orange red
  '#FF0000', // red
  '#DC143C', // crimson
  '#B22222', // fire brick
  '#8B0000', // dark red
  '#FF8C00', // dark orange
  '#FFA500', // orange
  '#FFD700', // gold
  '#DAA520', // goldenrod
  '#B8860B', // dark goldenrod
  '#CD853F', // peru
  '#D2691E', // chocolate
  '#A0522D', // sienna
  '#8B4513', // saddle brown
  '#654321', // dark brown
  '#000000'  // black (final wave)
];

module.exports = {
  connectCommandSocket,
  connectDownSocket,
  startColorWaves
};

/**
 * Connect to the command socket and store the connection ID
 * Returns a promise that resolves when connected and sync is received
 */
function connectCommandSocket(context, events, done) {
  console.log('=== connectCommandSocket called ===');

  const userId = context.vars.userId || `wave_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
  context.vars.userId = userId;

  console.log(`[${userId}] Connecting to command socket for color wave test...`);

  const ws = new WebSocket('ws://localhost:4225/command');

  const connectionTimeout = setTimeout(() => {
    console.error(`[${userId}] Connection timeout`);
    events.emit('counter', 'ws.command.timeout', 1);
    ws.terminate();
    done(new Error('Connection timeout'));
  }, 10000);

  ws.on('open', () => {
    console.log(`[${userId}] Connected to command socket`);
    clearTimeout(connectionTimeout);
    userSessions.set(userId, { commandSocket: ws });
  });

  ws.on('message', (data) => {
    try {
      const message = JSON.parse(data.toString());

      if (message.type === 'connection_confirm') {
        const connectionId = message.connectionId;
        console.log(`[${userId}] Connection ID: ${connectionId}`);

        const session = userSessions.get(userId) || {};
        session.connectionId = connectionId;
        userSessions.set(userId, session);

        context.vars.connectionId = connectionId;
        events.emit('counter', 'ws.command.success', 1);

        // Send sync request immediately
        const syncRequest = {
          command: "sync",
          timestamp: Date.now()
        };
        ws.send(JSON.stringify(syncRequest));
        console.log(`[${userId}] Sent sync request`);

        done();
      } else if (message.type === 'sync' && message.data) {
        // Store entities from sync response
        const session = userSessions.get(userId) || {};
        session.entities = message.data.entities || [];
        userSessions.set(userId, session);
        console.log(`[${userId}] Received ${session.entities.length} entities from sync`);
      }
    } catch (err) {
      console.error(`[${userId}] Error handling command socket message:`, err);
    }
  });

  ws.on('error', (error) => {
    console.error(`[${userId}] Command socket error:`, error);
    clearTimeout(connectionTimeout);
    done(error);
  });

  ws.on('close', (code, reason) => {
    console.log(`[${userId}] Command socket closed: ${code} ${reason}`);
  });
}

/**
 * Connect to the down socket for updates
 */
function connectDownSocket(context, events, done) {
  const userId = context.vars.userId;
  const connectionId = context.vars.connectionId;

  if (!connectionId) {
    console.error(`[${userId}] No connection ID available`);
    return done(new Error('No connection ID'));
  }

  console.log(`[${userId}] Connecting to down socket with ID: ${connectionId}`);

  const downSocket = new WebSocket('ws://localhost:4226/update');

  const connectionTimeout = setTimeout(() => {
    console.error(`[${userId}] Down socket connection timeout`);
    events.emit('counter', 'ws.update.timeout', 1);
    downSocket.terminate();
    done(new Error('Down socket connection timeout'));
  }, 10000);

  downSocket.on('open', () => {
    console.log(`[${userId}] Connected to down socket, sending connection ID`);

    // Send connection ID as a message (not in URL)
    try {
      const connectionMessage = {
        connectionId: connectionId
      };
      downSocket.send(JSON.stringify(connectionMessage));
      console.log(`[${userId}] Sent connection ID to down socket`);
    } catch (err) {
      console.error(`[${userId}] Error sending connection ID:`, err);
      clearTimeout(connectionTimeout);
      done(err);
      return;
    }

    const session = userSessions.get(userId) || {};
    session.downSocket = downSocket;
    userSessions.set(userId, session);

    events.emit('counter', 'ws.update.success', 1);
  });

  downSocket.on('message', (data) => {
    try {
      const message = JSON.parse(data.toString());

      // Look for confirmation message first
      if (message.type === 'down_socket_confirm') {
        console.log(`[${userId}] Down socket confirmed`);
        clearTimeout(connectionTimeout);
        done();
      } else if (message.type === 'sync' && message.data && message.data.entities) {
        const session = userSessions.get(userId) || {};
        session.entities = message.data.entities;
        userSessions.set(userId, session);
        console.log(`[${userId}] Received ${session.entities.length} entities from down socket sync`);
      }
    } catch (err) {
      console.error(`[${userId}] Error handling down socket message:`, err);
    }
  });

  downSocket.on('error', (error) => {
    console.error(`[${userId}] Down socket error:`, error);
    clearTimeout(connectionTimeout);
    done(error);
  });
}

/**
 * Start the color wave sequence
 * Uses callbacks to match the other functions
 */
function startColorWaves(context, events, done) {
  const userId = context.vars.userId;

  // Only let the first user run the color waves
  if (colorWaveStarted) {
    console.log(`[${userId}] Color waves already started by another user, skipping`);
    return done();
  }
  colorWaveStarted = true;

  const session = userSessions.get(userId);

  if (!session || !session.commandSocket || !session.entities || session.entities.length === 0) {
    console.error(`[${userId}] Cannot start color waves - missing session data`);
    return done(new Error('Missing session data for color waves'));
  }

  console.log(`[${userId}] Starting color waves with ${session.entities.length} entities`);

  let currentEntityIndex = 0;
  let currentColorIndex = 0;
  let currentDelay = 100; // Start at 1000ms

  // Helper function to send a single mutation
  function sendMutation(entity, color) {
    const mutationCommand = {
      command: "mutate",
      entity: {
        _id: entity._id,
        type: entity.type || 'Node',
        version: entity.version || 1,
        state: {
          color: color
        }
      }
    };

    try {
      session.commandSocket.send(JSON.stringify(mutationCommand));

      const label = entity.state?.label || entity._id;
      console.log(`[${userId}] Wave ${currentColorIndex + 1}/${COLORS.length}: ` +
                  `Entity ${currentEntityIndex + 1}/${session.entities.length} (${label}) → ${color} ` +
                  `[delay: ${currentDelay}ms]`);

      events.emit('counter', 'mutation.sent', 1);
      events.emit('counter', `wave.${currentColorIndex}.mutation`, 1);
      return true;
    } catch (error) {
      console.error(`[${userId}] Error sending mutation:`, error);
      events.emit('counter', 'mutation.error', 1);
      return false;
    }
  }

  // Recursive function to process each mutation with delays
  function processNextMutation() {
    // Check if we've completed all waves
    if (currentColorIndex >= COLORS.length) {
      console.log(`[${userId}] Color waves complete!`);
      events.emit('counter', 'waves.complete', 1);
      return done();
    }

    const color = COLORS[currentColorIndex];

    // Log wave start
    if (currentEntityIndex === 0) {
      console.log(`[${userId}] Starting wave ${currentColorIndex + 1} with color ${color} at ${currentDelay}ms intervals`);
    }

    // Send mutation for current entity
    const entity = session.entities[currentEntityIndex];
    const success = sendMutation(entity, color);

    if (!success) {
      console.error(`[${userId}] Aborting color waves due to mutation error`);
      return done(new Error('Mutation failed'));
    }

    // Move to next entity
    currentEntityIndex++;

    // If we've done all entities, move to next color wave
    if (currentEntityIndex >= session.entities.length) {
      currentEntityIndex = 0;
      currentColorIndex++;

      // Special handling for the final black wave
      if (currentColorIndex === COLORS.length - 1) {
        console.log(`[${userId}] Starting final black wave at 10ms intervals`);
        currentDelay = 10;
      } else if (currentColorIndex < COLORS.length - 1) {
        // Decrease delay by 25ms for each wave (except the last)
        currentDelay = Math.max(currentDelay - 25, 25); // Don't go below 25ms until final wave
      }
    }

    // Schedule the next mutation after the current delay
    setTimeout(processNextMutation, currentDelay);
  }

  // Start the color wave sequence
  processNextMutation();
}

// Cleanup function
process.on('SIGINT', () => {
  console.log('Cleaning up connections...');
  for (const [userId, session] of userSessions.entries()) {
    if (session.commandSocket) {
      try {
        session.commandSocket.close();
      } catch (err) {
        console.error(`Error closing command socket:`, err);
      }
    }
    if (session.downSocket) {
      try {
        session.downSocket.close();
      } catch (err) {
        console.error(`Error closing down socket:`, err);
      }
    }
  }
  console.log('Cleanup complete');
  process.exit(0);
});