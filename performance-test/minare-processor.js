'use strict';

const WebSocket = require('ws');

// Store virtual user sessions
const userSessions = new Map();

module.exports = {
  setupVirtualUser,
  connectCommandSocket,
  connectUpdateSocket,
  sendTestCommand
};

/**
 * Set up a new virtual user
 */
function setupVirtualUser(context, events, done) {
  // Generate a unique session ID for this virtual user
  const sessionId = `user_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;

  // Store session data
  userSessions.set(sessionId, {
    connectionId: null,
    commandSocket: null,
    updateSocket: null,
    entityGraph: null,
    connected: false,
    errors: []
  });

  // Store session ID in context for later functions
  context.vars.sessionId = sessionId;

  console.log(`[${sessionId}] Virtual user created`);
  return done();
}

/**
 * Connect to the command socket
 */
function connectCommandSocket(context, events, done) {
  const sessionId = context.vars.sessionId;
  const session = userSessions.get(sessionId);

  if (!session) {
    console.error(`[${sessionId}] Session not found`);
    return done(new Error('Session not found'));
  }

  const wsUrl = 'ws://localhost:8080/ws';
  console.log(`[${sessionId}] Connecting to command socket at ${wsUrl}`);

  try {
    const startTime = Date.now();
    const socket = new WebSocket(wsUrl);

    // Socket opened successfully
    socket.on('open', () => {
      console.log(`[${sessionId}] Command socket connected`);
      session.commandSocket = socket;

      // Record metrics
      events.emit('counter', 'ws.command.connect.success', 1);
      events.emit('response', 'ws.command.connect', startTime, Date.now() - startTime, 200, 'Connected');
    });

    // Handle messages from server
    socket.on('message', (data) => {
      try {
        const message = JSON.parse(data);
        console.log(`[${sessionId}] Command socket received: ${message.type}`);

        // If this is a welcome/connection confirmation message, store the connection ID
        if (message.type === 'welcome' || message.type === 'connection_confirm') {
          // Extract connection ID from message
          const connectionId = message.connection_id || message.connectionId;
          if (connectionId) {
            console.log(`[${sessionId}] Received connection ID: ${connectionId}`);
            session.connectionId = connectionId;

            // Mark as ready for the next step
            done();
          } else {
            console.error(`[${sessionId}] No connection ID in welcome message`);
            done(new Error('No connection ID received'));
          }
        }
      } catch (err) {
        console.error(`[${sessionId}] Error parsing message: ${err.message}`);
        session.errors.push(`Parse error: ${err.message}`);
      }
    });

    // Handle errors
    socket.on('error', (err) => {
      console.error(`[${sessionId}] Command socket error: ${err.message}`);
      session.errors.push(`Command socket error: ${err.message}`);
      events.emit('counter', 'ws.command.connect.error', 1);
      done(err);
    });

    // Handle socket closing
    socket.on('close', (code, reason) => {
      console.log(`[${sessionId}] Command socket closed: ${code} ${reason}`);
      session.commandSocket = null;

      // If we didn't get a connection ID yet, this is an error
      if (!session.connectionId) {
        done(new Error(`Command socket closed without connection ID: ${code} ${reason}`));
      }
    });
  } catch (err) {
    console.error(`[${sessionId}] Failed to create command socket: ${err.message}`);
    session.errors.push(`Socket creation error: ${err.message}`);
    events.emit('counter', 'ws.command.connect.error', 1);
    done(err);
  }
}

/**
 * Connect to the update socket
 */
function connectUpdateSocket(context, events, done) {
  const sessionId = context.vars.sessionId;
  const session = userSessions.get(sessionId);

  if (!session) {
    console.error(`[${sessionId}] Session not found`);
    return done(new Error('Session not found'));
  }

  if (!session.connectionId) {
    console.error(`[${sessionId}] No connection ID available`);
    return done(new Error('No connection ID available'));
  }

  const wsUrl = `ws://localhost:8080/ws/updates?connection_id=${session.connectionId}`;
  console.log(`[${sessionId}] Connecting to update socket at ${wsUrl}`);

  try {
    const startTime = Date.now();
    const socket = new WebSocket(wsUrl);

    // Socket opened successfully
    socket.on('open', () => {
      console.log(`[${sessionId}] Update socket connected`);
      session.updateSocket = socket;

      // Record metrics
      events.emit('counter', 'ws.update.connect.success', 1);
      events.emit('response', 'ws.update.connect', startTime, Date.now() - startTime, 200, 'Connected');

      // We've successfully connected to both sockets
      session.connected = true;
      done();
    });

    // Handle messages from server
    socket.on('message', (data) => {
      try {
        const message = JSON.parse(data);
        console.log(`[${sessionId}] Update socket received: ${message.type}`);

        // If we get a sync message, store the entity graph
        if (message.type === 'sync') {
          if (message.entities || (message.data && message.data.entities)) {
            const entities = message.entities || message.data.entities;
            console.log(`[${sessionId}] Received entity graph with ${entities.length} entities`);
            session.entityGraph = entities;

            // Record metrics
            events.emit('counter', 'entities.received', entities.length);
          }
        }
      } catch (err) {
        console.error(`[${sessionId}] Error parsing update message: ${err.message}`);
        session.errors.push(`Update parse error: ${err.message}`);
      }
    });

    // Handle errors
    socket.on('error', (err) => {
      console.error(`[${sessionId}] Update socket error: ${err.message}`);
      session.errors.push(`Update socket error: ${err.message}`);
      events.emit('counter', 'ws.update.connect.error', 1);
      done(err);
    });

    // Handle socket closing
    socket.on('close', (code, reason) => {
      console.log(`[${sessionId}] Update socket closed: ${code} ${reason}`);
      session.updateSocket = null;
      session.connected = false;
    });
  } catch (err) {
    console.error(`[${sessionId}] Failed to create update socket: ${err.message}`);
    session.errors.push(`Update socket creation error: ${err.message}`);
    events.emit('counter', 'ws.update.connect.error', 1);
    done(err);
  }
}

/**
 * Send a test command via the command socket
 */
function sendTestCommand(context, events, done) {
  const sessionId = context.vars.sessionId;
  const session = userSessions.get(sessionId);

  if (!session) {
    console.error(`[${sessionId}] Session not found`);
    return done(new Error('Session not found'));
  }

  if (!session.commandSocket || !session.connected) {
    console.error(`[${sessionId}] Not connected to server`);
    return done(new Error('Not connected to server'));
  }

  // Create a simple test command
  const testCommand = {
    command: 'ping',
    timestamp: Date.now()
  };

  console.log(`[${sessionId}] Sending test command: ${JSON.stringify(testCommand)}`);

  try {
    const startTime = Date.now();
    session.commandSocket.send(JSON.stringify(testCommand));

    // Record metrics
    events.emit('counter', 'command.sent', 1);
    events.emit('response', 'command.sent', startTime, Date.now() - startTime, 200, 'Sent');

    // We don't wait for a response in this simple test
    done();
  } catch (err) {
    console.error(`[${sessionId}] Error sending command: ${err.message}`);
    session.errors.push(`Command send error: ${err.message}`);
    events.emit('counter', 'command.error', 1);
    done(err);
  }
}

// Cleanup function
process.on('SIGINT', () => {
  console.log('Cleaning up WebSocket connections...');

  userSessions.forEach((session, sessionId) => {
    if (session.commandSocket) {
      try {
        session.commandSocket.close();
      } catch (err) {
        console.error(`[${sessionId}] Error closing command socket: ${err.message}`);
      }
    }

    if (session.updateSocket) {
      try {
        session.updateSocket.close();
      } catch (err) {
        console.error(`[${sessionId}] Error closing update socket: ${err.message}`);
      }
    }
  });

  console.log('Cleanup complete, exiting...');
  process.exit(0);
});