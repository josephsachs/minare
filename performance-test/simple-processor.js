'use strict';

const WebSocket = require('ws');

// Store active user sessions
const userSessions = new Map();

module.exports = {
  connectCommandSocket,
  connectUpdateSocket,
  sendMutationCommand
};

/**
 * Connect to the command socket and store the connection ID
 */
function connectCommandSocket(context, events, done) {
  // Generate user ID if not already present
  const userId = context.vars.userId || `user_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
  context.vars.userId = userId;

  console.log(`[${userId}] Connecting to command socket...`);

  // Create WebSocket connection
  const ws = new WebSocket('ws://localhost:8080/ws');

  // Set a timeout to detect connection issues
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
      console.log(`[${userId}] Received: ${JSON.stringify(message)}`);

      if (message.type === 'connection_confirm') {
        const connectionId = message.connectionId;
        console.log(`[${userId}] Connection ID: ${connectionId}`);

        // Store connection data
        const session = userSessions.get(userId) || {};
        session.connectionId = connectionId;
        userSessions.set(userId, session);

        // Save connection ID in context for next function
        context.vars.connectionId = connectionId;

        // Record success
        events.emit('counter', 'ws.command.success', 1);

        clearTimeout(connectionTimeout);
        done();
      }
    } catch (err) {
      console.error(`[${userId}] Error parsing message:`, err);
    }
  });

  ws.on('error', (error) => {
    console.error(`[${userId}] WebSocket error:`, error);
    clearTimeout(connectionTimeout);
    events.emit('counter', 'ws.command.error', 1);
    done(error);
  });

  ws.on('close', (code, reason) => {
    console.log(`[${userId}] Connection closed: ${code} ${reason}`);

    // If we don't have a connection ID yet, this is an error
    if (!context.vars.connectionId) {
      clearTimeout(connectionTimeout);
      done(new Error(`Connection closed before getting connection ID: ${code} ${reason}`));
    }
  });
}

/**
 * Connect to the update socket using the connection ID
 */
function connectUpdateSocket(context, events, done) {
  const userId = context.vars.userId;
  const connectionId = context.vars.connectionId;

  if (!connectionId) {
    console.error(`[${userId}] No connection ID available`);
    return done(new Error('No connection ID available'));
  }

  console.log(`[${userId}] Connecting to update socket with ID: ${connectionId}`);

  // Create update socket connection
  const updateUrl = `ws://localhost:8080/ws/updates?connection_id=${connectionId}`;
  const ws = new WebSocket(updateUrl);

  // Set a timeout to detect connection issues
  const connectionTimeout = setTimeout(() => {
    console.error(`[${userId}] Update socket connection timeout`);
    events.emit('counter', 'ws.update.timeout', 1);
    ws.terminate();
    done(new Error('Update socket connection timeout'));
  }, 10000);

  // After successful update socket connection, request entities
  ws.on('open', () => {
    console.log(`[${userId}] Connected to update socket`);
    clearTimeout(connectionTimeout);

    // Store update socket in session
    const session = userSessions.get(userId) || {};
    session.updateSocket = ws;
    userSessions.set(userId, session);

    // Record success
    events.emit('counter', 'ws.update.success', 1);
    events.emit('counter', 'ws.update.open', 1);

    // Try sending a sync request message
    try {
      console.log(`[${userId}] Sending sync request to update socket`);
      const syncRequest = {
        type: "sync_request",
        timestamp: Date.now()
      };
      ws.send(JSON.stringify(syncRequest));
    } catch (syncErr) {
      console.error(`[${userId}] Error sending sync request:`, syncErr);
    }

    done();
  });

  ws.on('message', (data) => {
    try {
      const rawData = data.toString();
      console.log(`[${userId}] RAW update socket message: ${rawData}`);

      const message = JSON.parse(rawData);
      const messageType = message.type || 'unknown';

      console.log(`[${userId}] Update socket received message type: ${messageType}`);

      // Check for confirmation message
      if (messageType === 'update_socket_confirm') {
        console.log(`[${userId}] Update socket confirmed for connection ${message.connectionId}`);

        // Record success
        events.emit('counter', 'ws.update.success', 1);

        // Once the update socket is confirmed, we're done with this step
        done();
      }

      // Check for sync message with entities (just in case)
      if (messageType === 'sync') {
        console.log(`[${userId}] Received sync message on update socket with keys: ${Object.keys(message).join(', ')}`);

        if (message.data && message.data.entities && Array.isArray(message.data.entities)) {
          console.log(`[${userId}] Found ${message.data.entities.length} entities in update socket sync message`);

          // Store entities in session
          const session = userSessions.get(userId) || {};
          session.entities = message.data.entities;
          userSessions.set(userId, session);
        }
      }
    } catch (err) {
      console.error(`[${userId}] Error handling update socket message:`, err);
    }
  });

  ws.on('error', (error) => {
    console.error(`[${userId}] Update socket error:`, error);
    clearTimeout(connectionTimeout);
    events.emit('counter', 'ws.update.error', 1);
    done(error);
  });

  ws.on('close', (code, reason) => {
    console.log(`[${userId}] Update socket closed: ${code} ${reason}`);
  });
}

/**
 * Send a mutation command
 */
function sendMutationCommand(context, events, done) {
  const userId = context.vars.userId;
  const session = userSessions.get(userId);

  if (!session || !session.commandSocket) {
    console.error(`[${userId}] No command socket available`);
    return done(new Error('No command socket available'));
  }

  // Wait for entity graph if we don't have it yet
  if (!session.entities || session.entities.length === 0) {
    console.log(`[${userId}] Waiting for entity graph before sending mutation...`);

    // Set a timeout to avoid waiting forever
    const timeout = setTimeout(() => {
      console.log(`[${userId}] Timeout waiting for entities to arrive`);

      // Check one more time if we have entities
      const updatedSession = userSessions.get(userId);
      if (updatedSession && updatedSession.entities && updatedSession.entities.length > 0) {
        console.log(`[${userId}] Found ${updatedSession.entities.length} entities after waiting`);
        sendMutationWithEntities(updatedSession.entities);
      } else {
        done(new Error('No entities available for mutation'));
      }
    }, 10000);

    // Set up a one-time listener for the sync message
    const messageHandler = (messageData) => {
      try {
        const message = JSON.parse(messageData.toString());

        if (message.type === 'sync' && message.data && message.data.entities) {
          clearTimeout(timeout);
          const entities = message.data.entities;
          console.log(`[${userId}] Captured ${entities.length} entities from sync message`);

          // Update session
          session.entities = entities;
          userSessions.set(userId, session);

          // Remove the listener
          session.commandSocket.removeEventListener('message', messageHandler);

          // Send the mutation
          sendMutationWithEntities(entities);
        }
      } catch (err) {
        console.error(`[${userId}] Error in sync message handler:`, err);
      }
    };

    // Add the listener to the command socket
    if (session.commandSocket) {
      session.commandSocket.addEventListener('message', messageHandler);
    }

    return;
  }

  // We already have entities, use them directly
  sendMutationWithEntities(session.entities);

  // Function to send mutation using real entities
  function sendMutationWithEntities(entities) {
    console.log(`[${userId}] Preparing mutation with ${entities.length} entities`);

    // Find Node entities
    const nodes = entities.filter(entity => entity.type === 'Node');

    if (nodes.length === 0) {
      console.error(`[${userId}] No Node entities found among ${entities.length} entities`);
      done(new Error('No Node entities found'));
      return;
    }

    // Pick a random node
    const randomNode = nodes[Math.floor(Math.random() * nodes.length)];
    console.log(`[${userId}] Selected node ${randomNode._id} (v${randomNode.version || 1})`);

    if (randomNode.state && randomNode.state.label) {
      console.log(`[${userId}] Node label: ${randomNode.state.label}`);
    }

    // Create mutation command
    const mutationCommand = {
      command: "mutate",
      entity: {
        _id: randomNode._id,
        type: randomNode.type,
        version: randomNode.version || 1,
        state: {
          color: "#" + Math.floor(Math.random()*16777215).toString(16).padStart(6, '0')
        }
      }
    };

    console.log(`[${userId}] Sending mutation command: ${JSON.stringify(mutationCommand)}`);

    try {
      session.commandSocket.send(JSON.stringify(mutationCommand));
      events.emit('counter', 'command.sent', 1);
      events.emit('counter', 'node.mutation.sent', 1);
      done();
    } catch (error) {
      console.error(`[${userId}] Error sending mutation command:`, error);
      events.emit('counter', 'command.error', 1);
      done(error);
    }
  }
}

// Cleanup function for process exit
process.on('SIGINT', () => {
  console.log('Cleaning up connections...');
  for (const [userId, session] of userSessions.entries()) {
    if (session.commandSocket) {
      try {
        session.commandSocket.close();
      } catch (err) {
        console.error(`[${userId}] Error closing command socket:`, err);
      }
    }
    if (session.updateSocket) {
      try {
        session.updateSocket.close();
      } catch (err) {
        console.error(`[${userId}] Error closing update socket:`, err);
      }
    }
  }
  console.log('Cleanup complete');
});