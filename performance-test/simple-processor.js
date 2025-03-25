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
  const ws = new WebSocket('ws://localhost:4225/command');

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

        // Send a sync request right after getting connection ID
        // This helps get entity data faster
        try {
          console.log(`[${userId}] Sending sync request to command socket`);
          const syncRequest = {
            command: "sync",
            timestamp: Date.now()
          };
          ws.send(JSON.stringify(syncRequest));
          events.emit('counter', 'sync.request.sent', 1);
        } catch (syncErr) {
          console.error(`[${userId}] Error sending sync request:`, syncErr);
        }

        clearTimeout(connectionTimeout);
        done();
      } else if (message.type === 'sync') {
        // Store entity data if we receive it on the command socket
        console.log(`[${userId}] Received sync data on command socket`);
        if (message.data && message.data.entities) {
          const session = userSessions.get(userId) || {};
          session.entities = message.data.entities;
          userSessions.set(userId, session);
          console.log(`[${userId}] Stored ${message.data.entities.length} entities from sync message`);
        }
      } else if (message.type === 'sync_initiated') {
        console.log(`[${userId}] Sync initiated successfully: ${message.success}`);
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
  // Note: No longer using query parameter, will send ID in first message
  const updateSocket = new WebSocket('ws://localhost:4226/update');

  // Set a timeout to detect connection issues
  const connectionTimeout = setTimeout(() => {
    console.error(`[${userId}] Update socket connection timeout`);
    events.emit('counter', 'ws.update.timeout', 1);
    updateSocket.terminate();
    done(new Error('Update socket connection timeout'));
  }, 10000);

  updateSocket.on('open', () => {
    console.log(`[${userId}] Connected to update socket, sending connection ID`);

    // Send connection ID in a message after connection
    try {
      const connectionMessage = {
        connectionId: connectionId
      };
      updateSocket.send(JSON.stringify(connectionMessage));
      console.log(`[${userId}] Sent connection ID to update socket: ${connectionId}`);
    } catch (err) {
      console.error(`[${userId}] Error sending connection ID:`, err);
      clearTimeout(connectionTimeout);
      done(err);
      return;
    }

    // Store update socket in session
    const session = userSessions.get(userId) || {};
    session.updateSocket = updateSocket;
    userSessions.set(userId, session);

    events.emit('counter', 'ws.update.open', 1);
  });

  updateSocket.on('message', (data) => {
    try {
      const message = JSON.parse(data.toString());
      console.log(`[${userId}] Update socket received message type: ${message.type || 'unknown'}`);

      // Check for confirmation message
      if (message.type === 'update_socket_confirm') {
        console.log(`[${userId}] Update socket confirmed for connection ${message.connectionId}`);
        clearTimeout(connectionTimeout);
        events.emit('counter', 'ws.update.success', 1);
        done();
      }
      // Handle updates containing entity changes
      else if (message.update && message.update.entities) {
        console.log(`[${userId}] Received entity updates (${message.update.entities.length} entities)`);
        // Merge updates with existing entities
        const session = userSessions.get(userId) || {};
        if (!session.entities) {
          session.entities = [];
        }

        // Update existing entities or add new ones
        message.update.entities.forEach(updateEntity => {
          const existingIndex = session.entities.findIndex(e => e._id === updateEntity.id);
          if (existingIndex >= 0) {
            // Update existing entity with new version and state
            session.entities[existingIndex].version = updateEntity.version;
            session.entities[existingIndex].state = {
              ...session.entities[existingIndex].state,
              ...updateEntity.state
            };
          } else {
            // Add as new entity
            session.entities.push({
              _id: updateEntity.id,
              version: updateEntity.version,
              state: updateEntity.state,
              type: 'Node' // Assume Node type for simplicity
            });
          }
        });

        userSessions.set(userId, session);
      }
    } catch (err) {
      console.error(`[${userId}] Error handling update socket message:`, err);
    }
  });

  updateSocket.on('error', (error) => {
    console.error(`[${userId}] Update socket error:`, error);
    clearTimeout(connectionTimeout);
    events.emit('counter', 'ws.update.error', 1);
    done(error);
  });

  updateSocket.on('close', (code, reason) => {
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
    console.log(`[${userId}] No entities available yet. Sending sync request...`);

    // Send a sync request to get entities
    try {
      const syncRequest = {
        command: "sync",
        timestamp: Date.now()
      };
      session.commandSocket.send(JSON.stringify(syncRequest));
      console.log(`[${userId}] Sent sync request to command socket`);
    } catch (err) {
      console.error(`[${userId}] Error sending sync request:`, err);
    }

    // Set a timeout to wait for entities
    const waitTimeout = setTimeout(() => {
      console.log(`[${userId}] Timeout waiting for entities`);

      // Check if we have entities now
      const updatedSession = userSessions.get(userId);
      if (updatedSession && updatedSession.entities && updatedSession.entities.length > 0) {
        sendMutationWithEntities(updatedSession.entities);
      } else {
        // Create a dummy node to mutate if we can't get real entities
        console.log(`[${userId}] Creating dummy node for mutation`);
        const dummyNode = {
          _id: `dummy-${Date.now()}`,
          type: 'Node',
          version: 1,
          state: {
            label: 'Dummy Node'
          }
        };
        sendMutationWithEntities([dummyNode]);
      }
    }, 5000);

    return;
  }

  // We already have entities, use them directly
  sendMutationWithEntities(session.entities);

  // Function to send mutation using real entities
  function sendMutationWithEntities(entities) {
    console.log(`[${userId}] Preparing mutation with ${entities.length} entities`);

    // Find Node entities
    const nodes = entities.filter(entity =>
      entity.type === 'Node' ||
      (entity._type && entity._type === 'Node')
    );

    if (nodes.length === 0) {
      console.error(`[${userId}] No Node entities found among ${entities.length} entities`);
      // Try to use any entity instead
      if (entities.length > 0) {
        console.log(`[${userId}] Using first available entity instead`);
        const randomNode = entities[0];
        prepareMutation(randomNode);
      } else {
        done(new Error('No entities found for mutation'));
      }
      return;
    }

    // Pick a random node
    const randomNode = nodes[Math.floor(Math.random() * nodes.length)];
    prepareMutation(randomNode);
  }

  function prepareMutation(node) {
    console.log(`[${userId}] Selected node ${node._id} (v${node.version || 1})`);

    if (node.state && node.state.label) {
      console.log(`[${userId}] Node label: ${node.state.label}`);
    }

    // Create mutation command
    const mutationCommand = {
      command: "mutate",
      entity: {
        _id: node._id,
        type: node.type || 'Node',
        version: node.version || 1,
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