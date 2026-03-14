'use strict';

const WebSocket = require('ws');

// Store active user sessions
const userSessions = new Map();

module.exports = {
  setupClient,
  userAction,
  microserviceAction,
  bigTickAction
};

/**
 * Shared connection setup for all client types.
 * Connects command socket, receives connection_confirm, sends sync,
 * connects down socket, receives down_socket_confirm.
 */
function setupClient(context, events, done) {
  const userId = context.vars.userId || `vu_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`;
  context.vars.userId = userId;

  const ws = new WebSocket('ws://localhost:4225/command');

  const connectionTimeout = setTimeout(() => {
    events.emit('counter', 'setup.timeout', 1);
    ws.terminate();
    done(new Error('Command socket connection timeout'));
  }, 10000);

  ws.on('open', () => {
    clearTimeout(connectionTimeout);
    userSessions.set(userId, { commandSocket: ws, entities: [] });
  });

  let setupDone = false;
  function finishSetup(err) {
    if (setupDone) return;
    setupDone = true;
    clearTimeout(connectionTimeout);
    done(err);
  }

  ws.on('message', (data) => {
    try {
      const message = JSON.parse(data.toString());

      if (message.type === 'connection_confirm') {
        const connectionId = message.connectionId;
        const session = userSessions.get(userId);
        session.connectionId = connectionId;
        context.vars.connectionId = connectionId;

        // Send sync request
        ws.send(JSON.stringify({ command: 'sync', timestamp: Date.now() }));
        events.emit('counter', 'setup.command.connected', 1);

        // Now connect down socket
        connectDownSocket(userId, connectionId, context, events, finishSetup);
      } else if (message.type === 'sync' && message.data && message.data.entities) {
        const session = userSessions.get(userId);
        if (session) {
          session.entities = message.data.entities;
        }
      }
    } catch (err) {
      // ignore parse errors on command socket
    }
  });

  ws.on('error', (error) => {
    events.emit('counter', 'setup.command.error', 1);
    finishSetup(error);
  });

  ws.on('close', () => {
    events.emit('counter', 'connection.command.closed', 1);
  });
}

/**
 * Connect the down socket (called internally by setupClient after command socket confirms).
 */
function connectDownSocket(userId, connectionId, context, events, done) {
  const downSocket = new WebSocket('ws://localhost:4226/update');

  const connectionTimeout = setTimeout(() => {
    events.emit('counter', 'setup.down.timeout', 1);
    downSocket.terminate();
    done(new Error('Down socket connection timeout'));
  }, 10000);

  downSocket.on('open', () => {
    downSocket.send(JSON.stringify({ connectionId }));

    const session = userSessions.get(userId);
    if (session) {
      session.downSocket = downSocket;
    }
  });

  downSocket.on('message', (data) => {
    try {
      const message = JSON.parse(data.toString());

      if (message.type === 'down_socket_confirm') {
        clearTimeout(connectionTimeout);
        events.emit('counter', 'setup.down.connected', 1);
        done();
      } else if (message.update && message.update.entities) {
        // Merge updates into session entity cache
        const session = userSessions.get(userId);
        if (session) {
          mergeEntityUpdates(session, message.update.entities);
          events.emit('counter', 'update.received', 1);
        }
      }
    } catch (err) {
      // ignore parse errors on down socket
    }
  });

  downSocket.on('error', (error) => {
    clearTimeout(connectionTimeout);
    events.emit('counter', 'setup.down.error', 1);
    done(error);
  });

  downSocket.on('close', () => {
    events.emit('counter', 'connection.down.closed', 1);
  });
}

/**
 * Merge entity updates from the down socket into the session cache.
 * Keeps versions current so subsequent mutations aren't stale.
 */
function mergeEntityUpdates(session, updateEntities) {
  for (const update of updateEntities) {
    const idx = session.entities.findIndex(e => e._id === update.id);
    if (idx >= 0) {
      if (update.version != null) {
        session.entities[idx].version = update.version;
      }
      if (update.state) {
        session.entities[idx].state = {
          ...session.entities[idx].state,
          ...update.state
        };
      }
    } else {
      // New entity (e.g. from a CREATE)
      session.entities.push({
        _id: update.id,
        type: update.type || 'Node',
        version: update.version || 1,
        state: update.state || {}
      });
    }
  }
}

// ── Client type actions ─────────────────────────────────────────────────

/**
 * User client: picks 1 random entity and mutates its color.
 * Simulates an interactive user editing a small piece of the graph.
 */
function userAction(context, events, done) {
  const session = getSession(context, events, done);
  if (!session) return;

  const nodes = getNodes(session);
  if (nodes.length === 0) {
    events.emit('counter', 'user.no_entities', 1);
    return done();
  }

  const node = nodes[Math.floor(Math.random() * nodes.length)];
  const color = randomColor();

  const command = {
    command: 'mutate',
    entity: {
      _id: node._id,
      type: 'Node',
      version: node.version || 1,
      state: { color }
    }
  };

  try {
    session.commandSocket.send(JSON.stringify(command));
    events.emit('counter', 'user.mutation.sent', 1);
    done();
  } catch (err) {
    events.emit('counter', 'user.mutation.error', 1);
    done(err);
  }
}

/**
 * Microservice client: sends an operationSet with 3-5 MUTATE steps
 * targeting different entities. Uses ABORT failure policy.
 */
function microserviceAction(context, events, done) {
  const session = getSession(context, events, done);
  if (!session) return;

  const nodes = getNodes(session);
  if (nodes.length < 2) {
    events.emit('counter', 'microservice.no_entities', 1);
    return done();
  }

  // Pick 3-5 distinct entities (or as many as available)
  const count = Math.min(3 + Math.floor(Math.random() * 3), nodes.length);
  const shuffled = nodes.slice().sort(() => Math.random() - 0.5);
  const targets = shuffled.slice(0, count);

  const steps = targets.map(node => ({
    action: 'MUTATE',
    entityId: node._id,
    delta: { color: randomColor() }
  }));

  const command = {
    command: 'operationSet',
    failurePolicy: 'ABORT',
    steps
  };

  try {
    session.commandSocket.send(JSON.stringify(command));
    events.emit('counter', 'microservice.opset.sent', 1);
    done();
  } catch (err) {
    events.emit('counter', 'microservice.opset.error', 1);
    done(err);
  }
}

/**
 * Big-tick client: sweeps ALL entities and mutates each one's color slightly.
 * Simulates a periodic full-graph update process.
 */
function bigTickAction(context, events, done) {
  const session = getSession(context, events, done);
  if (!session) return;

  const nodes = getNodes(session);
  if (nodes.length === 0) {
    events.emit('counter', 'bigtick.no_entities', 1);
    return done();
  }

  let sent = 0;
  let errors = 0;

  for (const node of nodes) {
    const color = shiftColor(node.state?.color || '#CCCCCC');
    const command = {
      command: 'mutate',
      entity: {
        _id: node._id,
        type: 'Node',
        version: node.version || 1,
        state: { color }
      }
    };

    try {
      session.commandSocket.send(JSON.stringify(command));
      sent++;
    } catch (err) {
      errors++;
    }
  }

  events.emit('counter', 'bigtick.sweep.sent', 1);
  events.emit('counter', 'bigtick.entity.sent', sent);
  if (errors > 0) {
    events.emit('counter', 'bigtick.entity.error', errors);
  }
  done();
}

// ── Helpers ──────────────────────────────────────────────────────────────

function getSession(context, events, done) {
  const userId = context.vars.userId;
  const session = userSessions.get(userId);

  if (!session || !session.commandSocket || session.commandSocket.readyState !== WebSocket.OPEN) {
    events.emit('counter', 'action.no_session', 1);
    done(new Error('No session available or socket closed'));
    return null;
  }

  return session;
}

function getNodes(session) {
  return session.entities.filter(e =>
    e.type === 'Node' || (e._type && e._type === 'Node')
  );
}

function randomColor() {
  return '#' + Math.floor(Math.random() * 16777215).toString(16).padStart(6, '0');
}

/**
 * Shift a hex color slightly — nudge each RGB channel by a small random amount.
 * Produces a visually subtle change for the big-tick sweep.
 */
function shiftColor(hex) {
  const match = hex.match(/^#?([0-9a-f]{2})([0-9a-f]{2})([0-9a-f]{2})$/i);
  if (!match) return randomColor();

  const shift = () => Math.floor(Math.random() * 21) - 10; // -10 to +10
  const clamp = (v) => Math.max(0, Math.min(255, v));

  const r = clamp(parseInt(match[1], 16) + shift());
  const g = clamp(parseInt(match[2], 16) + shift());
  const b = clamp(parseInt(match[3], 16) + shift());

  return '#' + [r, g, b].map(c => c.toString(16).padStart(2, '0')).join('');
}

// Cleanup on exit
process.on('SIGINT', () => {
  for (const [, session] of userSessions.entries()) {
    try { session.commandSocket?.close(); } catch (_) {}
    try { session.downSocket?.close(); } catch (_) {}
  }
  process.exit(0);
});
