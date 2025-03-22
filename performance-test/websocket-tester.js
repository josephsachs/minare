const WebSocket = require('ws');

console.log('Starting Minare WebSocket connection test');

// Connect to command socket
console.log('Connecting to command socket...');
const commandSocket = new WebSocket('ws://localhost:8080/ws');

commandSocket.on('open', () => {
  console.log('Command socket connected');
});

let connectionId = null;
let updateSocket = null;

commandSocket.on('message', (data) => {
  try {
    const message = JSON.parse(data.toString());
    console.log('Command socket received:', message);

    if (message.type === 'connection_confirm') {
      connectionId = message.connectionId;
      console.log(`Connection confirmed with ID: ${connectionId}`);

      // Connect to update socket after receiving connection ID
      connectUpdateSocket();

      // Send a sync request to get entity data
      setTimeout(() => {
        console.log('Sending sync request');
        commandSocket.send(JSON.stringify({
          command: 'sync',
          timestamp: Date.now()
        }));
      }, 1000);
    }
    else if (message.type === 'sync') {
      console.log('Received sync data:');
      if (message.data && message.data.entities) {
        console.log(`Entities: ${message.data.entities.length}`);

        // After receiving entities, try a mutation
        if (message.data.entities.length > 0) {
          setTimeout(() => {
            sendMutation(message.data.entities[0]);
          }, 1000);
        }
      }
    }
    else if (message.type === 'mutation_success') {
      console.log('Mutation succeeded:', message);
    }
    else if (message.type === 'mutation_error') {
      console.log('Mutation failed:', message);
    }
  } catch (err) {
    console.error('Error parsing command socket message:', err);
  }
});

commandSocket.on('error', (err) => {
  console.error('Command socket error:', err);
});

commandSocket.on('close', (code, reason) => {
  console.log(`Command socket closed: ${code} ${reason}`);
});

function connectUpdateSocket() {
  console.log(`Connecting to update socket...`);
  updateSocket = new WebSocket('ws://localhost:8080/ws/updates');

  updateSocket.on('open', () => {
    console.log('Update socket connected, sending connection ID');
    // Send connection ID message
    updateSocket.send(JSON.stringify({
      connectionId: connectionId
    }));
  });

  updateSocket.on('message', (data) => {
    try {
      const message = JSON.parse(data.toString());
      console.log('Update socket received:', message);
    } catch (err) {
      console.error('Error parsing update socket message:', err);
    }
  });

  updateSocket.on('error', (err) => {
    console.error('Update socket error:', err);
  });

  updateSocket.on('close', (code, reason) => {
    console.log(`Update socket closed: ${code} ${reason}`);
  });
}

function sendMutation(entity) {
  if (!entity || !entity._id) {
    console.log('No valid entity for mutation');
    return;
  }

  console.log(`Sending mutation for entity ${entity._id}`);

  const mutationCommand = {
    command: "mutate",
    entity: {
      _id: entity._id,
      type: entity.type,
      version: entity.version || 1,
      state: {
        color: "#" + Math.floor(Math.random()*16777215).toString(16).padStart(6, '0')
      }
    }
  };

  console.log('Mutation command:', mutationCommand);
  commandSocket.send(JSON.stringify(mutationCommand));
}

// Handle process termination
process.on('SIGINT', () => {
  console.log('Closing WebSocket connections');
  if (commandSocket.readyState === WebSocket.OPEN) {
    commandSocket.close();
  }
  if (updateSocket && updateSocket.readyState === WebSocket.OPEN) {
    updateSocket.close();
  }
  process.exit(0);
});