const WebSocket = require('ws');

console.log('Attempting to connect to ws://localhost:8080/ws');

const socket = new WebSocket('ws://localhost:8080/ws');

socket.on('open', () => {
  console.log('Connected to WebSocket');
});

socket.on('message', (data) => {
  try {
    const message = JSON.parse(data.toString());
    console.log('Received message:', message);

    if (message.type === 'connection_confirm') {
      console.log(`Connection confirmed with ID: ${message.connectionId}`);

      // Now try to connect to the update socket
      console.log(`Connecting to update socket with connection ID: ${message.connectionId}`);
      const updateSocket = new WebSocket(`ws://localhost:8080/ws/updates?connection_id=${message.connectionId}`);

      updateSocket.on('open', () => {
        console.log('Update socket connected');
      });

      updateSocket.on('message', (updateData) => {
        try {
          const updateMessage = JSON.parse(updateData.toString());
          console.log('Update socket message:', updateMessage);
        } catch (err) {
          console.error('Error parsing update message:', err);
        }
      });

      updateSocket.on('error', (err) => {
        console.error('Update socket error:', err);
      });

      updateSocket.on('close', (code, reason) => {
        console.log(`Update socket closed: ${code} ${reason}`);
      });

      // Send a test command after a short delay
      setTimeout(() => {
        console.log('Sending test ping command');
        socket.send(JSON.stringify({
          command: 'ping',
          timestamp: Date.now()
        }));
      }, 2000);
    }
  } catch (err) {
    console.error('Error parsing message:', err);
  }
});

socket.on('error', (err) => {
  console.error('WebSocket error:', err);
});

socket.on('close', (code, reason) => {
  console.log(`WebSocket closed: ${code} ${reason}`);
});

// Handle process termination
process.on('SIGINT', () => {
  console.log('Closing WebSocket connections');
  if (socket.readyState === WebSocket.OPEN) {
    socket.close();
  }
  process.exit(0);
});