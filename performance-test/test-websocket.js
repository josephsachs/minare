const WebSocket = require('ws');

console.log('Attempting to connect to ws://localhost:4225/command');

const socket = new WebSocket('ws://localhost:4225/command');

socket.on('open', () => {
  console.log('Connected to WebSocket');
});

socket.on('message', (data) => {
  try {
    const message = JSON.parse(data.toString());
    console.log('Received message:', message);

    if (message.type === 'connection_confirm') {
      console.log(`Connection confirmed with ID: ${message.connectionId}`);

      // Now try to connect to the down socket
      console.log(`Connecting to down socket with connection ID: ${message.connectionId}`);
      const downSocket = new WebSocket(`ws://localhost:4226/update?connection_id=${message.connectionId}`);

      downSocket.on('open', () => {
        console.log('Down socket connected');
      });

      downSocket.on('message', (updateData) => {
        try {
          const updateMessage = JSON.parse(updateData.toString());
          console.log('Down socket message:', updateMessage);
        } catch (err) {
          console.error('Error parsing update message:', err);
        }
      });

      downSocket.on('error', (err) => {
        console.error('Down socket error:', err);
      });

      downSocket.on('close', (code, reason) => {
        console.log(`Down socket closed: ${code} ${reason}`);
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