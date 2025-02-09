class TestClient {
    constructor(clientId, serverUrl = 'ws://localhost:8080/ws') {
        this.clientId = clientId;
        this.serverUrl = serverUrl;
        this.entities = [];
        this.ws = null;
        this.connected = false;
    }

    connect() {
        this.ws = new WebSocket(this.serverUrl);

        this.ws.onopen = () => {
            console.log(`Client ${this.clientId} connected`);
            // Send handshake immediately on connection
            const handshake = {
                type: 'handshake',
                clientId: this.clientId,
                timestamp: Date.now()
            };
            this.ws.send(JSON.stringify(handshake));
        };

        this.ws.onmessage = (event) => {
            const data = JSON.parse(event.data);

            if (data.type === 'error') {
                console.error(`Error from server: ${data.message}`);
                if (data.code === 'SYNC_ERROR' || data.code === 'CHECKSUM_MISMATCH') {
                    console.log('Reconnecting due to sync error...');
                    this.reconnect();
                }
                return;
            }

            // Handle state updates
            if (data.type === 'update') {
                this.entities = data.entities;
                console.log(`State updated, new checksum: ${data.checksum}`);
            }
        };

        this.ws.onclose = () => {
            console.log(`Client ${this.clientId} disconnected`);
            this.connected = false;
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };
    }

    reconnect() {
        if (this.ws) {
            this.ws.close();
        }
        setTimeout(() => this.connect(), 1000);
    }

    // Calculate checksum from current state
    calculateChecksum() {
        // Sort entities by clientId for consistent ordering
        const sortedEntities = [...this.entities].sort((a, b) => a.clientId - b.clientId);
        return this.hashString(JSON.stringify(sortedEntities));
    }

    // Simple hash function for testing
    hashString(str) {
        let hash = 0;
        for (let i = 0; i < str.length; i++) {
            const char = str.charCodeAt(i);
            hash = ((hash << 5) - hash) + char;
            hash = hash & hash;
        }
        return hash.toString(16);
    }

    // Send a test message
    sendMessage() {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN) {
            console.error('WebSocket not connected');
            return;
        }

        const message = {
            clientId: this.clientId,
            message: Math.random().toString(36).substring(7), // Random string
            timestamp: Date.now(),
            checksum: this.calculateChecksum()
        };

        this.ws.send(JSON.stringify(message));
    }

    // Start sending messages at a specified rate
    startSending(messagesPerSecond = 1) {
        const interval = 1000 / messagesPerSecond;
        setInterval(() => this.sendMessage(), interval);
    }
}

// Usage example:
// const client = new TestClient(1);
// client.connect();
// client.startSending(10); // 10 messages per second