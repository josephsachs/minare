class TestClient {
    constructor(serverUrl = 'ws://localhost:8080/ws') {
        this.serverUrl = serverUrl;
        this.connectionId = null;
        this.ws = null;
        this.connected = false;
        this.messageInterval = null;  // Track the interval
    }

    connect() {
        this.ws = new WebSocket(this.serverUrl);

        this.ws.onopen = () => {
            console.log('WebSocket connected');
            // Send handshake immediately on connection
            const handshake = {
                type: 'handshake',
                timestamp: Date.now()
            };
            this.ws.send(JSON.stringify(handshake));
        };

        this.ws.onmessage = (event) => {
            const data = JSON.parse(event.data);

            if (data.type === 'handshake_confirm') {
                this.connectionId = data.connectionId;
                this.connected = true;
                console.log(`Connection established with ID: ${this.connectionId}`);
            } else if (data.type === 'error') {
                console.error(`Error from server: ${data.message}`);
                if (data.code === 'SYNC_ERROR' || data.code === 'CHECKSUM_MISMATCH') {
                    console.log('Reconnecting due to sync error...');
                    this.reconnect();
                }
            }
        };

        this.ws.onclose = () => {
            console.log('WebSocket disconnected');
            this.connected = false;
            this.connectionId = null;
        };

        this.ws.onerror = (error) => {
            console.error('WebSocket error:', error);
        };
    }

    sendMessage() {
        if (!this.ws || this.ws.readyState !== WebSocket.OPEN || !this.connectionId) {
            console.error('WebSocket not ready or connection ID not received');
            return;
        }

        const message = {
            type: 'update',
            connectionId: this.connectionId,
            message: Math.random().toString(36).substring(7),
            timestamp: Date.now()
        };

        this.ws.send(JSON.stringify(message));
    }

    startSending(messagesPerSecond = 1) {
        const interval = 1000 / messagesPerSecond;
        this.messageInterval = setInterval(() => this.sendMessage(), interval);
    }

    stop() {
        if (this.messageInterval) {
            clearInterval(this.messageInterval);
            this.messageInterval = null;
        }
        if (this.ws) {
            this.ws.close();
        }
    }
}
