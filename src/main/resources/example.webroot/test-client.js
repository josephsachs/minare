class TestClient {
    constructor(serverUrl = 'ws://localhost:8080/ws') {
        this.serverUrl = serverUrl;
        this.connectionId = null;
        this.ws = null;
        this.connected = false;
        this.messageInterval = null;  // Track the interval
        this.entityId = null;
        this.version = null;
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
            entityType: 'counter',
            entityId: this.entityId,
            state: {
                increment: 1
            },
            connectionId: this.connectionId,
            timestamp: Date.now(),
            version: this.version
        };

        let result = await this.ws.send(JSON.stringify(message));

        console.log(result);
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
