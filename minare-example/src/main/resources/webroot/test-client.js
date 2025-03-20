// State management
const state = {
    connectionId: null,
    commandSocket: null,
    updateSocket: null,
    entities: {},
    connected: false
};

// DOM elements
const connectBtn = document.getElementById('connectBtn');
const disconnectBtn = document.getElementById('disconnectBtn');
const connectionStatus = document.getElementById('connectionStatus');
const graphContainer = document.getElementById('graph');
const logEntries = document.getElementById('logEntries');

// Logging utility
function log(message, type = 'info') {
    const entry = document.createElement('div');
    entry.className = `log-entry log-entry-${type}`;
    entry.textContent = `[${new Date().toLocaleTimeString()}] ${message}`;
    logEntries.appendChild(entry);
    logEntries.scrollTop = logEntries.scrollHeight;
}

// Connect to the server
function connect() {
    try {
        // Create command socket
        const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsBaseUrl = `${wsProtocol}//${window.location.host}`;
        
        state.commandSocket = new WebSocket(`${wsBaseUrl}/ws`);
        
        state.commandSocket.onopen = () => {
            log('Command socket connected', 'command');
        };
        
        state.commandSocket.onmessage = handleCommandSocketMessage;
        
        state.commandSocket.onerror = (error) => {
            log(`Command socket error: ${error}`, 'error');
            updateConnectionStatus(false);
        };
        
        state.commandSocket.onclose = () => {
            log('Command socket closed', 'command');
            if (state.connected) disconnect();
        };
        
        connectBtn.disabled = true;
    } catch (error) {
        log(`Connection error: ${error.message}`, 'error');
    }
}

// Handle messages from the command socket
function handleCommandSocketMessage(event) {
    try {
        const message = JSON.parse(event.data);
        log(`Command socket received: ${JSON.stringify(message)}`, 'command');
        
        if (message.type === 'connection_confirm') {
            state.connectionId = message.connectionId;
            log(`Connection established with ID: ${state.connectionId}`, 'command');
            
            // Now connect the update socket
            connectUpdateSocket();
        }
        
    } catch (error) {
        log(`Error processing command message: ${error.message}`, 'error');
    }
}

// Connect the update socket
function connectUpdateSocket() {
    try {
        const wsProtocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        const wsBaseUrl = `${wsProtocol}//${window.location.host}`;
        
        state.updateSocket = new WebSocket(`${wsBaseUrl}/ws/updates`);
        
        state.updateSocket.onopen = () => {
            log('Update socket connected', 'update');
            
            // Send connection ID to associate the update socket
            const associationMessage = {
                connectionId: state.connectionId
            };
            state.updateSocket.send(JSON.stringify(associationMessage));
        };
        
        state.updateSocket.onmessage = handleUpdateSocketMessage;
        
        state.updateSocket.onerror = (error) => {
            log(`Update socket error: ${error}`, 'error');
        };
        
        state.updateSocket.onclose = () => {
            log('Update socket closed', 'update');
            if (state.connected) disconnect();
        };
        
    } catch (error) {
        log(`Update socket connection error: ${error.message}`, 'error');
    }
}

// Handle messages from the update socket
function handleUpdateSocketMessage(event) {
    try {
        const message = JSON.parse(event.data);
        log(`Update socket received: ${JSON.stringify(message)}`, 'update');
        
        if (message.type === 'update_socket_confirm') {
            // Update socket confirmed, we're fully connected
            updateConnectionStatus(true);
        } else if (message.update && message.update.entities) {
            // Process entity updates
            processEntityUpdates(message.update.entities);
        }
        
    } catch (error) {
        log(`Error processing update message: ${error.message}`, 'error');
    }
}

// Process entity updates from the server
function processEntityUpdates(entities) {
    log(`Processing ${entities.length} entity updates`, 'update');
    
    for (const entity of entities) {
        state.entities[entity.id] = {
            id: entity.id,
            version: entity.version,
            state: entity.state
        };
    }
    
    // Refresh the displayed graph
    renderGraph();
}

// Render the entity graph
function renderGraph() {
    graphContainer.innerHTML = '';
    
    // Convert entities object to array for rendering
    const nodes = Object.values(state.entities);
    
    log(`Rendering graph with ${nodes.length} nodes`, 'info');
    
    for (const node of nodes) {
        if (!node.state || node.state.type !== 'Node') continue;
        
        const nodeElement = document.createElement('div');
        nodeElement.className = 'node';
        nodeElement.style.backgroundColor = node.state.color || '#ffffff';
        
        // Make text color black or white depending on background brightness
        const color = node.state.color || '#ffffff';
        const r = parseInt(color.slice(1, 3), 16);
        const g = parseInt(color.slice(3, 5), 16);
        const b = parseInt(color.slice(5, 7), 16);
        const brightness = (r * 299 + g * 587 + b * 114) / 1000;
        const textColor = brightness > 128 ? 'black' : 'white';
        nodeElement.style.color = textColor;
        
        nodeElement.innerHTML = `
            <div class="node-label">Node ${node.state.label}</div>
            <div class="node-content">
                <div>ID: ${node.id}</div>
                <div>Version: ${node.version}</div>
                <div>Color: ${node.state.color}</div>
            </div>
        `;
        
        graphContainer.appendChild(nodeElement);
    }
}

// Disconnect from the server
function disconnect() {
    if (state.updateSocket) {
        state.updateSocket.close();
        state.updateSocket = null;
    }
    
    if (state.commandSocket) {
        state.commandSocket.close();
        state.commandSocket = null;
    }
    
    state.connectionId = null;
    state.connected = false;
    state.entities = {};
    
    updateConnectionStatus(false);
    renderGraph();
}

// Update connection status UI
function updateConnectionStatus(connected) {
    state.connected = connected;
    
    connectionStatus.textContent = `Status: ${connected ? 'Connected' : 'Disconnected'}`;
    connectionStatus.className = connected ? 'connected' : 'disconnected';
    
    connectBtn.disabled = connected;
    disconnectBtn.disabled = !connected;
    
    if (connected) {
        log('Fully connected to server', 'info');
    } else {
        log('Disconnected from server', 'info');
    }
}

// Set up event listeners once the DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    connectBtn.addEventListener('click', connect);
    disconnectBtn.addEventListener('click', disconnect);
});