// State management
const state = {
    connectionId: null,
    commandSocket: null,
    updateSocket: null,
    entities: {},
    connected: false,
    graphVisualization: null
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
    // Convert entities object to array for rendering and D3 visualization
    const nodes = Object.values(state.entities);

    log(`Rendering graph with ${nodes.length} nodes`, 'info');
    console.log(`Rendering graph with ${nodes.length} nodes`);

    // Update D3 visualization if available
    if (state.graphVisualization) {
        try {
            console.log('Using D3 visualization');
            state.graphVisualization.updateData(nodes);
        } catch (e) {
            console.error('Error updating D3 visualization:', e);
            log('Error in D3 visualization: ' + e.message, 'error');

            // If D3 visualization fails, fall back to grid view
            state.graphVisualization = null;
            document.getElementById('toggleVisBtn').textContent = 'Show D3 Visualization';
            renderGridGraph(nodes);
        }
    } else {
        console.log('Using grid visualization');
        // Fall back to the original grid visualization
        renderGridGraph(nodes);
    }
}

// Original grid-based graph rendering
function renderGridGraph(nodes) {
    console.log(`Rendering grid graph with ${nodes.length} nodes`);

    // Make sure to completely clear the container to remove any D3 elements
    const container = document.getElementById('graph');
    container.innerHTML = '';

    // Create a flex container for the nodes
    const flexContainer = document.createElement('div');
    flexContainer.style.display = 'flex';
    flexContainer.style.flexWrap = 'wrap';
    flexContainer.style.gap = '20px';
    flexContainer.style.padding = '15px';

    // Counting nodes that actually get rendered
    let renderedCount = 0;

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
            <div class="node-label">${node.state.label}</div>
            <div class="node-content">
                <div>ID: ${node.id}</div>
                <div>Version: ${node.version}</div>
                <div>Color: ${node.state.color}</div>
            </div>
        `;

        flexContainer.appendChild(nodeElement);
        renderedCount++;
    }

    container.appendChild(flexContainer);
    console.log(`Rendered ${renderedCount} nodes in grid view`);

    // If no nodes were rendered, add a message
    if (renderedCount === 0) {
        const message = document.createElement('div');
        message.style.padding = '20px';
        message.style.color = '#666';
        message.style.textAlign = 'center';
        message.innerText = 'No nodes available. Connect to the server or toggle to D3 view to see sample data.';
        container.appendChild(message);
    }
}

// D3 Force-Directed Graph Visualization
class D3GraphVisualizer {
    constructor(containerId) {
        console.log(`Initializing D3GraphVisualizer for container: ${containerId}`);

        // Verify D3 is available
        if (typeof d3 === 'undefined') {
            throw new Error('D3 library is not loaded');
        }

        this.containerId = containerId;
        const containerElement = document.getElementById(containerId);
        if (!containerElement) {
            throw new Error(`Container element #${containerId} not found`);
        }

        console.log(`Container found: ${containerElement.tagName}`);

        this.container = d3.select(`#${containerId}`);

        // Clear any previous content
        this.container.html('');

        // Get dimensions
        const rect = containerElement.getBoundingClientRect();
        this.width = rect.width;
        this.height = rect.height || 500; // Use container height or fallback to 500px

        console.log(`Container dimensions: ${this.width}x${this.height}`);

        this.nodes = [];
        this.links = [];
        this.simulation = null;
        this.svg = null;
        this.tooltip = null;

        // Start initialization
        this.initialize();
    }

    initialize() {
        console.log('Initializing D3 visualization components');

        try {
            // Create SVG element
            this.svg = this.container.append('svg')
                .attr('width', this.width)
                .attr('height', this.height)
                .attr('class', 'd3-graph');

            console.log('SVG element created');

            // Add zoom behavior
            const zoom = d3.zoom()
                .scaleExtent([0.1, 4])
                .on('zoom', (event) => {
                    this.graphContainer.attr('transform', event.transform);
                });

            this.svg.call(zoom);

            // Create a container for zoom
            this.graphContainer = this.svg.append('g')
                .attr('class', 'graph-container');

            console.log('Graph container created');

            // Create tooltip
            this.tooltip = d3.select('body').append('div')
                .attr('class', 'tooltip')
                .style('position', 'absolute')
                .style('padding', '10px')
                .style('background', 'rgba(255, 255, 255, 0.9)')
                .style('border', '1px solid #ddd')
                .style('border-radius', '5px')
                .style('pointer-events', 'none')
                .style('font-size', '12px')
                .style('opacity', 0);

            console.log('Tooltip created');

            // Setup force simulation
            this.simulation = d3.forceSimulation()
                .force('link', d3.forceLink().id(d => d.id).distance(100))
                .force('charge', d3.forceManyBody().strength(-300))
                .force('center', d3.forceCenter(this.width / 2, this.height / 2))
                .force('collision', d3.forceCollide().radius(40))
                .on('tick', () => this.ticked());

            console.log('Force simulation created');

            // Setup event listeners
            window.addEventListener('resize', () => this.handleResize());

            console.log('D3 visualization initialization complete');
        } catch (e) {
            console.error('Error during D3 initialization:', e);
            throw e;
        }
    }

    updateData(entities) {
        if (!entities || entities.length === 0) return;

        // Convert entities to D3 format
        this.processEntityData(entities);

        // Update visualization
        this.update();
    }

    processEntityData(entities) {
        // Reset nodes and links
        this.nodes = [];
        this.links = [];

        // Map for quick lookup
        const entityMap = new Map();

        console.log(`Processing ${entities.length} entities for D3 visualization`);
        log(`Processing ${entities.length} entities for visualization`, 'info');

        // Log all entity IDs and types for debugging
        console.log("All entity IDs:", entities.map(e => e.id).join(", "));

        // Check for entity structure issues
        const entitiesWithoutState = entities.filter(e => !e.state);
        if (entitiesWithoutState.length > 0) {
            console.warn(`Found ${entitiesWithoutState.length} entities without state property`, entitiesWithoutState);
        }

        // Create nodes from entities
        let skippedEntities = 0;
        entities.forEach(entity => {
            // Skip entities without proper data
            if (!entity.id) {
                console.warn("Skipping entity without ID", entity);
                skippedEntities++;
                return;
            }

            const node = {
                id: entity.id,
                label: entity.state?.label || 'Unknown',
                version: entity.version,
                color: entity.state?.color || '#CCCCCC',
                data: entity
            };

            this.nodes.push(node);
            entityMap.set(entity.id, node);
        });

        if (skippedEntities > 0) {
            console.warn(`Skipped ${skippedEntities} entities due to missing ID`);
        }

        // Create links from parent-child relationships
        let childToParentLinks = 0;
        let parentToChildLinks = 0;

        entities.forEach(entity => {
            if (entity.state?.parentId && entityMap.has(entity.state.parentId)) {
                this.links.push({
                    source: entity.id,
                    target: entity.state.parentId,
                    type: 'child-to-parent'
                });
                childToParentLinks++;
            }

            if (entity.state?.childIds) {
                const childIds = Array.isArray(entity.state.childIds) ? entity.state.childIds : [];
                childIds.forEach(childId => {
                    if (entityMap.has(childId)) {
                        this.links.push({
                            source: entity.id,
                            target: childId,
                            type: 'parent-to-child'
                        });
                        parentToChildLinks++;
                    } else {
                        console.warn(`Child ID ${childId} referenced by ${entity.id} does not exist in the entity map`);
                    }
                });
            }
        });

        console.log(`Created ${this.nodes.length} nodes and ${this.links.length} links (${childToParentLinks} child→parent, ${parentToChildLinks} parent→child)`);
        log(`Processed ${this.nodes.length} nodes and ${this.links.length} links for visualization`, 'info');
    }

    update() {
        // Update links
        this.linkElements = this.graphContainer.selectAll('.link')
            .data(this.links, d => `${d.source}-${d.target}`);

        this.linkElements.exit().remove();

        const linkEnter = this.linkElements.enter()
            .append('line')
            .attr('class', 'link')
            .attr('stroke', '#999')
            .attr('stroke-width', 1.5)
            .attr('stroke-opacity', 0.6)
            .attr('stroke-dasharray', d => d.type === 'child-to-parent' ? '5,5' : '0');

        this.linkElements = linkEnter.merge(this.linkElements);

        // Update nodes
        this.nodeElements = this.graphContainer.selectAll('.node')
            .data(this.nodes, d => d.id);

        this.nodeElements.exit().remove();

        const nodeEnter = this.nodeElements.enter()
            .append('g')
            .attr('class', 'node')
            .call(d3.drag()
                .on('start', (event, d) => this.dragstarted(event, d))
                .on('drag', (event, d) => this.dragged(event, d))
                .on('end', (event, d) => this.dragended(event, d)));

        nodeEnter.append('circle')
            .attr('r', 30)
            .attr('fill', d => d.color)
            .on('mouseover', (event, d) => this.showTooltip(event, d))
            .on('mouseout', () => this.hideTooltip());

        nodeEnter.append('text')
            .attr('dy', '0.35em')
            .attr('text-anchor', 'middle')
            .attr('fill', d => {
                // Determine text color based on background brightness
                const color = d.color || '#CCCCCC';
                const r = parseInt(color.slice(1, 3), 16);
                const g = parseInt(color.slice(3, 5), 16);
                const b = parseInt(color.slice(5, 7), 16);
                const brightness = (r * 299 + g * 587 + b * 114) / 1000;
                return brightness > 128 ? 'black' : 'white';
            })
            .text(d => d.label);

        this.nodeElements = nodeEnter.merge(this.nodeElements);

        // Update simulation
        this.simulation.nodes(this.nodes);
        this.simulation.force('link').links(this.links);
        this.simulation.alpha(1).restart();
    }

    ticked() {
        if (!this.linkElements || !this.nodeElements) return;

        this.linkElements
            .attr('x1', d => d.source.x)
            .attr('y1', d => d.source.y)
            .attr('x2', d => d.target.x)
            .attr('y2', d => d.target.y);

        this.nodeElements.attr('transform', d => `translate(${d.x},${d.y})`);
    }

    dragstarted(event, d) {
        if (!event.active) this.simulation.alphaTarget(0.3).restart();
        d.fx = d.x;
        d.fy = d.y;
    }

    dragged(event, d) {
        d.fx = event.x;
        d.fy = event.y;
    }

    dragended(event, d) {
        if (!event.active) this.simulation.alphaTarget(0);
        d.fx = null;
        d.fy = null;
    }

    showTooltip(event, d) {
        const entity = d.data;
        let content = `<strong>${d.label}</strong><br>ID: ${d.id}<br>Version: ${d.version}<br>`;

        // Add state properties
        if (entity.state) {
            content += '<hr>State:<br>';
            for (const [key, value] of Object.entries(entity.state)) {
                if (key === 'childIds' && Array.isArray(value)) {
                    content += `${key}: [${value.length} items]<br>`;
                } else if (typeof value !== 'object') {
                    content += `${key}: ${value}<br>`;
                }
            }
        }

        this.tooltip.html(content)
            .style('left', (event.pageX + 10) + 'px')
            .style('top', (event.pageY - 10) + 'px')
            .style('opacity', 0.9);
    }

    hideTooltip() {
        this.tooltip.style('opacity', 0);
    }

    handleResize() {
        this.width = this.container.node().getBoundingClientRect().width;

        this.svg
            .attr('width', this.width);

        this.simulation.force('center', d3.forceCenter(this.width / 2, this.height / 2));
        this.simulation.alpha(1).restart();
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

// Toggle between grid view and D3 visualization
function toggleVisualization() {
    if (state.graphVisualization) {
        // Switch to grid view
        state.graphVisualization = null;
        document.getElementById('toggleVisBtn').textContent = 'Show D3 Visualization';
        renderGraph();
    } else {
        // Switch to D3 visualization
        state.graphVisualization = new D3GraphVisualizer('graph');
        document.getElementById('toggleVisBtn').textContent = 'Show Grid View';
        renderGraph();
    }
}

// Add controls div for visualization options
function addVisualizationControls() {
    const controlsDiv = document.createElement('div');
    controlsDiv.className = 'visualization-controls';
    controlsDiv.style.marginBottom = '10px';

    const toggleBtn = document.createElement('button');
    toggleBtn.id = 'toggleVisBtn';
    toggleBtn.textContent = 'Show D3 Visualization';
    toggleBtn.addEventListener('click', toggleVisualization);

    controlsDiv.appendChild(toggleBtn);

    // Insert controls before the graph container
    graphContainer.parentNode.insertBefore(controlsDiv, graphContainer);
}

// Add debugging functions
function debugD3Availability() {
    if (typeof d3 === 'undefined') {
        log('DEBUGGING: D3 is not available', 'error');
        console.error('D3 is not available');
        return false;
    } else {
        log('DEBUGGING: D3 is available (version ' + d3.version + ')', 'info');
        console.log('D3 is available (version ' + d3.version + ')');
        return true;
    }
}

// Add debug version of toggleVisualization with console logging
function toggleVisualization() {
    console.log('Toggle visualization called');
    log('Toggling visualization mode', 'info');

    // Check if D3 is available
    if (typeof d3 === 'undefined') {
        log('Cannot initialize D3 visualization: D3 library not loaded', 'error');
        console.error('D3 library not loaded');
        return;
    }

    if (state.graphVisualization) {
        // Switch to grid view
        console.log('Switching to grid view');
        state.graphVisualization = null;
        document.getElementById('toggleVisBtn').textContent = 'Show D3 Visualization';
        renderGraph();
    } else {
        // Switch to D3 visualization
        console.log('Initializing D3 visualization');
        try {
            state.graphVisualization = new D3GraphVisualizer('graph');
            document.getElementById('toggleVisBtn').textContent = 'Show Grid View';

            // If we have entities, update the visualization
            if (Object.keys(state.entities).length > 0) {
                console.log(`Updating visualization with ${Object.keys(state.entities).length} entities`);
                renderGraph();
            } else {
                console.log('No entities available yet');
                // Load some mock data for testing
                const mockEntities = createMockEntities();
                for (const entity of mockEntities) {
                    state.entities[entity.id] = entity;
                }
                console.log(`Created ${mockEntities.length} mock entities`);
                renderGraph();
            }
        } catch (e) {
            console.error('Error initializing D3 visualization:', e);
            log('Error initializing D3 visualization: ' + e.message, 'error');
            state.graphVisualization = null;
        }
    }
}

// Create mock data for testing visualization
function createMockEntities() {
    return [
        {
            id: '1',
            version: 1,
            state: {
                label: 'Root',
                type: 'Node',
                childIds: ['2', '3', '4'],
                color: '#CCCCCC'
            }
        },
        {
            id: '2',
            version: 1,
            state: {
                label: 'Root-1',
                type: 'Node',
                parentId: '1',
                childIds: ['5'],
                color: '#CCCCCC'
            }
        },
        {
            id: '3',
            version: 1,
            state: {
                label: 'Root-2',
                type: 'Node',
                parentId: '1',
                childIds: ['6', '7'],
                color: '#CCCCCC'
            }
        },
        {
            id: '4',
            version: 1,
            state: {
                label: 'Root-3',
                type: 'Node',
                parentId: '1',
                childIds: ['8', '9'],
                color: '#CCCCCC'
            }
        },
        {
            id: '5',
            version: 1,
            state: {
                label: 'Root-1-1',
                type: 'Node',
                parentId: '2',
                childIds: ['10'],
                color: '#CCCCCC'
            }
        },
        {
            id: '6',
            version: 1,
            state: {
                label: 'Root-2-1',
                type: 'Node',
                parentId: '3',
                color: '#CCCCCC'
            }
        },
        {
            id: '7',
            version: 1,
            state: {
                label: 'Root-2-2',
                type: 'Node',
                parentId: '3',
                color: '#CCCCCC'
            }
        },
        {
            id: '8',
            version: 1,
            state: {
                label: 'Root-3-1',
                type: 'Node',
                parentId: '4',
                color: '#CCCCCC'
            }
        },
        {
            id: '9',
            version: 1,
            state: {
                label: 'Root-3-2',
                type: 'Node',
                parentId: '4',
                color: '#CCCCCC'
            }
        },
        {
            id: '10',
            version: 1,
            state: {
                label: 'Root-1-1-1',
                type: 'Node',
                parentId: '5',
                color: '#CCCCCC'
            }
        }
    ];
}

// Initialize everything only after DOM is fully loaded
window.addEventListener('load', () => {
    log('Page fully loaded', 'info');

    // Check D3 availability
    debugD3Availability();

    // Initialize button event listeners
    connectBtn.addEventListener('click', connect);
    disconnectBtn.addEventListener('click', disconnect);

    // Initialize toggle button (now directly in HTML)
    document.getElementById('toggleVisBtn').addEventListener('click', toggleVisualization);

    // Add styles for D3 visualization
    const style = document.createElement('style');
    style.textContent = `
        .d3-graph {
            background-color: #f9f9f9;
            border: 1px solid #ddd;
            border-radius: 5px;
        }
        .link {
            stroke: #999;
            stroke-opacity: 0.6;
        }
    `;
    document.head.appendChild(style);

    // Log ready message
    log('Client initialized and ready', 'info');
});