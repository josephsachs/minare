/**
 * Main application entry point
 *
 * Initializes the Minare Example Client
 */
import config from './config.js';
import store from './store.js';
import logger from './logger.js';
import { connect, disconnect, sendCommand } from './connection.js';
import { GridVisualizer } from './grid-visualizer.js';
import { D3GraphVisualizer } from './d3-visualizer.js';
// Lurker import removed

/**
 * Main application class
 */
class App {
  constructor() {
    // DOM elements
    this.elements = {
      connectBtn: null,
      disconnectBtn: null,
      connectionStatus: null,
      logEntries: null,
      graphContainer: null,
      toggleVisBtn: null,
      simulationContainer: null,
      simulationPanel: null,
      simulationPanelHeader: null
    };

    // Active visualizer instance
    this.visualizer = null;

    // Initialize the application when DOM is ready
    window.addEventListener('load', () => this.init());
  }

  /**
   * Initialize the application
   */
  init() {
    // Get DOM elements
    this.elements.connectBtn = document.getElementById('connectBtn');
    this.elements.disconnectBtn = document.getElementById('disconnectBtn');
    this.elements.connectionStatus = document.getElementById('connectionStatus');
    this.elements.logEntries = document.getElementById('logEntries');
    this.elements.graphContainer = document.getElementById('graph');
    this.elements.toggleVisBtn = document.getElementById('toggleVisBtn');
    this.elements.simulationContainer = document.querySelector('.simulation-panel-content');
    this.elements.simulationPanel = document.querySelector('.simulation-panel');
    this.elements.simulationPanelHeader = document.querySelector('.simulation-panel h2');

    // Initialize logger
    logger.init(this.elements.logEntries);
    logger.info('Application initialized');

    // Set up event listeners
    this.setupEventListeners();

    // Initialize visualization
    this.initVisualization(config.visualization?.defaultType || 'grid');

    // Initialize simulation controls
    this.initSimulationControls();

    // Set up store subscriptions
    this.setupStoreSubscriptions();

    logger.info('Minare Example Client ready');
  }

  /**
   * Set up event listeners
   */
  setupEventListeners() {
    // Connect/disconnect buttons
    this.elements.connectBtn.addEventListener('click', () => this.handleConnect());
    this.elements.disconnectBtn.addEventListener('click', () => this.handleDisconnect());

    // Toggle visualization button
    this.elements.toggleVisBtn.addEventListener('click', () => this.toggleVisualization());

    // Toggle simulation panel
    this.elements.simulationPanelHeader.addEventListener('click', () => this.toggleSimulationPanel());

    logger.info('Event listeners set up');
  }

  /**
   * Toggle the simulation panel collapsed state
   */
  toggleSimulationPanel() {
    this.elements.simulationPanel.classList.toggle('collapsed');
  }

  /**
   * Set up store subscriptions
   */
  setupStoreSubscriptions() {
    // Connection status changes
    store.on('connection.status.changed', (connected) => {
      this.updateConnectionStatus(connected);
    });

    // Entity updates
    store.on('entities.updated', () => {
      this.updateVisualization();
    });
  }

  /**
   * Initialize visualization
   * @param {string} type - Visualization type ('grid' or 'd3')
   */
  initVisualization(type) {
    // Clean up existing visualizer if any
    if (this.visualizer) {
      this.visualizer.destroy();
      this.visualizer = null;
    }

    try {
      if (type === 'd3') {
        // Initialize D3 visualization
        this.visualizer = new D3GraphVisualizer('graph');
        this.elements.toggleVisBtn.textContent = 'Show Grid View';
      } else {
        // Initialize grid visualization
        this.visualizer = new GridVisualizer('graph');
        this.elements.toggleVisBtn.textContent = 'Show D3 Visualization';
      }

      // Store current visualization type
      store.setVisualizationType(type);
      store.setVisualizationInstance(this.visualizer);

      // Update visualization with current entities
      this.updateVisualization();

      logger.info(`Initialized ${type} visualization`);
    } catch (error) {
      logger.error(`Failed to initialize visualization: ${error.message}`);

      // Fall back to grid visualization if D3 fails
      if (type === 'd3') {
        logger.info('Falling back to grid visualization');
        this.initVisualization('grid');
      }
    }
  }

  /**
   * Toggle between grid and D3 visualization
   */
  toggleVisualization() {
    const currentType = store.getVisualizationType();
    const newType = currentType === 'grid' ? 'd3' : 'grid';

    logger.info(`Switching visualization from ${currentType} to ${newType}`);
    this.initVisualization(newType);
  }

  /**
   * Update visualization with current entities
   */
  updateVisualization() {
    if (!this.visualizer) return;

    const entities = store.getEntities();
    this.visualizer.updateData(entities);
  }

  /**
   * Initialize simulation controls
   */
  initSimulationControls() {
    // Clear simulation container
    this.elements.simulationContainer.innerHTML = '';

    // Create container
    const controlsContainer = document.createElement('div');
    controlsContainer.className = 'simulation-controls';
    controlsContainer.style.display = 'flex';
    controlsContainer.style.flexDirection = 'column';
    controlsContainer.style.gap = '10px';

    // Add information text
    const infoText = document.createElement('div');
    infoText.innerHTML = `
      <h3>Simulation Information</h3>
      <p>Simulation controls have been moved to Artillery load tests.</p>
      <p>This panel is kept for development purposes.</p>
    `;

    // Add connection testing buttons
    const connectionTest = document.createElement('div');
    connectionTest.innerHTML = `
      <h3>Connection Testing</h3>
      <div style="margin-bottom: 10px;">
        <button id="testWebsocketBtn" class="sim-button">Test WebSocket</button>
        <button id="clearLogBtn" class="sim-button">Clear Log</button>
      </div>
    `;

    // Add to container
    controlsContainer.appendChild(infoText);
    controlsContainer.appendChild(connectionTest);
    this.elements.simulationContainer.appendChild(controlsContainer);

    // Add event listeners
    document.getElementById('testWebsocketBtn')?.addEventListener('click', () => {
      // Simple test command to verify websocket works
      if (store.isConnected()) {
        sendCommand({ command: 'ping', timestamp: Date.now() });
        logger.info('Sent test ping command');
      } else {
        logger.error('Cannot send test command: Not connected to server');
      }
    });

    document.getElementById('clearLogBtn')?.addEventListener('click', () => {
      logger.clear();
      logger.info('Log cleared');
    });

    // Style for simulation buttons
    const style = document.createElement('style');
    style.textContent = `
      .sim-button {
        margin: 0 5px 0 0;
        padding: 6px 10px;
        background-color: #6c757d;
        color: white;
        border: none;
        border-radius: 4px;
        cursor: pointer;
      }
      .sim-button:hover {
        background-color: #5a6268;
      }
    `;
    document.head.appendChild(style);

    logger.info('Simulation controls initialized');
  }

  /**
   * Handle connect button click
   */
  async handleConnect() {
    this.elements.connectBtn.disabled = true;
    logger.info('Connecting to server...');

    try {
      const success = await connect();

      if (!success) {
        this.elements.connectBtn.disabled = false;
        logger.error('Failed to connect to server');
      }
    } catch (error) {
      this.elements.connectBtn.disabled = false;
      logger.error(`Connection error: ${error.message}`);
    }
  }

  /**
   * Handle disconnect button click
   */
  handleDisconnect() {
    // Disconnect from server
    disconnect();
  }

  /**
   * Update connection status UI
   * @param {boolean} connected - Connection status
   */
  updateConnectionStatus(connected) {
    this.elements.connectionStatus.textContent = `Status: ${connected ? 'Connected' : 'Disconnected'}`;
    this.elements.connectionStatus.className = connected ? 'connected' : 'disconnected';

    this.elements.connectBtn.disabled = connected;
    this.elements.disconnectBtn.disabled = !connected;
  }
}

// Create and export application instance
const app = new App();

export default app;