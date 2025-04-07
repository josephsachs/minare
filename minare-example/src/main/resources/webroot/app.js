/**
 * Main application entry point
 *
 * Initializes the Minare Example Client
 * Optimized for high-frequency updates
 */
import config from './config.js';
import store from './store.js';
import logger from './logger.js';
import { connect, disconnect, sendCommand } from './connection.js';
import { GridVisualizer } from './grid-visualizer.js';
import { D3GraphVisualizer } from './d3-visualizer.js';
import { VisNetworkVisualizer } from './vis-visualizer.js';

/**
 * Main application class
 */
class App {
  constructor() {

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


    this.visualizer = null;


    this.visualizationUpdateTimer = null;
    this.visualizationUpdatePending = false;
    this.VISUALIZATION_DEBOUNCE_MS = 250;


    window.addEventListener('load', () => this.init());
  }

  /**
   * Initialize the application
   */
  init() {

    this.elements.connectBtn = document.getElementById('connectBtn');
    this.elements.disconnectBtn = document.getElementById('disconnectBtn');
    this.elements.connectionStatus = document.getElementById('connectionStatus');
    this.elements.logEntries = document.getElementById('logEntries');
    this.elements.graphContainer = document.getElementById('graph');
    this.elements.toggleVisBtn = document.getElementById('toggleVisBtn');
    this.elements.simulationContainer = document.querySelector('.simulation-panel-content');
    this.elements.simulationPanel = document.querySelector('.simulation-panel');
    this.elements.simulationPanelHeader = document.querySelector('.simulation-panel h2');


    logger.init(this.elements.logEntries);
    logger.info('Application initialized');


    if (typeof vis === 'undefined' || typeof vis.Network !== 'function') {
      throw new Error('Vis.js Network library not loaded properly');
    }

    logger.info('Vis.js Network library detected');


    this.setupEventListeners();


    this.initVisualization(config.visualization?.defaultType || 'grid');


    this.initSimulationControls();


    this.setupStoreSubscriptions();

    logger.info('Minare Example Client ready');
  }

  /**
   * Set up event listeners
   */
  setupEventListeners() {

    this.elements.connectBtn.addEventListener('click', () => this.handleConnect());
    this.elements.disconnectBtn.addEventListener('click', () => this.handleDisconnect());


    this.elements.toggleVisBtn.addEventListener('click', () => this.toggleVisualization());


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

    store.on('connection.status.changed', (connected) => {
      this.updateConnectionStatus(connected);
    });


    store.on('entities.updated', () => {
      this.scheduleVisualizationUpdate();
    });


    store.on('connection.health.warning', (healthInfo) => {
      logger.warn(`Connection may be unhealthy: No messages for ${(healthInfo.timeSinceLastMessage/1000).toFixed(1)}s`);
    });
  }

  /**
   * Schedule a visualization update with debouncing to reduce rendering load
   */
  scheduleVisualizationUpdate() {
    this.visualizationUpdatePending = true;

    if (!this.visualizationUpdateTimer) {
      this.visualizationUpdateTimer = setTimeout(() => {
        if (this.visualizationUpdatePending) {
          this.updateVisualization();
          this.visualizationUpdatePending = false;
        }
        this.visualizationUpdateTimer = null;
      }, this.VISUALIZATION_DEBOUNCE_MS);
    }
  }

  /**
   * Initialize visualization
   * @param {string} type - Visualization type ('grid' or 'vis')
   */
  initVisualization(type) {

    if (this.visualizer) {
      this.visualizer.destroy();
      this.visualizer = null;
    }


    if (type === 'vis') {
      // Initialize Vis.js visualization
      this.visualizer = new VisNetworkVisualizer('graph');
      this.elements.toggleVisBtn.textContent = 'Show Grid View';
    } else {
      // Initialize grid visualization
      this.visualizer = new GridVisualizer('graph');
      this.elements.toggleVisBtn.textContent = 'Show Network Visualization';
    }

    // Store current visualization type
    store.setVisualizationType(type);
    store.setVisualizationInstance(this.visualizer);

    // Update visualization with current entities
    this.updateVisualization();

    logger.info(`Initialized ${type} visualization`);
  }

  /**
   * Toggle between grid and Vis.js visualization
   */
  toggleVisualization() {
    const currentType = store.getVisualizationType();
    const newType = currentType === 'grid' ? 'vis' : 'grid';

    logger.info(`Switching visualization from ${currentType} to ${newType}`);
    this.initVisualization(newType);
  }

  /**
   * Update visualization with current entities
   * This is now called less frequently due to debouncing
   */
  updateVisualization() {
    if (!this.visualizer) return;

    const entities = store.getEntities();

    // Performance tracking for visualization updates
    const startTime = performance.now();

    this.visualizer.updateData(entities);

    const endTime = performance.now();
    const duration = endTime - startTime;

    // Only log if updates take a significant amount of time
    if (duration > 50) {
      logger.info(`Visualization update took ${duration.toFixed(1)}ms for ${entities.length} entities`);
    }
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

    // Add performance monitoring section
    const perfMonitoring = document.createElement('div');
    perfMonitoring.innerHTML = `
      <h3>Performance</h3>
      <div style="margin-bottom: 10px;">
        <button id="toggleThrottlingBtn" class="sim-button">Toggle Update Throttling</button>
        <span id="throttlingStatus">Throttling: Enabled</span>
      </div>
    `;

    // Add to container
    controlsContainer.appendChild(infoText);
    controlsContainer.appendChild(connectionTest);
    controlsContainer.appendChild(perfMonitoring);
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

    // Throttling toggle button
    document.getElementById('toggleThrottlingBtn')?.addEventListener('click', () => {
      // Toggle the visualization debounce time between normal and none
      if (this.VISUALIZATION_DEBOUNCE_MS > 0) {
        this.VISUALIZATION_DEBOUNCE_MS = 0;
        document.getElementById('throttlingStatus').textContent = 'Throttling: Disabled';
        logger.info('Visualization update throttling disabled');
      } else {
        this.VISUALIZATION_DEBOUNCE_MS = 250;
        document.getElementById('throttlingStatus').textContent = 'Throttling: Enabled';
        logger.info('Visualization update throttling enabled');
      }
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