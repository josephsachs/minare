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
import { Lurker } from './lurker.js';

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
      simulationContainer: null
    };

    // Active visualizer instance
    this.visualizer = null;

    // Simulation state
    this.lurkers = [];

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

    logger.info('Event listeners set up');
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

    // Add lurker controls
    const lurkerControls = document.createElement('div');
    lurkerControls.innerHTML = `
      <h3>Lurker Simulation</h3>
      <div style="margin-bottom: 10px;">
        <button id="addLurkerBtn" class="sim-button">Add Lurker</button>
        <button id="removeAllLurkersBtn" class="sim-button">Remove All</button>
      </div>
      <div id="activeLurkers" class="active-lurkers">
        No active lurkers
      </div>
    `;

    // Add to container
    controlsContainer.appendChild(lurkerControls);
    this.elements.simulationContainer.appendChild(controlsContainer);

    // Add event listeners
    document.getElementById('addLurkerBtn').addEventListener('click', () => this.addLurker());
    document.getElementById('removeAllLurkersBtn').addEventListener('click', () => this.removeAllLurkers());

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
      .lurker-item {
        padding: 8px;
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 4px;
        margin-bottom: 5px;
        display: flex;
        justify-content: space-between;
        align-items: center;
      }
      .lurker-status {
        font-size: 0.85em;
        color: #6c757d;
      }
      .active-lurkers {
        max-height: 200px;
        overflow-y: auto;
        color: #666;
      }
    `;
    document.head.appendChild(style);

    logger.info('Simulation controls initialized');
  }

  /**
   * Add a lurker to the simulation
   */
  addLurker() {
    if (!store.isConnected()) {
      logger.error('Cannot add lurker: Not connected to server');
      alert('Please connect to the server first');
      return;
    }

    const lurker = new Lurker();
    this.lurkers.push(lurker);

    // Start the lurker
    lurker.start();

    // Update UI
    this.updateLurkerUI();

    logger.info(`Added lurker: ${lurker.id}`);
  }

  /**
   * Remove all lurkers
   */
  removeAllLurkers() {
    // Stop all lurkers
    for (const lurker of this.lurkers) {
      lurker.stop();
    }

    // Clear the list
    this.lurkers = [];

    // Update UI
    this.updateLurkerUI();

    logger.info('Removed all lurkers');
  }

  /**
   * Update lurker UI
   */
  updateLurkerUI() {
    const container = document.getElementById('activeLurkers');

    if (!container) return;

    if (this.lurkers.length === 0) {
      container.innerHTML = 'No active lurkers';
      return;
    }

    // Create list of lurkers
    const lurkerList = document.createElement('div');

    this.lurkers.forEach((lurker, index) => {
      const status = lurker.getStatus();
      const lurkerItem = document.createElement('div');
      lurkerItem.className = 'lurker-item';

      lurkerItem.innerHTML = `
        <div>
          <strong>Lurker ${index + 1}</strong>
          <span class="lurker-status">
            (${status.active ? 'Active' : 'Inactive'},
            Targets: ${status.targetCount})
          </span>
        </div>
        <button class="sim-button remove-lurker" data-index="${index}">Remove</button>
      `;

      lurkerList.appendChild(lurkerItem);
    });

    // Replace container content
    container.innerHTML = '';
    container.appendChild(lurkerList);

    // Add event listeners to remove buttons
    const removeButtons = container.querySelectorAll('.remove-lurker');
    removeButtons.forEach(button => {
      button.addEventListener('click', (event) => {
        const index = parseInt(event.target.getAttribute('data-index'));
        this.removeLurker(index);
      });
    });
  }

  /**
   * Remove a specific lurker
   * @param {number} index - Lurker index
   */
  removeLurker(index) {
    if (index >= 0 && index < this.lurkers.length) {
      // Stop the lurker
      this.lurkers[index].stop();

      // Remove from array
      this.lurkers.splice(index, 1);

      // Update UI
      this.updateLurkerUI();

      logger.info(`Removed lurker at index ${index}`);
    }
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
    // Stop all lurkers
    for (const lurker of this.lurkers) {
      lurker.stop();
    }

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

    // Update lurker UI
    this.updateLurkerUI();
  }
}

// Create and export application instance
const app = new App();

export default app;