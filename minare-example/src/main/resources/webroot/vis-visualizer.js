/**
 * Vis.js Network-based graph visualization
 * Optimized for performance with efficient batch updates and better memory management
 */
import config from './config.js';
import logger from './logger.js';

export class VisNetworkVisualizer {
  /**
   * Create a Vis.js network visualizer
   * @param {string} containerId - ID of container element
   */
  constructor(containerId) {
    // Fail loudly if vis.js is not available
    if (typeof vis === 'undefined') {
      throw new Error('Vis.js network library is not loaded');
    }

    if (typeof vis.Network !== 'function') {
      throw new Error('Vis.js Network constructor is not available');
    }

    this.containerId = containerId;
    this.container = document.getElementById(containerId);

    if (!this.container) {
      throw new Error(`Container element #${containerId} not found`);
    }

    // Clear any previous content
    this.container.innerHTML = '';

    // Debug container size
    console.log(`Container dimensions: ${this.container.offsetWidth}x${this.container.offsetHeight}`);

    // Initialize data structures
    this.nodes = new vis.DataSet();
    this.edges = new vis.DataSet();
    this.nodeMap = new Map(); // For tracking nodes we've processed
    this.network = null;

    // Configuration options for the network
    this.options = {
      nodes: {
        shape: 'dot',
        size: 20,
        font: {
          size: 14,
          color: '#000000'
        },
        borderWidth: 2
      },
      edges: {
        width: 1,
        color: {
          color: config.visualization?.d3?.linkColor || '#999',
          opacity: config.visualization?.d3?.linkOpacity || 0.6
        },
        arrows: {
          to: { enabled: false },
          from: { enabled: false }
        },
        smooth: {
          enabled: true,
          type: 'continuous',
          roundness: 0.5
        }
      },
      physics: {
        enabled: true,
        barnesHut: {
          gravitationalConstant: -2000,
          centralGravity: 0.3,
          springLength: 95,
          springConstant: 0.04,
          damping: 0.09
        },
        stabilization: {
          enabled: true,
          iterations: 100,
          fit: true
        }
      },
      interaction: {
        tooltipDelay: 200,
        hideEdgesOnDrag: true,
        hover: true
      },
      layout: {
        improvedLayout: true,
        hierarchical: {
          enabled: false
        }
      }
    };

    // Initialize the network
    this.initialize();

    // Add window resize handler
    this.resizeTimer = null;
    window.addEventListener('resize', () => this.handleResize());

    logger.info('Vis.js network visualizer initialized');
  }

  /**
   * Initialize the Vis.js visualization
   */
  initialize() {
    // No try/catch - we want errors to fail loudly for debugging

    // Add a simple debug node to verify the network is working
    this.nodes.add({
      id: 'debug-node',
      label: 'Debug Node',
      color: {
        background: '#FF0000',
        border: '#CC0000'
      }
    });

    // Create the network
    const data = {
      nodes: this.nodes,
      edges: this.edges
    };

    this.network = new vis.Network(this.container, data, this.options);
    console.log('Network created, data:', {
      container: this.container,
      nodesCount: this.nodes.length,
      edgesCount: this.edges.length
    });

    // Add event listeners
    this.network.on('stabilizationProgress', (params) => {
      const progress = Math.round(params.iterations / params.total * 100);
      logger.info(`Network stabilization: ${progress}%`);
    });

    this.network.on('stabilizationIterationsDone', () => {
      logger.info('Network stabilization complete');
      // Disable physics after initial layout to improve performance
      this.network.setOptions({ physics: { enabled: false } });
    });

    // Add performance mode indicator
    this.performanceIndicator = document.createElement('div');
    this.performanceIndicator.className = 'performance-indicator';
    this.performanceIndicator.style.position = 'absolute';
    this.performanceIndicator.style.top = '10px';
    this.performanceIndicator.style.left = '10px';
    this.performanceIndicator.style.fontSize = '12px';
    this.performanceIndicator.style.color = '#666';
    this.performanceIndicator.textContent = 'Vis.js Network Mode';
    this.container.appendChild(this.performanceIndicator);

    // Add physics toggle button
    this.physicsToggle = document.createElement('button');
    this.physicsToggle.className = 'physics-toggle';
    this.physicsToggle.style.position = 'absolute';
    this.physicsToggle.style.top = '10px';
    this.physicsToggle.style.right = '10px';
    this.physicsToggle.style.fontSize = '12px';
    this.physicsToggle.textContent = 'Enable Physics';
    this.physicsToggle.addEventListener('click', () => this.togglePhysics());
    this.container.appendChild(this.physicsToggle);

    // Current physics state
    this.physicsEnabled = false;
  }

  /**
   * Toggle physics simulation
   */
  togglePhysics() {
    this.physicsEnabled = !this.physicsEnabled;
    this.network.setOptions({ physics: { enabled: this.physicsEnabled } });
    this.physicsToggle.textContent = this.physicsEnabled ? 'Disable Physics' : 'Enable Physics';
    logger.info(`Physics simulation ${this.physicsEnabled ? 'enabled' : 'disabled'}`);
  }

  /**
   * Update visualization with new data
   * @param {Array} entities - Array of entity objects
   */
  updateData(entities) {
    const startTime = performance.now();
    console.log(`updateData called with ${entities ? entities.length : 0} entities`);

    if (!entities || entities.length === 0) {
      // Clear the visualization if no entities
      this.nodes.clear();
      this.edges.clear();
      this.nodeMap.clear();
      return;
    }

    // Log a sample entity for debugging
    if (entities.length > 0) {
      console.log('Sample entity:', entities[0]);
    }

    // Process entity data into nodes and edges
    this.processEntityData(entities);

    const endTime = performance.now();
    const duration = endTime - startTime;

    // Always log for debugging
    logger.info(`Vis.js visualization update took ${duration.toFixed(1)}ms for ${this.nodes.length} nodes, ${this.edges.length} edges`);
    console.log(`After update: ${this.nodes.length} nodes, ${this.edges.length} edges`);

    if (this.nodes.length > 0 && this.network) {
      // Force a redraw and fit
      this.network.redraw();
      this.network.fit();
    }
  }

  /**
   * Process entity data into nodes and edges
   * @param {Array} entities - Array of entity objects
   */
  processEntityData(entities) {
    // Track processed entities to determine removals
    const processedNodeIds = new Set();
    const processedEdgeIds = new Set();

    // Maps for quick entity lookup
    const entityMap = new Map();

    // Track metrics for logging
    let addedNodes = 0;
    let updatedNodes = 0;
    let addedEdges = 0;

    // Batch updates for better performance
    const nodeUpdates = [];
    const nodeAdditions = [];
    const edgeUpdates = [];
    const edgeAdditions = [];

    // Process all entities to extract nodes
    for (const entity of entities) {
      // Skip entities without proper data
      if (!entity.id) continue;

      // Try multiple ways to identify nodes - be more permissive
      const hasLabel = entity.state && entity.state.label;
      const hasNodeType = entity.type === 'Node';
      const typeContainsNode = entity.type && typeof entity.type === 'string' &&
                             entity.type.toLowerCase().includes('node');
      const hasColor = entity.state && entity.state.color;

      const isNode = hasLabel || hasNodeType || typeContainsNode || hasColor;

      if (!isNode) continue;

      // Default color from config
      const defaultColor = config.visualization?.d3?.nodeDefaultColor || '#CCCCCC';

      // Mark this node ID as processed
      processedNodeIds.add(entity.id);

      // Add to entity map for edge creation
      entityMap.set(entity.id, entity);

      // Create node data object
      const nodeData = {
        id: entity.id,
        label: entity.state?.label || 'Unknown',
        color: {
          background: entity.state?.color || defaultColor,
          border: this.adjustColor(entity.state?.color || defaultColor, -30)
        },
        title: this.generateTooltip(entity),
        entity: entity  // Store reference to original entity for later use
      };

      // Calculate text color based on background brightness
      const bgColor = entity.state?.color || defaultColor;
      nodeData.font = {
        color: this.getTextColor(bgColor)
      };

      // Check if node already exists
      if (this.nodeMap.has(entity.id)) {
        // Update existing node
        nodeUpdates.push(nodeData);
        updatedNodes++;
      } else {
        // Add new node
        nodeAdditions.push(nodeData);
        addedNodes++;
      }

      // Update node map
      this.nodeMap.set(entity.id, nodeData);
    }

    // Apply node updates in batches for better performance
    if (nodeUpdates.length > 0) {
      this.nodes.update(nodeUpdates);
    }

    if (nodeAdditions.length > 0) {
      this.nodes.add(nodeAdditions);
    }

    // Process relationships to create edges
    for (const entity of entities) {
      if (!entity.id || !entityMap.has(entity.id)) continue;

      // Process parent-child relationships
      if (entity.state?.parentId && entityMap.has(entity.state.parentId)) {
        const edgeId = `${entity.id}-${entity.state.parentId}`;
        processedEdgeIds.add(edgeId);

        const edgeData = {
          id: edgeId,
          from: entity.id,
          to: entity.state.parentId,
          dashes: true
        };

        // Check if edge exists
        if (this.edges.get(edgeId)) {
          // Update existing edge
          edgeUpdates.push(edgeData);
        } else {
          // Add new edge
          edgeAdditions.push(edgeData);
          addedEdges++;
        }
      }

      // Process child references
      if (entity.state?.childIds) {
        const childIds = Array.isArray(entity.state.childIds) ? entity.state.childIds : [];
        childIds.forEach(childId => {
          if (entityMap.has(childId)) {
            const edgeId = `${entity.id}-${childId}`;
            processedEdgeIds.add(edgeId);

            const edgeData = {
              id: edgeId,
              from: entity.id,
              to: childId,
              dashes: false
            };

            // Check if edge exists
            if (this.edges.get(edgeId)) {
              // Update existing edge
              edgeUpdates.push(edgeData);
            } else {
              // Add new edge
              edgeAdditions.push(edgeData);
              addedEdges++;
            }
          }
        });
      }
    }

    // Apply edge updates in batches
    if (edgeUpdates.length > 0) {
      this.edges.update(edgeUpdates);
    }

    if (edgeAdditions.length > 0) {
      this.edges.add(edgeAdditions);
    }

    // Remove nodes and edges that no longer exist
    // Get current node IDs and remove those not in the processed set
    const currentNodeIds = this.nodes.getIds();
    const nodesToRemove = currentNodeIds.filter(id => !processedNodeIds.has(id));

    if (nodesToRemove.length > 0) {
      this.nodes.remove(nodesToRemove);
      // Also remove from our map
      nodesToRemove.forEach(id => this.nodeMap.delete(id));
    }

    // Get current edge IDs and remove those not in the processed set
    const currentEdgeIds = this.edges.getIds();
    const edgesToRemove = currentEdgeIds.filter(id => !processedEdgeIds.has(id));

    if (edgesToRemove.length > 0) {
      this.edges.remove(edgesToRemove);
    }

    // Log update metrics if significant changes
    if (addedNodes > 0 || addedEdges > 0 || nodesToRemove.length > 0 || edgesToRemove.length > 0) {
      logger.info(`Network update: +${addedNodes} nodes, ~${updatedNodes} updated, -${nodesToRemove.length} removed, +${addedEdges} edges, -${edgesToRemove.length} edges removed`);
    }
  }

  /**
   * Generate HTML tooltip content for a node
   * @param {Object} entity - Entity data
   * @returns {string} HTML content for tooltip
   */
  generateTooltip(entity) {
    let content = `<div style="max-width: 300px; padding: 10px;">`;
    content += `<strong>${entity.state?.label || 'Unknown'}</strong><br>`;
    content += `ID: ${entity.id}<br>`;
    content += `Version: ${entity.version}<br>`;

    // Add state properties
    if (entity.state) {
      content += '<hr style="margin: 5px 0;">State:<br>';
      for (const [key, value] of Object.entries(entity.state)) {
        if (key === 'childIds' && Array.isArray(value)) {
          content += `${key}: [${value.length} items]<br>`;
        } else if (typeof value !== 'object') {
          content += `${key}: ${value}<br>`;
        }
      }
    }

    content += '</div>';
    return content;
  }

  /**
   * Calculate text color based on background brightness
   * @param {string} backgroundColor - Background color in hex format
   * @returns {string} Appropriate text color (black or white)
   */
  getTextColor(backgroundColor) {
    const color = backgroundColor || '#CCCCCC';
    const r = parseInt(color.slice(1, 3), 16);
    const g = parseInt(color.slice(3, 5), 16);
    const b = parseInt(color.slice(5, 7), 16);
    const brightness = (r * 299 + g * 587 + b * 114) / 1000;
    return brightness > 128 ? 'black' : 'white';
  }

  /**
   * Adjust a color by a given amount
   * @param {string} color - Hex color
   * @param {number} amount - Amount to adjust (-255 to 255)
   * @returns {string} Adjusted color
   */
  adjustColor(color, amount) {
    const clamp = (val) => Math.max(0, Math.min(255, val));

    // Default color if input is invalid
    if (!color || typeof color !== 'string' || !color.startsWith('#') || color.length !== 7) {
      return '#CCCCCC';
    }

    let r = parseInt(color.slice(1, 3), 16);
    let g = parseInt(color.slice(3, 5), 16);
    let b = parseInt(color.slice(5, 7), 16);

    r = clamp(r + amount);
    g = clamp(g + amount);
    b = clamp(b + amount);

    return `#${r.toString(16).padStart(2, '0')}${g.toString(16).padStart(2, '0')}${b.toString(16).padStart(2, '0')}`;
  }

  /**
   * Handle window resize with debounce
   */
  handleResize() {
    if (this.resizeTimer) {
      clearTimeout(this.resizeTimer);
    }

    this.resizeTimer = setTimeout(() => {
      if (this.network) {
        this.network.redraw();
        this.network.fit();
      }
      this.resizeTimer = null;
    }, 250);
  }

  /**
   * Clean up the visualizer
   */
  destroy() {
    if (this.resizeTimer) {
      clearTimeout(this.resizeTimer);
      this.resizeTimer = null;
    }

    window.removeEventListener('resize', this.handleResize);

    if (this.network) {
      this.network.destroy();
      this.network = null;
    }

    if (this.container) {
      this.container.innerHTML = '';
    }

    // Clear data structures to prevent memory leaks
    this.nodes.clear();
    this.edges.clear();
    this.nodeMap.clear();
  }
}

export default VisNetworkVisualizer;