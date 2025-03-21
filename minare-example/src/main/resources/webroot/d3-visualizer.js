/**
 * D3-based force-directed graph visualization
 * Optimized for efficient updates
 */
import config from './config.js';
import logger from './logger.js';

export class D3GraphVisualizer {
  /**
   * Create a D3 graph visualizer
   * @param {string} containerId - ID of container element
   */
  constructor(containerId) {
    // Check if D3 is available
    if (typeof d3 === 'undefined') {
      throw new Error('D3 library is not loaded');
    }

    this.containerId = containerId;
    const containerElement = document.getElementById(containerId);

    if (!containerElement) {
      throw new Error(`Container element #${containerId} not found`);
    }

    this.container = d3.select(`#${containerId}`);

    // Clear any previous content
    this.container.html('');

    // Get dimensions
    const rect = containerElement.getBoundingClientRect();
    this.width = rect.width;
    this.height = rect.height || 500; // Use container height or fallback to 500px

    this.nodes = [];
    this.links = [];
    this.nodeMap = new Map(); // For efficient node lookup
    this.linkMap = new Map(); // For efficient link lookup
    this.simulation = null;
    this.svg = null;
    this.graphContainer = null;
    this.tooltip = null;
    this.linkElements = null;
    this.nodeElements = null;
    this.structureChanged = false;

    // Initialize the visualization
    this.initialize();

    logger.info('D3 graph visualizer initialized');
  }

  /**
   * Initialize the D3 visualization
   */
  initialize() {
    try {
      // Track initial render
      this.firstRender = true;

      // Create SVG element
      this.svg = this.container.append('svg')
        .attr('width', this.width)
        .attr('height', this.height)
        .attr('class', 'd3-graph');

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

      // Create tooltip
      this.tooltip = d3.select('body').append('div')
        .attr('class', 'tooltip')
        .style('opacity', 0);

      // Setup force simulation
      const d3Config = config.visualization?.d3 || {};

      this.simulation = d3.forceSimulation()
        .force('link', d3.forceLink().id(d => d.id).distance(d3Config.linkDistance || 100))
        .force('charge', d3.forceManyBody().strength(d3Config.chargeStrength || -300))
        .force('center', d3.forceCenter(this.width / 2, this.height / 2))
        .force('collision', d3.forceCollide().radius(d3Config.collisionRadius || 40))
        .on('tick', () => this.ticked());

      // Disable auto-simulation after initialization
      this.simulation.stop();

      // Setup event listeners
      window.addEventListener('resize', () => this.handleResize());

    } catch (error) {
      logger.error(`Error initializing D3 visualization: ${error.message}`);
      throw error;
    }
  }

  /**
   * Update visualization with new data
   * @param {Array} entities - Array of entity objects
   */
  updateData(entities) {
    if (!entities || entities.length === 0) {
      // Clear the visualization if no entities
      this.nodes = [];
      this.links = [];
      this.nodeMap.clear();
      this.linkMap.clear();
      this.structureChanged = true;
      this.update();
      return;
    }

    // Process entity data into nodes and links
    this.processEntityData(entities);

    // Update visualization
    this.update();
  }

  /**
   * Process entity data into nodes and links
   * @param {Array} entities - Array of entity objects
   */
  processEntityData(entities) {
    // Track old node and link counts to detect structural changes
    const oldNodeCount = this.nodes.length;
    const oldLinksString = JSON.stringify(this.links.map(l => `${l.source}-${l.target}`).sort());

    // Store previous node positions
    const nodePositions = new Map();
    this.nodes.forEach(node => {
      if (node.x !== undefined && node.y !== undefined) {
        nodePositions.set(node.id, { x: node.x, y: node.y });
      }
    });

    // Store previous node map for comparison
    const prevNodeMap = new Map(this.nodeMap);

    // Reset maps
    this.nodeMap.clear();
    this.linkMap.clear();

    // New arrays for nodes and links
    const newNodes = [];
    const newLinks = [];

    logger.info(`Processing ${entities.length} entities for D3 visualization`);

    // Map for quick lookup
    const entityMap = new Map();

    // Create nodes from entities - filter to those with state.label or type='Node'
    entities.forEach(entity => {
      // Skip entities without proper data
      if (!entity.id) {
        return;
      }

      // Check if it's likely a node
      const isNode = (entity.state && entity.state.label) || entity.type === 'Node';
      if (!isNode) {
        return;
      }

      const defaultColor = config.visualization?.d3?.nodeDefaultColor || '#CCCCCC';

      const node = {
        id: entity.id,
        label: entity.state?.label || 'Unknown',
        version: entity.version,
        color: entity.state?.color || defaultColor,
        data: entity,
        // Store type from either state.type or entity.type
        type: entity.state?.type || entity.type
      };

      newNodes.push(node);
      entityMap.set(entity.id, node);
      this.nodeMap.set(entity.id, node);
    });

    // Create links from parent-child relationships
    entities.forEach(entity => {
      if (entity.state?.parentId && entityMap.has(entity.state.parentId)) {
        const link = {
          source: entity.id,
          target: entity.state.parentId,
          type: 'child-to-parent'
        };
        newLinks.push(link);
        this.linkMap.set(`${entity.id}-${entity.state.parentId}`, link);
      }

      if (entity.state?.childIds) {
        const childIds = Array.isArray(entity.state.childIds) ? entity.state.childIds : [];
        childIds.forEach(childId => {
          if (entityMap.has(childId)) {
            const link = {
              source: entity.id,
              target: childId,
              type: 'parent-to-child'
            };
            newLinks.push(link);
            this.linkMap.set(`${entity.id}-${childId}`, link);
          }
        });
      }
    });

    // Determine if structure has changed
    const newNodeCount = newNodes.length;
    const newLinksString = JSON.stringify(newLinks.map(l => `${l.source}-${l.target}`).sort());

    this.structureChanged = oldNodeCount !== newNodeCount || oldLinksString !== newLinksString;

    // If this is the first time we're seeing nodes or if most nodes are new, create a good initial layout
    const percentNewNodes = oldNodeCount === 0 ? 1 : (newNodeCount - oldNodeCount) / newNodeCount;
    const needsInitialLayout = this.firstRender || (this.structureChanged && percentNewNodes > 0.3);

    if (needsInitialLayout) {
      logger.info('Creating initial spread-out layout');

      // Calculate a wide initial layout area
      const totalNodes = newNodes.length;
      const areaPerNode = 7000; // Reduced from 20000 to about 1/3 the spacing
      const totalArea = totalNodes * areaPerNode;
      const layoutWidth = Math.sqrt(totalArea * (this.width / this.height));
      const layoutHeight = totalArea / layoutWidth;

      // Distribute nodes in a grid-like pattern
      const columns = Math.ceil(Math.sqrt(totalNodes));
      const rows = Math.ceil(totalNodes / columns);
      const cellWidth = layoutWidth / columns;
      const cellHeight = layoutHeight / rows;

      // Center this grid in the visible area
      const offsetX = (this.width - layoutWidth) / 2 + cellWidth / 2;
      const offsetY = (this.height - layoutHeight) / 2 + cellHeight / 2;

      newNodes.forEach((node, index) => {
        const col = index % columns;
        const row = Math.floor(index / columns);

        // Add some randomness to the grid to make it look more natural
        const jitter = 0.3; // Percentage of the cell size to jitter
        const jitterX = cellWidth * jitter * (Math.random() - 0.5);
        const jitterY = cellHeight * jitter * (Math.random() - 0.5);

        node.x = offsetX + col * cellWidth + jitterX;
        node.y = offsetY + row * cellHeight + jitterY;
      });

      // No fixed positions yet - let simulation adjust the initial layout
      this.nodes = newNodes;
    } else {
      // Apply previous positions to nodes
      this.nodes = newNodes.map(newNode => {
        // Try to get position from previous positions
        const prevPosition = nodePositions.get(newNode.id);

        if (prevPosition) {
          // If node existed before, preserve its exact position with fixed coordinates
          return {
            ...newNode,
            x: prevPosition.x,
            y: prevPosition.y,
            fx: prevPosition.x, // Force fixed x position
            fy: prevPosition.y  // Force fixed y position
          };
        }

        // For new nodes, they'll get positioned by the simulation
        return newNode;
      });
    }

    // Update links array - handle source/target by id for new nodes
    this.links = newLinks;

    logger.info(`Created ${this.nodes.length} nodes and ${this.links.length} links for visualization`);

    if (this.structureChanged) {
      if (needsInitialLayout) {
        logger.info('Will perform full layout simulation for spread-out positioning');
      } else {
        logger.info('Graph structure has changed, will perform layout for new nodes only');
      }
    } else {
      logger.info('Only node attributes changed, keeping all positions fixed');
    }

    // Remember if we need to do a full layout
    this.needsFullLayout = needsInitialLayout;
  }

  /**
   * Update the visualization with current nodes and links
   */
  update() {
    // Handle links
    this.linkElements = this.graphContainer.selectAll('.link')
      .data(this.links, d => {
        // For consistency, ensure source and target are proper objects
        const source = typeof d.source === 'string' ? this.nodeMap.get(d.source) : d.source;
        const target = typeof d.target === 'string' ? this.nodeMap.get(d.target) : d.target;
        return `${source?.id || d.source}-${target?.id || d.target}`;
      });

    this.linkElements.exit()
      .transition()
      .duration(300)
      .attr('stroke-opacity', 0)
      .remove();

    const linkEnter = this.linkElements.enter()
      .append('line')
      .attr('class', 'link')
      .attr('stroke', config.visualization?.d3?.linkColor || '#999')
      .attr('stroke-width', 1.5)
      .attr('stroke-opacity', 0)
      .attr('stroke-dasharray', d => d.type === 'child-to-parent' ? '5,5' : '0');

    linkEnter.transition()
      .duration(300)
      .attr('stroke-opacity', config.visualization?.d3?.linkOpacity || 0.6);

    this.linkElements = linkEnter.merge(this.linkElements);

    // Handle nodes
    this.nodeElements = this.graphContainer.selectAll('.node')
      .data(this.nodes, d => d.id);

    // Remove nodes that no longer exist
    this.nodeElements.exit()
      .transition()
      .duration(300)
      .attr('opacity', 0)
      .remove();

    // Create new nodes
    const nodeEnter = this.nodeElements.enter()
      .append('g')
      .attr('class', 'node')
      .attr('opacity', 0)
      .attr('transform', d => {
        // Position at current x/y coordinates if available
        return `translate(${d.x || this.width/2},${d.y || this.height/2})`;
      })
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
      .attr('fill', d => this.getTextColor(d.color))
      .text(d => d.label);

    nodeEnter.transition()
      .duration(300)
      .attr('opacity', 1);

    // Merge enter and update selections
    this.nodeElements = nodeEnter.merge(this.nodeElements);

    // Update existing nodes - only update the attributes that change
    this.nodeElements.select('circle')
      .transition()
      .duration(300)
      .attr('fill', d => d.color);

    this.nodeElements.select('text')
      .text(d => d.label)
      .transition()
      .duration(300)
      .attr('fill', d => this.getTextColor(d.color));

    // Configure simulation for new node layout
    this.simulation.nodes(this.nodes);
    this.simulation.force('link').links(this.links);

    if (this.needsFullLayout) {
      // Adjust forces for better spread
      this.simulation.force('charge')
        .strength(-600); // Moderate repulsion (reduced from -1000)

      this.simulation.force('link')
        .distance(100); // Shorter links (reduced from 150)

      // Run simulation longer for better spread
      this.simulation
        .alpha(1)
        .alphaDecay(0.01) // Slower decay for more iterations
        .restart();

      // After layout completes, fix all positions
      this.simulation.on('end', () => {
        this.nodes.forEach(node => {
          // Fix nodes in their final positions
          node.fx = node.x;
          node.fy = node.y;
        });
        // Update visualization with fixed positions
        this.ticked();
      });

      this.needsFullLayout = false;
      this.firstRender = false;
    } else if (this.structureChanged && this.nodeElements.enter().size() > 0) {
      // Only run simulation briefly for new nodes
      this.simulation.alpha(0.3).restart();
    } else {
      // Just render current positions without simulation
      this.ticked();
    }
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
   * Update positions on simulation tick
   */
  ticked() {
    if (!this.linkElements || !this.nodeElements) return;

    this.linkElements
      .attr('x1', d => d.source.x)
      .attr('y1', d => d.source.y)
      .attr('x2', d => d.target.x)
      .attr('y2', d => d.target.y);

    this.nodeElements.attr('transform', d => `translate(${d.x},${d.y})`);
  }

  /**
   * Handle drag start event
   */
  dragstarted(event, d) {
    if (!event.active) this.simulation.alphaTarget(0.3).restart();
    d.fx = d.x;
    d.fy = d.y;
    d.userFixed = true; // Mark as explicitly fixed by user
  }

  /**
   * Handle drag event
   */
  dragged(event, d) {
    d.fx = event.x;
    d.fy = event.y;
  }

  /**
   * Handle drag end event
   */
  dragended(event, d) {
    if (!event.active) this.simulation.alphaTarget(0);
    // Keep fixed at final position instead of releasing
    // d.fx = null;
    // d.fy = null;
  }

  /**
   * Show tooltip for node
   */
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
      .transition()
      .duration(200)
      .style('opacity', 0.9);
  }

  /**
   * Hide tooltip
   */
  hideTooltip() {
    this.tooltip
      .transition()
      .duration(200)
      .style('opacity', 0);
  }

  /**
   * Handle window resize
   */
  handleResize() {
    const containerElement = document.getElementById(this.containerId);
    if (!containerElement) return;

    this.width = containerElement.getBoundingClientRect().width;

    this.svg
      .attr('width', this.width);

    this.simulation.force('center', d3.forceCenter(this.width / 2, this.height / 2));
    this.simulation.alpha(0.5).restart();
  }

  /**
   * Clean up the visualizer
   */
  destroy() {
    window.removeEventListener('resize', this.handleResize);
    if (this.tooltip) {
      this.tooltip.remove();
    }
    if (this.container) {
      this.container.html('');
    }
    if (this.simulation) {
      this.simulation.stop();
    }
  }
}

export default D3GraphVisualizer;