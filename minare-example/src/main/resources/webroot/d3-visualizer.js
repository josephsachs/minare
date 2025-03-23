/**
 * D3-based static graph visualization
 * Optimized for performance with minimal animations and simplified render path
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

    // Data structures - using Maps for O(1) lookups
    this.nodes = [];
    this.links = [];
    this.nodeMap = new Map(); // For efficient node lookup
    this.linkMap = new Map(); // For efficient link lookup

    // Static layout grid parameters
    this.gridColumns = 5;
    this.nodeSpacing = 120;
    this.nodeRadius = 30;

    // Draw elements
    this.svg = null;
    this.graphContainer = null;
    this.tooltip = null;
    this.linkElements = null;
    this.nodeElements = null;

    // Initialize the visualization
    this.initialize();

    // Adding resize handler with debounce
    this.resizeTimer = null;
    window.addEventListener('resize', () => this.handleResize());

    logger.info('D3 graph visualizer initialized (static layout)');
  }

  /**
   * Initialize the D3 visualization
   */
  initialize() {
    try {
      // Create SVG element
      this.svg = this.container.append('svg')
        .attr('width', this.width)
        .attr('height', this.height)
        .attr('class', 'd3-graph');

      // Add basic zoom behavior (no animations)
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

      // Add performance mode indicator
      this.performanceIndicator = this.svg.append('text')
        .attr('x', 10)
        .attr('y', 20)
        .attr('class', 'performance-indicator')
        .style('font-size', '12px')
        .style('fill', '#666')
        .text('Static Layout Mode');

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
    const startTime = performance.now();

    if (!entities || entities.length === 0) {
      // Clear the visualization if no entities
      this.nodes = [];
      this.links = [];
      this.nodeMap.clear();
      this.linkMap.clear();
      this.update();
      return;
    }

    // Process entity data into nodes and links
    this.processEntityData(entities);

    // Update visualization
    this.update();

    const endTime = performance.now();
    const duration = endTime - startTime;

    // Only log if update took significant time
    if (duration > 50) {
      logger.info(`D3 visualization update took ${duration.toFixed(1)}ms for ${this.nodes.length} nodes`);
    }
  }

  /**
   * Process entity data into nodes and links
   * @param {Array} entities - Array of entity objects
   */
  processEntityData(entities) {
    // Store previous node map for comparison
    const prevNodeMap = new Map(this.nodeMap);

    // Reset maps to build new graph
    this.nodeMap.clear();
    this.linkMap.clear();

    // New arrays for nodes and links
    const newNodes = [];
    const newLinks = [];

    // Map for quick lookup
    const entityMap = new Map();

    // Create nodes from entities
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

      // Keep the same position if node existed before
      const prevNode = prevNodeMap.get(entity.id);
      if (prevNode && prevNode.x !== undefined && prevNode.y !== undefined) {
        node.x = prevNode.x;
        node.y = prevNode.y;
      }

      newNodes.push(node);
      entityMap.set(entity.id, node);
      this.nodeMap.set(entity.id, node);
    }

    // Create links from parent-child relationships
    for (const entity of entities) {
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
    }

    // Calculate static positions for any nodes that don't have them
    this.assignStaticPositions(newNodes);

    // Update the class properties
    this.nodes = newNodes;
    this.links = newLinks;
  }

  /**
   * Assign static grid-based positions to nodes
   * @param {Array} nodes - Array of node objects
   */
  assignStaticPositions(nodes) {
    // Only calculate positions for nodes that don't already have them
    const nodesToPosition = nodes.filter(node => node.x === undefined || node.y === undefined);

    if (nodesToPosition.length === 0) return;

    // Calculate grid dimensions
    const nodeCount = nodesToPosition.length;
    const totalNodes = nodes.length;

    // Adjust columns based on total nodes for better layout
    this.gridColumns = Math.ceil(Math.sqrt(totalNodes));
    const rows = Math.ceil(totalNodes / this.gridColumns);

    // Calculate cell size based on container dimensions and node count
    const cellWidth = Math.min(this.width / this.gridColumns, this.nodeSpacing);
    const cellHeight = Math.min(this.height / rows, this.nodeSpacing);

    // Offset to center the grid
    const offsetX = (this.width - (this.gridColumns * cellWidth)) / 2 + cellWidth / 2;
    const offsetY = (this.height - (rows * cellHeight)) / 2 + cellHeight / 2;

    // Assign positions based on grid
    nodesToPosition.forEach((node, index) => {
      const col = index % this.gridColumns;
      const row = Math.floor(index / this.gridColumns);

      node.x = offsetX + col * cellWidth;
      node.y = offsetY + row * cellHeight;
    });
  }

  /**
   * Update the visualization with current nodes and links
   */
  update() {
    // First update links
    this.linkElements = this.graphContainer.selectAll('.link')
      .data(this.links, d => `${d.source}-${d.target}`);

    // Remove old links
    this.linkElements.exit().remove();

    // Add new links
    const linkEnter = this.linkElements.enter()
      .append('line')
      .attr('class', 'link')
      .attr('stroke', config.visualization?.d3?.linkColor || '#999')
      .attr('stroke-width', 1.5)
      .attr('stroke-opacity', config.visualization?.d3?.linkOpacity || 0.6)
      .attr('stroke-dasharray', d => d.type === 'child-to-parent' ? '5,5' : '0');

    this.linkElements = linkEnter.merge(this.linkElements);

    // Update link positions (source/target may be either objects or IDs)
    this.linkElements
      .attr('x1', d => {
        const source = typeof d.source === 'string' ? this.nodeMap.get(d.source) : d.source;
        return source?.x || 0;
      })
      .attr('y1', d => {
        const source = typeof d.source === 'string' ? this.nodeMap.get(d.source) : d.source;
        return source?.y || 0;
      })
      .attr('x2', d => {
        const target = typeof d.target === 'string' ? this.nodeMap.get(d.target) : d.target;
        return target?.x || 0;
      })
      .attr('y2', d => {
        const target = typeof d.target === 'string' ? this.nodeMap.get(d.target) : d.target;
        return target?.y || 0;
      });

    // Handle nodes
    this.nodeElements = this.graphContainer.selectAll('.node')
      .data(this.nodes, d => d.id);

    // Remove nodes that no longer exist
    this.nodeElements.exit().remove();

    // Create new nodes
    const nodeEnter = this.nodeElements.enter()
      .append('g')
      .attr('class', 'node')
      .attr('transform', d => `translate(${d.x || 0},${d.y || 0})`)
      .call(d3.drag()
        .on('start', (event, d) => this.dragstarted(event, d))
        .on('drag', (event, d) => this.dragged(event, d))
        .on('end', (event, d) => this.dragended(event, d)));

    nodeEnter.append('circle')
      .attr('r', this.nodeRadius)
      .attr('fill', d => d.color)
      .on('mouseover', (event, d) => this.showTooltip(event, d))
      .on('mouseout', () => this.hideTooltip());

    nodeEnter.append('text')
      .attr('dy', '0.35em')
      .attr('text-anchor', 'middle')
      .attr('fill', d => this.getTextColor(d.color))
      .text(d => d.label);

    // Merge enter and update selections
    this.nodeElements = nodeEnter.merge(this.nodeElements);

    // Update existing nodes position
    this.nodeElements.attr('transform', d => `translate(${d.x || 0},${d.y || 0})`);

    // Update visual attributes of existing nodes
    this.nodeElements.select('circle')
      .attr('fill', d => d.color);

    this.nodeElements.select('text')
      .text(d => d.label)
      .attr('fill', d => this.getTextColor(d.color));
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
   * Handle drag start event
   */
  dragstarted(event, d) {
    // Store starting position
    d._startX = d.x;
    d._startY = d.y;
  }

  /**
   * Handle drag event
   */
  dragged(event, d) {
    // Update node position directly
    d.x = event.x;
    d.y = event.y;

    // Update node position
    d3.select(event.sourceEvent.target.parentNode)
      .attr('transform', `translate(${d.x},${d.y})`);

    // Update connected links
    this.updateConnectedLinks(d);
  }

  /**
   * Update links connected to the dragged node
   */
  updateConnectedLinks(node) {
    this.linkElements
      .filter(link => {
        const sourceId = typeof link.source === 'string' ? link.source : link.source.id;
        const targetId = typeof link.target === 'string' ? link.target : link.target.id;
        return sourceId === node.id || targetId === node.id;
      })
      .attr('x1', link => {
        const sourceId = typeof link.source === 'string' ? link.source : link.source.id;
        return sourceId === node.id ? node.x : this.nodeMap.get(sourceId)?.x || 0;
      })
      .attr('y1', link => {
        const sourceId = typeof link.source === 'string' ? link.source : link.source.id;
        return sourceId === node.id ? node.y : this.nodeMap.get(sourceId)?.y || 0;
      })
      .attr('x2', link => {
        const targetId = typeof link.target === 'string' ? link.target : link.target.id;
        return targetId === node.id ? node.x : this.nodeMap.get(targetId)?.x || 0;
      })
      .attr('y2', link => {
        const targetId = typeof link.target === 'string' ? link.target : link.target.id;
        return targetId === node.id ? node.y : this.nodeMap.get(targetId)?.y || 0;
      });
  }

  /**
   * Handle drag end event - just update final positions
   */
  dragended(event, d) {
    // Nothing needed here - positions already updated
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
      .duration(100) // Faster transition
      .style('opacity', 0.9);
  }

  /**
   * Hide tooltip
   */
  hideTooltip() {
    this.tooltip
      .transition()
      .duration(100) // Faster transition
      .style('opacity', 0);
  }

  /**
   * Handle window resize with debounce
   */
  handleResize() {
    if (this.resizeTimer) {
      clearTimeout(this.resizeTimer);
    }

    this.resizeTimer = setTimeout(() => {
      const containerElement = document.getElementById(this.containerId);
      if (!containerElement) return;

      this.width = containerElement.getBoundingClientRect().width;
      this.height = containerElement.getBoundingClientRect().height || 500;

      this.svg
        .attr('width', this.width)
        .attr('height', this.height);

      // Recalculate static positions if many nodes
      if (this.nodes.length > 10) {
        this.assignStaticPositions(this.nodes);
        this.update();
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

    if (this.tooltip) {
      this.tooltip.remove();
    }

    if (this.container) {
      this.container.html('');
    }
  }
}

export default D3GraphVisualizer;