/**
 * D3-based force-directed graph visualization
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
    this.simulation = null;
    this.svg = null;
    this.graphContainer = null;
    this.tooltip = null;
    this.linkElements = null;
    this.nodeElements = null;

    // Initialize the visualization
    this.initialize();

    logger.info('D3 graph visualizer initialized');
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
    // Reset nodes and links
    this.nodes = [];
    this.links = [];

    // Map for quick lookup
    const entityMap = new Map();

    logger.info(`Processing ${entities.length} entities for D3 visualization`);

    // Create nodes from entities
    entities.forEach(entity => {
      // Skip entities without proper data
      if (!entity.id) {
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

      this.nodes.push(node);
      entityMap.set(entity.id, node);
    });

    // Create links from parent-child relationships
    entities.forEach(entity => {
      if (entity.state?.parentId && entityMap.has(entity.state.parentId)) {
        this.links.push({
          source: entity.id,
          target: entity.state.parentId,
          type: 'child-to-parent'
        });
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
          }
        });
      }
    });

    logger.info(`Created ${this.nodes.length} nodes and ${this.links.length} links for visualization`);
  }

  /**
   * Update the visualization with current nodes and links
   */
  update() {
    // Update links
    this.linkElements = this.graphContainer.selectAll('.link')
      .data(this.links, d => `${d.source}-${d.target}`);

    this.linkElements.exit().remove();

    const linkEnter = this.linkElements.enter()
      .append('line')
      .attr('class', 'link')
      .attr('stroke', config.visualization?.d3?.linkColor || '#999')
      .attr('stroke-width', 1.5)
      .attr('stroke-opacity', config.visualization?.d3?.linkOpacity || 0.6)
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
    d.fx = null;
    d.fy = null;
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
      .style('opacity', 0.9);
  }

  /**
   * Hide tooltip
   */
  hideTooltip() {
    this.tooltip.style('opacity', 0);
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
    this.simulation.alpha(1).restart();
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
  }
}

export default D3GraphVisualizer;