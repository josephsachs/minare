/**
 * Grid-based visualization for entity graph
 */
import logger from './logger.js';
import config from './config.js';

export class GridVisualizer {
  /**
   * Create a grid visualizer
   * @param {string} containerId - ID of container element
   */
  constructor(containerId) {
    this.containerId = containerId;
    this.container = document.getElementById(containerId);

    if (!this.container) {
      throw new Error(`Container element #${containerId} not found`);
    }

    logger.info('Grid visualizer initialized');
  }

  /**
   * Update visualization with new data
   * @param {Array} entities - Array of entity objects
   */
  updateData(entities) {
    if (config.logging?.verbose) {
      console.log('GridVisualizer received entities:', entities.length);
    }

    if (!entities || entities.length === 0) {
      this.renderEmptyMessage();
      return;
    }


    const identifiedNodes = entities.filter(entity =>
      (entity.state && entity.state.label) ||
      (entity.type === 'Node') ||
      (entity.type && typeof entity.type === 'string' && entity.type.toLowerCase().includes('node')) ||
      (entity.state && entity.state.color)
    );

    if (config.logging?.verbose) {
      console.log(`GridVisualizer: Found ${identifiedNodes.length} nodes out of ${entities.length} entities`);
    }


    if (identifiedNodes.length === 0 && entities.length > 0 && config.logging?.logDetailedEntities && config.logging?.verbose) {
      console.log('Entity structure examples:');
      console.log('First entity:', JSON.stringify(entities[0]));
      console.log('Entity properties:', Object.keys(entities[0]));
      if (entities[0].state) {
        console.log('State properties:', Object.keys(entities[0].state));
      }
    }

    if (identifiedNodes.length === 0) {
      this.renderEmptyMessage();
      return;
    }

    logger.info(`Rendering grid with ${identifiedNodes.length} nodes`);
    this.renderGrid(entities);
  }

  /**
   * Render the grid view
   * @param {Array} nodes - Array of node objects
   */
  renderGrid(nodes) {

    this.container.innerHTML = '';


    const flexContainer = document.createElement('div');
    flexContainer.style.display = 'flex';
    flexContainer.style.flexWrap = 'wrap';
    flexContainer.style.gap = '20px';
    flexContainer.style.padding = '15px';

    // Counting nodes that actually get rendered
    let renderedCount = 0;
    let skippedCount = 0;

    if (config.logging?.verbose) {
      console.log('renderGrid received nodes:', nodes.length);
    }

    for (const node of nodes) {
      // Try multiple ways to identify nodes - be more permissive
      const hasLabel = node.state && node.state.label;
      const hasNodeType = node.type === 'Node';
      const typeContainsNode = node.type && typeof node.type === 'string' &&
                             node.type.toLowerCase().includes('node');
      const hasColor = node.state && node.state.color;

      const isNode = hasLabel || hasNodeType || typeContainsNode || hasColor;

      if (!isNode) {
        skippedCount++;
        continue;
      }

      const nodeElement = document.createElement('div');
      nodeElement.className = 'node';
      nodeElement.style.backgroundColor = node.state?.color || '#ffffff';

      // Make text color black or white depending on background brightness
      const color = node.state?.color || '#ffffff';
      const r = parseInt(color.slice(1, 3), 16);
      const g = parseInt(color.slice(3, 5), 16);
      const b = parseInt(color.slice(5, 7), 16);
      const brightness = (r * 299 + g * 587 + b * 114) / 1000;
      const textColor = brightness > 128 ? 'black' : 'white';
      nodeElement.style.color = textColor;

      nodeElement.innerHTML = `
        <div class="node-label">${node.state?.label || 'Unnamed Node'}</div>
        <div class="node-content">
          <div>ID: ${node.id}</div>
          <div>Version: ${node.version}</div>
          <div>Color: ${color}</div>
        </div>
      `;

      flexContainer.appendChild(nodeElement);
      renderedCount++;
    }

    this.container.appendChild(flexContainer);
    logger.info(`Rendered ${renderedCount} nodes in grid view (skipped ${skippedCount})`);

    // If no nodes were rendered, show empty message
    if (renderedCount === 0) {
      this.renderEmptyMessage();
    }
  }

  /**
   * Render a message when no nodes are available
   */
  renderEmptyMessage() {
    this.container.innerHTML = '';

    const message = document.createElement('div');
    message.style.padding = '20px';
    message.style.color = '#666';
    message.style.textAlign = 'center';
    message.innerText = 'No nodes available. Connect to the server or toggle visualization to see data.';

    this.container.appendChild(message);
    logger.info('Rendered empty message (no nodes available)');
  }

  /**
   * Clean up the visualizer
   */
  destroy() {
    this.container.innerHTML = '';
  }
}

export default GridVisualizer;