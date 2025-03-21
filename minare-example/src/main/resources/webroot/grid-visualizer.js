/**
 * Grid-based visualization for entity graph
 */
import logger from './logger.js';

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
   * @param {Array} nodes - Array of node objects
   */
  updateData(nodes) {
    if (!nodes || nodes.length === 0) {
      this.renderEmptyMessage();
      return;
    }

    logger.info(`Rendering grid with ${nodes.length} nodes`);
    this.renderGrid(nodes);
  }

  /**
   * Render the grid view
   * @param {Array} nodes - Array of node objects
   */
  renderGrid(nodes) {
    // Clear the container
    this.container.innerHTML = '';

    // Create a flex container for the nodes
    const flexContainer = document.createElement('div');
    flexContainer.style.display = 'flex';
    flexContainer.style.flexWrap = 'wrap';
    flexContainer.style.gap = '20px';
    flexContainer.style.padding = '15px';

    // Counting nodes that actually get rendered
    let renderedCount = 0;

    for (const node of nodes) {
      // Check if it's a Node type - accept either in state or as a direct property
      const isNode = (node.state && node.state.type === 'Node') || node.type === 'Node';

      if (!isNode) continue;

      const nodeElement = document.createElement('div');
      nodeElement.className = 'node';

      // Get color from state if available
      const color = node.state?.color || '#ffffff';
      nodeElement.style.backgroundColor = color;

      // Make text color black or white depending on background brightness
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
    logger.info(`Rendered ${renderedCount} nodes in grid view`);

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