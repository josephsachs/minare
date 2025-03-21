/**
 * Logging utility for the Minare client
 */
import config from './config.js';
import createEventEmitter from './events.js';

class Logger {
  constructor() {
    this.logContainer = null;
    this.events = createEventEmitter();
    this.logCount = 0;
    this.maxEntries = 1000; // Default, will be updated from config
  }

  /**
   * Initialize the logger with DOM elements
   * @param {HTMLElement} container - Log container element
   */
  init(container) {
    this.logContainer = container;

    // Update from config if available
    if (config.logging && config.logging.maxEntries) {
      this.maxEntries = config.logging.maxEntries;
    }

    return this;
  }

  /**
   * Log a message
   * @param {string} message - Message to log
   * @param {string} type - Log level (info, command, update, error)
   */
  log(message, type = 'info') {
    // Add timestamp
    const timestamp = new Date().toLocaleTimeString();
    const formattedMessage = `[${timestamp}] ${message}`;

    // Log to console if enabled
    if (!config.logging || config.logging.console !== false) {
      switch (type) {
        case 'error':
          console.error(formattedMessage);
          break;
        case 'command':
        case 'update':
          console.info(`[${type}] ${formattedMessage}`);
          break;
        default:
          console.log(formattedMessage);
      }
    }

    // Create log entry for UI
    if (this.logContainer) {
      const entry = document.createElement('div');
      entry.className = `log-entry log-entry-${type}`;
      entry.textContent = formattedMessage;

      // Add to log container
      this.logContainer.appendChild(entry);

      // Auto-scroll to bottom
      this.logContainer.scrollTop = this.logContainer.scrollHeight;

      // Limit log entries
      this.logCount++;
      if (this.logCount > this.maxEntries) {
        this.logContainer.firstChild?.remove();
        this.logCount--;
      }
    }

    // Emit event
    this.events.emit('log', { message, type, timestamp });
  }

  /**
   * Log an info message
   * @param {string} message - Message to log
   */
  info(message) {
    this.log(message, 'info');
  }

  /**
   * Log a command message
   * @param {string} message - Message to log
   */
  command(message) {
    this.log(message, 'command');
  }

  /**
   * Log an update message
   * @param {string} message - Message to log
   */
  update(message) {
    this.log(message, 'update');
  }

  /**
   * Log an error message
   * @param {string} message - Message to log
   */
  error(message) {
    this.log(message, 'error');
  }

  /**
   * Subscribe to log events
   * @param {Function} callback - Function to call when a log is added
   */
  onLog(callback) {
    return this.events.on('log', callback);
  }

  /**
   * Clear all logs
   */
  clear() {
    if (this.logContainer) {
      this.logContainer.innerHTML = '';
      this.logCount = 0;
    }
  }
}

// Create and export singleton instance
const logger = new Logger();

export default logger;