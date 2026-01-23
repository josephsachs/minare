/**
 * Logging utility for the Minare client
 * Enhanced with buffer and rate limiting
 */
import config from './config.js';
import createEventEmitter from './events.js';

class Logger {
  constructor() {
    this.logContainer = null;
    this.events = createEventEmitter();
    this.logCount = 0;
    this.maxEntries = 1000;


    this.logBuffer = [];
    this.flushInterval = null;
    this.flushIntervalMs = 500;
    this.maxBufferSize = 100;


    this.updateMessageCount = 0;
    this.commandMessageCount = 0;
    this.infoMessageCount = 0;
    this.errorMessageCount = 0;
    this.lastFlushTime = Date.now();
  }

  /**
   * Initialize the logger with DOM elements
   * @param {HTMLElement} container - Log container element
   */
  init(container) {
    this.logContainer = container;


    if (config.logging) {
      if (config.logging.maxEntries) {
        this.maxEntries = config.logging.maxEntries;
      }


      if (config.logging.flushIntervalMs) {
        this.flushIntervalMs = config.logging.flushIntervalMs;
      }
      if (config.logging.maxBufferSize) {
        this.maxBufferSize = config.logging.maxBufferSize;
      }
    }


    this.startBufferFlush();

    return this;
  }

  /**
   * Start the buffer flush interval
   */
  startBufferFlush() {

    if (this.flushInterval) {
      clearInterval(this.flushInterval);
    }


    this.flushInterval = setInterval(() => {
      this.flushBuffer();
    }, this.flushIntervalMs);
  }

  /**
   * Flush the log buffer to the UI
   */
  flushBuffer() {

    if (this.logBuffer.length === 0) {
      return;
    }


    const now = Date.now();
    const elapsed = now - this.lastFlushTime;
    this.lastFlushTime = now;


    const shouldSummarizeUpdates = this.updateMessageCount > 10;
    const shouldSummarizeCommands = this.commandMessageCount > 10;


    const fragment = document.createDocumentFragment();


    if (shouldSummarizeUpdates) {
      const summary = document.createElement('div');
      summary.className = 'log-entry log-entry-summary';
      summary.textContent = `[${new Date().toLocaleTimeString()}] Summarized ${this.updateMessageCount} update messages in the last ${elapsed}ms`;
      fragment.appendChild(summary);
      this.updateMessageCount = 0;
    }

    if (shouldSummarizeCommands) {
      const summary = document.createElement('div');
      summary.className = 'log-entry log-entry-summary';
      summary.textContent = `[${new Date().toLocaleTimeString()}] Summarized ${this.commandMessageCount} command messages in the last ${elapsed}ms`;
      fragment.appendChild(summary);
      this.commandMessageCount = 0;
    }

    // Process individual messages (ones we haven't summarized)
    const remainingMessages = [];

    for (const logEntry of this.logBuffer) {
      // Skip update messages if they were summarized
      if (logEntry.type === 'update' && shouldSummarizeUpdates) {
        continue;
      }

      // Skip command messages if they were summarized
      if (logEntry.type === 'command' && shouldSummarizeCommands) {
        continue;
      }

      // Create UI element for this log entry
      const entry = document.createElement('div');
      entry.className = `log-entry log-entry-${logEntry.type}`;
      entry.textContent = `[${logEntry.timestamp}] ${logEntry.message}`;
      fragment.appendChild(entry);

      // Count this entry for the log limit
      remainingMessages.push(logEntry);
    }

    // Add to log container
    if (this.logContainer && fragment.childNodes.length > 0) {
      this.logContainer.appendChild(fragment);

      // Update log count and enforce the maximum
      this.logCount += fragment.childNodes.length;

      // Remove oldest entries if we've exceeded max
      while (this.logCount > this.maxEntries && this.logContainer.firstChild) {
        this.logContainer.removeChild(this.logContainer.firstChild);
        this.logCount--;
      }

      // Auto-scroll to bottom
      this.logContainer.scrollTop = this.logContainer.scrollHeight;
    }

    // Reset buffer with messages that weren't added (should be none but just in case)
    this.logBuffer = remainingMessages;
  }

  /**
   * Log a message
   * @param {string} message - Message to log
   * @param {string} type - Log level (info, command, update, error)
   */
  log(message, type = 'info') {
    // Add timestamp
    const timestamp = new Date().toLocaleTimeString();

    // Track message counts by type
    switch (type) {
      case 'update':
        this.updateMessageCount++;
        break;
      case 'command':
        this.commandMessageCount++;
        break;
      case 'info':
        this.infoMessageCount++;
        break;
      case 'error':
        this.errorMessageCount++;
        break;
    }

    // Always log errors to console
    if (type === 'error' || (!config.logging || config.logging.console !== false)) {
      switch (type) {
        case 'error':
          console.error(`[${timestamp}] ${message}`);
          break;
        case 'command':
        case 'update':
          // Reduce console noise by only logging these in verbose mode
          if (config.logging?.verbose) {
            console.info(`[${type}] [${timestamp}] ${message}`);
          }
          break;
        default:
          // Only log info messages in verbose mode unless they're important
          if (config.logging?.verbose || message.includes('error') || message.includes('fail')) {
            console.log(`[${timestamp}] ${message}`);
          }
      }
    }

    // Add to buffer
    this.logBuffer.push({
      message,
      type,
      timestamp
    });

    // Flush immediately for errors or if buffer is getting large
    if (type === 'error' || this.logBuffer.length >= this.maxBufferSize) {
      this.flushBuffer();
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

    // Also clear buffer
    this.logBuffer = [];
    this.updateMessageCount = 0;
    this.commandMessageCount = 0;
    this.infoMessageCount = 0;
    this.errorMessageCount = 0;
  }

  /**
   * Clean up resources used by the logger
   */
  destroy() {
    if (this.flushInterval) {
      clearInterval(this.flushInterval);
      this.flushInterval = null;
    }

    this.clear();
  }
}

// Create and export singleton instance
const logger = new Logger();

export default logger;