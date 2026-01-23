/**
 * Simple event emitter implementation
 */
const createEventEmitter = () => {
  const listeners = new Map();

  return {
    /**
     * Subscribe to an event
     * @param {string} event - Event name
     * @param {Function} callback - Function to call when event is emitted
     * @returns {Function} Unsubscribe function
     */
    on: (event, callback) => {
      if (!listeners.has(event)) {
        listeners.set(event, new Set());
      }

      listeners.get(event).add(callback);


      return () => {
        const eventListeners = listeners.get(event);
        if (eventListeners) {
          eventListeners.delete(callback);
          if (eventListeners.size === 0) {
            listeners.delete(event);
          }
        }
      };
    },

    /**
     * Unsubscribe from an event
     * @param {string} event - Event name
     * @param {Function} callback - Function to unsubscribe
     */
    off: (event, callback) => {
      const eventListeners = listeners.get(event);
      if (eventListeners) {
        eventListeners.delete(callback);
        if (eventListeners.size === 0) {
          listeners.delete(event);
        }
      }
    },

    /**
     * Emit an event
     * @param {string} event - Event name
     * @param {any} data - Data to pass to listeners
     */
    emit: (event, data) => {
      const eventListeners = listeners.get(event);
      if (eventListeners) {
        for (const callback of eventListeners) {
          try {
            callback(data);
          } catch (error) {
            console.error(`Error in event listener for ${event}:`, error);
          }
        }
      }
    }
  };
};

export default createEventEmitter;