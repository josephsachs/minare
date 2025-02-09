// controllers/AbstractEntityController.java
package com.asyncloadtest.controller;

import com.asyncloadtest.core.state.EntityStateManager;
import com.asyncloadtest.core.state.StateCache;
import io.vertx.core.json.JsonObject;

import javax.inject.Inject;

public abstract class AbstractEntityController {
    private final EntityStateManager stateManager;
    private final StateCache stateCache;

    @Inject
    public AbstractEntityController(EntityStateManager stateManager, StateCache stateCache) {
        this.stateManager = stateManager;
        this.stateCache = stateCache;
    }

    protected abstract boolean hasAccess(String userId, String channelId);
    protected abstract JsonObject filterStateForUser(String userId, JsonObject state);

    public void handleUpdate(String channelId, JsonObject update, String checksum, long timestamp) {
        if (!stateCache.validateChecksum(channelId, timestamp, checksum)) {
            throw new IllegalStateException("Checksum validation failed");
        }

        if (isSignificantChange(channelId, update)) {
            stateManager.updateState(channelId, update);
        }
    }

    protected boolean isSignificantChange(String channelId, JsonObject newState) {
        return true; // Default implementation treats all changes as significant
    }
}