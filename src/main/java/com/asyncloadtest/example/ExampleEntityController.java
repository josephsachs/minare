// example/ExampleEntityController.java
package com.asyncloadtest.example;

import com.asyncloadtest.controller.AbstractEntityController;
import com.asyncloadtest.core.state.EntityStateManager;
import com.asyncloadtest.core.state.StateCache;
import io.vertx.core.json.JsonObject;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ExampleEntityController extends AbstractEntityController {

    @Inject
    public ExampleEntityController(EntityStateManager stateManager, StateCache stateCache) {
        super(stateManager, stateCache);
    }

    @Override
    protected boolean hasAccess(String userId, String channelId) {
        return true; // Example implementation grants access to all
    }

    @Override
    protected JsonObject filterStateForUser(String userId, JsonObject state) {
        return state; // Example implementation doesn't filter state
    }
}