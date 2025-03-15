package com.minare.example;

import com.minare.controller.EntityController;
import com.minare.persistence.EntityStore;
import io.vertx.core.json.JsonObject;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ExampleEntityController extends EntityController {

    @Inject
    public ExampleEntityController(EntityStore entityStore) {
        super(entityStore);
    }

    @Override
    protected boolean hasAccess(String connectionId, String entityId) {
        return true;  // Example implementation grants access to all
    }

    @Override
    protected boolean validateUpdate(JsonObject currentState, JsonObject proposedUpdate) {
        return true;  // Example implementation accepts all updates
    }
}