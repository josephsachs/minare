package com.asyncloadtest.example;

import com.asyncloadtest.controller.AbstractEntityController;
import com.asyncloadtest.core.annotations.Entity;
import com.asyncloadtest.core.annotations.Field;
import com.asyncloadtest.persistence.EntityStore;
import io.vertx.core.json.JsonObject;

import javax.inject.Inject;
import javax.inject.Singleton;

@Singleton
public class ExampleEntityController extends AbstractEntityController {
    // Define the structure of entities this controller manages
    @Entity(name = "example")
    public static class ExampleEntity {
        @Field
        private String id;
        @Field
        private long version;
        @Field(name = "count")
        private int value = 0;
    }

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