package com.minare.core.models;

import io.vertx.core.json.JsonObject;

public class DefaultEntityBuilder implements IEntityStateBuilder {
    public JsonObject build() {
        return new JsonObject();
    }
}
