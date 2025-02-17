package com.minare.core.models;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.minare.persistence.EntityStore;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.Getter;

import javax.inject.Inject;

public class User implements IEntityOwner {
    @Getter
    @JsonProperty("_id")
    public final String id;

    @JsonProperty
    public Set<String> ownedEntityIds;  // Store IDs instead of entities

    @JsonProperty
    public String connectionId;

    @JsonCreator
    public User(
            @JsonProperty("_id") String id,
            @JsonProperty("ownedEntityIds") Set<String> entityIds,
            @JsonProperty("connectionId") String connectionId) {
        this.id = id;
        this.ownedEntityIds = entityIds != null ? entityIds : new HashSet<>();
        this.connectionId = connectionId;
    }

    @Inject
    public User(String id, Set<String> entityIds, String connectionId, EntityStore entityStore) {
        this.id = id;
        this.ownedEntityIds = entityIds;
        this.connectionId = connectionId;
    }

    @Override
    public Set<String> getEntitiesOwned() {
        return ownedEntityIds;
    }
}