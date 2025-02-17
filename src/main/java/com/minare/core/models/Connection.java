package com.minare.core.models;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Connection {
    @JsonProperty("_id")
    public String id;
    public String userId;
}
