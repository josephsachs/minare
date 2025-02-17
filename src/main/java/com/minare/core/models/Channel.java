package com.minare.core.models;

import java.util.Set;

public class Channel {
    public String id;
    public Set<Connection> subscribers;

    public Channel(String channelId) {
        this.id = channelId;
    }
}
