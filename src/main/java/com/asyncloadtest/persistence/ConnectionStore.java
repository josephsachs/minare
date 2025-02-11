package com.asyncloadtest.persistence;

public interface ConnectionStore extends Store {
    void storeConnection(String connectionId, long timestamp);
    void removeConnection(String connectionId);
    boolean isConnectionActive(String connectionId);
}