package com.minare.core.entity;

import java.util.Set;

public interface EntityOwner {
    public Set<String> getEntitiesOwned();
    public String getId();
}
