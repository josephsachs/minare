package com.minare.core.models;

import java.util.Set;

public interface IEntityOwner {
    public Set<String> getEntitiesOwned();
    public String getId();
}
