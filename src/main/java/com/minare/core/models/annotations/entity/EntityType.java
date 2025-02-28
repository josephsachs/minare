// annotations/entity.java
package com.minare.core.models.annotations.entity;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(RetentionPolicy.RUNTIME)
public @interface EntityType {
    String value();
}