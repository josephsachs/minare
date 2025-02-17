// annotations/Field.java
package com.minare.core.models.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface Field {
    boolean persist() default true;  // Should this be stored in DynamoDB
    String name() default "";        // Optional field name override
}