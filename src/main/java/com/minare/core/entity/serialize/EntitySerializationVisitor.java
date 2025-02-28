package com.minare.core.entity.serialize;

import com.minare.core.models.Entity;
import com.minare.core.models.annotations.entity.EntityType;
import com.minare.core.models.annotations.entity.State;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.Getter;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

public class EntitySerializationVisitor {
    @Getter
    private final JsonArray documents;

    public EntitySerializationVisitor() {
        this.documents = new JsonArray();
    }

    public void visit(Entity entity) {
        JsonObject document = new JsonObject()
                .put("_id", entity._id)
                .put("type", entity.getClass().getAnnotation(EntityType.class).value())
                .put("version", 1)  // For now hardcoded since we haven't implemented versioning
                .put("state", serializeState(entity));

        documents.add(document);
    }

    private JsonObject serializeState(Entity entity) {
        JsonObject state = new JsonObject();
        List<Field> stateFields = new ArrayList<>();

        // Collect all state fields first
        for (Field field : entity.getClass().getDeclaredFields()) {
            if (field.isAnnotationPresent(State.class)) {
                stateFields.add(field);
            }
        }

        // Sort fields by name
        stateFields.sort(Comparator.comparing(Field::getName));

        // Add fields in sorted order
        for (Field field : stateFields) {
            field.setAccessible(true);
            try {
                Object value = field.get(entity);
                addFieldToState(state, field.getName(), value);
            } catch (IllegalAccessException e) {
                throw new RuntimeException("Failed to access field: " + field.getName(), e);
            }
        }

        return state;
    }

    private void addFieldToState(JsonObject state, String fieldName, Object value) {
        if (value == null) {
            return;  // Skip null values
        }

        if (value instanceof Entity) {
            // Entity reference
            state.put(fieldName, new JsonObject()
                    .put("$ref", "entity")
                    .put("$id", ((Entity)value)._id));
        } else if (value instanceof Collection<?>) {
            // Convert collection to JsonArray
            JsonArray array = new JsonArray();
            for (Object item : (Collection<?>)value) {
                if (item instanceof Entity) {
                    array.add(new JsonObject()
                            .put("$ref", "entity")
                            .put("$id", ((Entity)item)._id));
                } else if (item instanceof String) {
                    array.add((String)item);  // For things like our status strings
                } else {
                    array.add(item);  // Basic types
                }
            }
            state.put(fieldName, array);
        } else if (value instanceof JsonObject) {
            // Already a JsonObject (like our offense field)
            state.put(fieldName, value);
        } else {
            // Basic types (numbers, strings, etc)
            state.put(fieldName, value);
        }
    }
}
