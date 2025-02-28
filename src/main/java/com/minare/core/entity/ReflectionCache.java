package com.minare.core.entity;

import com.google.inject.Inject;
import com.google.inject.Singleton;

import java.util.List;
import java.util.Optional;

/**
 * Stores reflection information for entity classes to avoid repeated reflection operations.
 */
public class ReflectionCache {

    private final String entityType;
    private final List<ParentReferenceField> parentReferenceFields;

    public ReflectionCache(String entityType, List<ParentReferenceField> parentReferenceFields) {
        this.entityType = entityType;
        this.parentReferenceFields = parentReferenceFields;
    }

    public String getEntityType() {
        return entityType;
    }

    public List<ParentReferenceField> getParentReferenceFields() {
        return parentReferenceFields;
    }

    /**
     * Find a parent reference field by name
     */
    public Optional<ParentReferenceField> findParentReferenceField(String fieldName) {
        return parentReferenceFields.stream()
                .filter(field -> field.getFieldName().equals(fieldName))
                .findFirst();
    }

    /**
     * Check if a field is a parent reference
     */
    public boolean isParentReference(String fieldName) {
        return parentReferenceFields.stream()
                .anyMatch(field -> field.getFieldName().equals(fieldName));
    }

    // Represents a field with @ParentReference annotation
    public static class ParentReferenceField {
        private final String fieldName;
        private final boolean bubbleVersion;

        public ParentReferenceField(String fieldName, boolean bubbleVersion) {
            this.fieldName = fieldName;
            this.bubbleVersion = bubbleVersion;
        }

        public String getFieldName() {
            return fieldName;
        }

        public boolean isBubbleVersion() {
            return bubbleVersion;
        }
    }
}