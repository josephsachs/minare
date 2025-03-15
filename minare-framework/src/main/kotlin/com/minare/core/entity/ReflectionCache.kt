package com.minare.core.entity

/**
 * Stores reflection information for entity classes to avoid repeated reflection operations.
 */
class ReflectionCache(
        val entityType: String,
        val parentReferenceFields: List<ParentReferenceField>
) {
    /**
     * Find a parent reference field by name
     */
    fun findParentReferenceField(fieldName: String): ParentReferenceField? =
            parentReferenceFields.find { it.fieldName == fieldName }

    /**
     * Check if a field is a parent reference
     */
    fun isParentReference(fieldName: String): Boolean =
            parentReferenceFields.any { it.fieldName == fieldName }

    // Represents a field with @ParentReference annotation
    data class ParentReferenceField(
            val fieldName: String,
            val isBubbleVersion: Boolean
    )
}