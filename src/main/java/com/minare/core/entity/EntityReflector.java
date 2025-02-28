package com.minare.core.entity;


import com.google.inject.Singleton;
import com.google.inject.Inject;
import com.minare.core.models.Entity;
import com.minare.core.models.annotations.entity.EntityType;
import com.minare.core.models.annotations.entity.ParentReference;
import com.minare.core.models.annotations.entity.State;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
/**
 * Injectable utility class for building reflection caches for entity classes.
 */
@Singleton
public class EntityReflector {

    // Cache of entity type to reflection information
    private final Map<String, ReflectionCache> reflectionCacheByType = new ConcurrentHashMap<>();
    private final Map<Class<?>, ReflectionCache> reflectionCacheByClass = new ConcurrentHashMap<>();

    @Inject
    public EntityReflector() {
        // Constructor for Guice
    }

    /**
     * Gets or builds the reflection cache for an entity type.
     */
    public ReflectionCache getReflectionCache(String entityType) {
        return reflectionCacheByType.get(entityType);
    }

    /**
     * Gets or builds the reflection cache for an entity class.
     */
    public ReflectionCache getReflectionCache(Class<?> entityClass) {
        return reflectionCacheByClass.computeIfAbsent(entityClass, this::buildReflectionCache);
    }

    /**
     * Builds reflection cache for an entity class.
     */
    public ReflectionCache buildReflectionCache(Class<?> entityClass) {
        EntityType entityAnnotation =
                entityClass.getAnnotation(EntityType.class);
        if (entityAnnotation == null) {
            throw new IllegalArgumentException("Class " + entityClass.getName() + " is not annotated with @Entity");
        }

        String entityType = entityAnnotation.value();

        List<ReflectionCache.ParentReferenceField> parentReferenceFields = new ArrayList<>();

        // Process all fields in the class hierarchy
        Class<?> currentClass = entityClass;
        while (currentClass != null && !currentClass.equals(Object.class)) {
            processClassFields(currentClass, parentReferenceFields);
            currentClass = currentClass.getSuperclass();
        }

        ReflectionCache cache = new ReflectionCache(entityType, parentReferenceFields);

        // Store in both maps for efficient lookup
        reflectionCacheByType.put(entityType, cache);

        return cache;
    }

    private void processClassFields(
            Class<?> clazz,
            List<ReflectionCache.ParentReferenceField> parentReferenceFields) {

        // Use streams to process fields
        List<ReflectionCache.ParentReferenceField> classFields = List.of(clazz.getDeclaredFields()).stream()
                .filter(field -> field.isAnnotationPresent(State.class) &&
                        field.isAnnotationPresent(ParentReference.class))
                .map(field -> {
                    ParentReference parentRef =
                            field.getAnnotation(ParentReference.class);
                    boolean bubbleVersion = parentRef.bubble_version();
                    return new ReflectionCache.ParentReferenceField(field.getName(), bubbleVersion);
                })
                .collect(Collectors.toList());

        parentReferenceFields.addAll(classFields);
    }

    /**
     * Gets all parent reference field paths for all known entity types.
     * This is useful for building MongoDB traversal queries.
     */
    public List<String> getAllParentReferenceFieldPaths() {
        return reflectionCacheByType.values().stream()
                .flatMap(cache -> cache.getParentReferenceFields().stream())
                .map(field -> "state." + field.getFieldName())
                .distinct()
                .collect(Collectors.toList());
    }

    /**
     * Registers all entity classes to pre-build the reflection cache.
     */
    public void registerEntityClasses(List<Class<? extends Entity>> entityClasses) {
        entityClasses.forEach(this::buildReflectionCache);
    }
}