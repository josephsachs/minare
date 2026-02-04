package com.minare.application.config

import com.minare.core.entity.annotations.Property
import com.minare.core.entity.annotations.State
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.models.Entity
import com.minare.exceptions.EntityFactoryException
import java.lang.reflect.Field
import java.lang.reflect.ParameterizedType
import java.lang.reflect.WildcardType

class EntityValidator {

    companion object {
        private const val MAX_RECURSION_DEPTH = 5

        private val BLACKLISTED_TYPES = setOf(
            "io.vertx.core.Vertx",
            "io.vertx.core.http.WebSocket",
            "io.vertx.core.http.ServerWebSocket",
            "java.lang.Thread",
            "java.net.Socket",
            "java.net.ServerSocket"
        )

        private val BLACKLISTED_PACKAGES = setOf(
            "io.vertx.core.http",
            "java.util.concurrent",
            "java.io"
        )

        private val FLOAT_TYPES = setOf(
            Float::class.java,
            java.lang.Float::class.java,
            Double::class.java,
            java.lang.Double::class.java
        )
    }

    data class ValidationIssue(
        val entityClass: String,
        val fieldPath: String,
        val customTypeClass: String?,
        val depth: Int,
        val message: String,
        val isError: Boolean
    )

    fun validate(entityFactory: EntityFactory): Boolean {
        val issues = mutableListOf<ValidationIssue>()

        entityFactory.getTypeNames().forEach { typeName ->
            val entityClass = entityFactory.useClass(typeName) ?: return@forEach
            validateEntityClass(entityClass, entityClass.simpleName, issues)
        }

        if (issues.isEmpty()) {
            return true
        }

        val grouped = issues.groupBy { it.entityClass }
        val report = buildReport(grouped)

        val hasErrors = issues.any { it.isError }

        if (hasErrors) {
            throw EntityFactoryException("Entity validation failed with ${issues.count { it.isError }} error(s):\n$report")
        } else {
            return true
        }
    }

    private fun validateEntityClass(
        entityClass: Class<*>,
        entityName: String,
        issues: MutableList<ValidationIssue>
    ) {
        val fields = getAnnotatedFields(entityClass)

        fields.forEach { field ->
            validateField(
                field = field,
                entityName = entityName,
                fieldPath = field.name,
                depth = 0,
                issues = issues
            )
        }
    }

    private fun validateField(
        field: Field,
        entityName: String,
        fieldPath: String,
        depth: Int,
        issues: MutableList<ValidationIssue>,
        visitedTypes: MutableSet<Class<*>> = mutableSetOf()
    ) {
        val fieldType = field.type

        if (isBlacklisted(fieldType)) {
            issues.add(
                ValidationIssue(
                    entityClass = entityName,
                    fieldPath = fieldPath,
                    customTypeClass = null,
                    depth = depth,
                    message = "Blacklisted type ${fieldType.name}",
                    isError = true
                )
            )
            return
        }

        if (Map::class.java.isAssignableFrom(fieldType)) {
            issues.add(
                ValidationIssue(
                    entityClass = entityName,
                    fieldPath = fieldPath,
                    customTypeClass = null,
                    depth = depth,
                    message = "Map types have known deserialization issues with Jackson: (1) Deserialization can fail based on JSON property order (Jackson #1183), (2) Enum keys with unknown values throw InvalidFormatException - READ_UNKNOWN_ENUM_VALUES_AS_NULL does not work for Map keys (Jackson #1859, #1674, #1882), (3) Custom enum serializers are ignored for Map keys (Jackson #2440), (4) WRITE_ENUM_KEYS_USING_INDEX cannot round-trip (Jackson #1877, #2536).",
                    isError = false
                )
            )
            return
        }

        if (fieldType in FLOAT_TYPES) {
            issues.add(
                ValidationIssue(
                    entityClass = entityName,
                    fieldPath = fieldPath,
                    customTypeClass = null,
                    depth = depth,
                    message = "IEEE-754 floating-point types (Float, Double) are nondeterministic across distributed clusters; consider using BigDecimal, BigInteger, or Long instead.",
                    isError = false
                )
            )
            return
        }

        if (Collection::class.java.isAssignableFrom(fieldType)) {
            validateCollection(field, entityName, fieldPath, depth, issues)
            return
        }

        if (shouldRecurseIntoType(fieldType) && depth < MAX_RECURSION_DEPTH) {
            if (visitedTypes.contains(fieldType)) {
                return
            }

            visitedTypes.add(fieldType)
            recurseIntoCustomType(fieldType, entityName, fieldPath, depth, issues, visitedTypes)
        }
    }

    private fun validateCollection(
        field: Field,
        entityName: String,
        fieldPath: String,
        depth: Int,
        issues: MutableList<ValidationIssue>
    ) {
        val elementType = getCollectionElementType(field)

        if (elementType == null) {
            issues.add(
                ValidationIssue(
                    entityClass = entityName,
                    fieldPath = fieldPath,
                    customTypeClass = null,
                    depth = depth,
                    message = "Collection has unresolvable element type",
                    isError = true
                )
            )
            return
        }

        if (Collection::class.java.isAssignableFrom(elementType)) {
            issues.add(
                ValidationIssue(
                    entityClass = entityName,
                    fieldPath = fieldPath,
                    customTypeClass = null,
                    depth = depth,
                    message = "Nested collections are not supported (${field.type.simpleName}<${elementType.simpleName}>)",
                    isError = true
                )
            )
            return
        }

        if (Entity::class.java.isAssignableFrom(elementType)) {
            issues.add(
                ValidationIssue(
                    entityClass = entityName,
                    fieldPath = fieldPath,
                    customTypeClass = null,
                    depth = depth,
                    message = "Collections of Entity types don't rehydrate yet (${field.type.simpleName}<${elementType.simpleName}>)",
                    isError = false
                )
            )
            return
        }
    }

    private fun recurseIntoCustomType(
        customType: Class<*>,
        entityName: String,
        parentPath: String,
        currentDepth: Int,
        issues: MutableList<ValidationIssue>,
        visitedTypes: MutableSet<Class<*>>
    ) {
        if (currentDepth >= MAX_RECURSION_DEPTH) {
            return
        }

        val fields = getAllFields(customType)

        fields.forEach { field ->
            validateField(
                field = field,
                entityName = entityName,
                fieldPath = "$parentPath.${field.name}",
                depth = currentDepth + 1,
                issues = issues,
                visitedTypes = visitedTypes
            )
        }
    }

    private fun getAnnotatedFields(entityClass: Class<*>): List<Field> {
        val fields = mutableListOf<Field>()
        var currentClass: Class<*>? = entityClass

        while (currentClass != null && currentClass != Any::class.java) {
            currentClass.declaredFields
                .filter { it.isAnnotationPresent(State::class.java) || it.isAnnotationPresent(Property::class.java) }
                .forEach { fields.add(it) }
            currentClass = currentClass.superclass
        }

        return fields
    }

    private fun getAllFields(clazz: Class<*>): List<Field> {
        val fields = mutableListOf<Field>()
        var currentClass: Class<*>? = clazz

        while (currentClass != null && currentClass != Any::class.java) {
            fields.addAll(currentClass.declaredFields)
            currentClass = currentClass.superclass
        }

        return fields
    }

    private fun isBlacklisted(type: Class<*>): Boolean {
        if (BLACKLISTED_TYPES.contains(type.name)) {
            return true
        }

        return BLACKLISTED_PACKAGES.any { pkg ->
            type.name.startsWith("$pkg.")
        }
    }

    private fun shouldRecurseIntoType(type: Class<*>): Boolean {
        return when {
            type.isPrimitive -> false
            type.isArray -> false
            type.isEnum -> false
            type == String::class.java -> false
            type.name.startsWith("java.lang.") -> false
            type.name.startsWith("java.util.") -> false
            type.name.startsWith("java.time.") -> false
            type.name.startsWith("kotlin.") -> false
            type.name.startsWith("io.vertx.core.json.") -> false
            Entity::class.java.isAssignableFrom(type) -> false
            else -> true
        }
    }

    private fun getCollectionElementType(field: Field): Class<*>? {
        val genericType = field.genericType
        if (genericType is ParameterizedType) {
            var typeArg = genericType.actualTypeArguments.firstOrNull() ?: return null

            if (typeArg is WildcardType) {
                typeArg = typeArg.upperBounds.firstOrNull() ?: return null
            }

            return when (typeArg) {
                is Class<*> -> typeArg
                is ParameterizedType -> {
                    val rawType = typeArg.rawType
                    when (rawType) {
                        is Class<*> -> rawType
                        else -> null
                    }
                }
                else -> null
            }
        }
        return null
    }

    private fun buildReport(grouped: Map<String, List<ValidationIssue>>): String {
        val builder = StringBuilder()

        grouped.forEach { (entityName, entityIssues) ->
            builder.append("\nEntity: $entityName\n")

            val errors = entityIssues.filter { it.isError }
            val warnings = entityIssues.filter { !it.isError }

            errors.forEach { issue ->
                builder.append("  ERROR in ${issue.fieldPath}")
                if (issue.customTypeClass != null) {
                    builder.append(" (${issue.customTypeClass})")
                }
                if (issue.depth > 0) {
                    builder.append(" [depth: ${issue.depth}]")
                }
                builder.append(": ${issue.message}\n")
            }

            warnings.forEach { issue ->
                builder.append("  WARNING in ${issue.fieldPath}")
                if (issue.customTypeClass != null) {
                    builder.append(" (${issue.customTypeClass})")
                }
                if (issue.depth > 0) {
                    builder.append(" [depth: ${issue.depth}]")
                }
                builder.append(": ${issue.message}\n")
            }
        }

        return builder.toString()
    }
}