package com.minare.application.config

import com.minare.core.entity.annotations.Property
import com.minare.core.entity.annotations.State
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.models.Entity
import com.minare.exceptions.EntityFactoryException
import io.vertx.core.Vertx
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.net.Socket
import java.util.concurrent.ExecutorService

class EntityValidatorTest {

    private lateinit var validator: EntityValidator

    @BeforeEach
    fun setup() {
        validator = EntityValidator()
    }

    // Test entity classes
    class ValidEntity : Entity() {
        @State
        var name: String = ""

        @State
        var health: Int = 100

        @Property
        var level: Long = 1L
    }

    class EntityWithMap : Entity() {
        @State
        var name: String = ""

        @State
        var attributes: Map<String, Int> = emptyMap()
    }

    class EntityWithHashMap : Entity() {
        @State
        var config: HashMap<String, String> = hashMapOf()
    }

    class EntityWithBlacklistedType : Entity() {
        @State
        var name: String = ""

        @State
        var vertxInstance: Vertx? = null
    }

    class EntityWithBlacklistedPackage : Entity() {
        @State
        var executor: ExecutorService? = null
    }

    class EntityWithSocket : Entity() {
        @State
        var connection: Socket? = null
    }

    class EntityWithNestedCollection : Entity() {
        @State
        var matrix: List<List<Int>> = emptyList()
    }

    class EntityWithEntityCollection : Entity() {
        @State
        var allies: List<ValidEntity> = emptyList()
    }

    class EntityWithEntitySet : Entity() {
        @State
        var friends: Set<ValidEntity> = emptySet()
    }

    data class SimpleDto(
        val name: String = "",
        val value: Int = 0
    )

    class EntityWithSimpleDto : Entity() {
        @State
        var name: String = ""

        @State
        var config: SimpleDto = SimpleDto()
    }

    data class DtoWithMap(
        val name: String = "",
        val settings: Map<String, String> = emptyMap()
    )

    class EntityWithDtoContainingMap : Entity() {
        @State
        var config: DtoWithMap = DtoWithMap()
    }

    data class DtoWithBlacklist(
        val name: String = "",
        val vertx: Vertx? = null
    )

    class EntityWithDtoContainingBlacklist : Entity() {
        @State
        var config: DtoWithBlacklist = DtoWithBlacklist()
    }

    data class NestedDto(
        val value: Int = 0
    )

    data class MiddleDto(
        val nested: NestedDto = NestedDto()
    )

    data class OuterDto(
        val middle: MiddleDto = MiddleDto()
    )

    class EntityWithNestedDtos : Entity() {
        @State
        var config: OuterDto = OuterDto()
    }

    data class Level1Dto(val next: Level2Dto? = null)
    data class Level2Dto(val next: Level3Dto? = null)
    data class Level3Dto(val next: Level4Dto? = null)
    data class Level4Dto(val next: Level5Dto? = null)
    data class Level5Dto(val next: Level6Dto? = null)
    data class Level6Dto(val value: String = "")

    class EntityWithDeepNesting : Entity() {
        @State
        var config: Level1Dto = Level1Dto()
    }

    data class CircularA(var b: CircularB? = null)
    data class CircularB(var a: CircularA? = null)

    class EntityWithCircularReference : Entity() {
        @State
        var circular: CircularA = CircularA()
    }

    class EntityWithMixedIssues : Entity() {
        @State
        var name: String = ""

        @State
        var badMap: Map<String, Int> = emptyMap()

        @State
        var allies: List<ValidEntity> = emptyList()

        @State
        var matrix: List<List<String>> = emptyList()
    }

    private fun createFactory(vararg entityClasses: Pair<String, Class<out Entity>>): EntityFactory {
        return object : EntityFactory() {
            override val entityTypes = mapOf(*entityClasses)
        }
    }

    @Nested
    @DisplayName("Valid entities")
    inner class ValidEntities {

        @Test
        @DisplayName("should pass validation for entity with only primitives")
        fun validEntityPassesValidation() {
            val factory = createFactory("ValidEntity" to ValidEntity::class.java)

            val result = validator.validate(factory)

            assertThat(result).isTrue()
        }

        @Test
        @DisplayName("should pass validation for empty factory")
        fun emptyFactoryPassesValidation() {
            val factory = createFactory()

            val result = validator.validate(factory)

            assertThat(result).isTrue()
        }

        @Test
        @DisplayName("should pass validation for entity with simple DTO")
        fun entityWithSimpleDtoPassesValidation() {
            val factory = createFactory("EntityWithSimpleDto" to EntityWithSimpleDto::class.java)

            val result = validator.validate(factory)

            assertThat(result).isTrue()
        }

        @Test
        @DisplayName("should pass validation for entity with nested DTOs")
        fun entityWithNestedDtosPassesValidation() {
            val factory = createFactory("EntityWithNestedDtos" to EntityWithNestedDtos::class.java)

            val result = validator.validate(factory)

            assertThat(result).isTrue()
        }
    }

    @Nested
    @DisplayName("Map field validation")
    inner class MapFieldValidation {

        @Test
        @DisplayName("should throw error for Map field")
        fun entityWithMapFieldThrowsError() {
            val factory = createFactory("EntityWithMap" to EntityWithMap::class.java)

            assertThatThrownBy { validator.validate(factory) }
                .isInstanceOf(EntityFactoryException::class.java)
                .hasMessageContaining("Map types are not supported")
                .hasMessageContaining("attributes")
        }

        @Test
        @DisplayName("should throw error for HashMap field")
        fun entityWithHashMapFieldThrowsError() {
            val factory = createFactory("EntityWithHashMap" to EntityWithHashMap::class.java)

            assertThatThrownBy { validator.validate(factory) }
                .isInstanceOf(EntityFactoryException::class.java)
                .hasMessageContaining("Map types are not supported")
                .hasMessageContaining("config")
        }

        @Test
        @DisplayName("should throw error for Map inside DTO at depth 1")
        fun entityWithDtoContainingMapThrowsError() {
            val factory = createFactory("EntityWithDtoContainingMap" to EntityWithDtoContainingMap::class.java)

            assertThatThrownBy { validator.validate(factory) }
                .isInstanceOf(EntityFactoryException::class.java)
                .hasMessageContaining("Map types are not supported")
                .hasMessageContaining("config.settings")
                .hasMessageContaining("[depth: 1]")
        }
    }

    @Nested
    @DisplayName("Blacklisted type validation")
    inner class BlacklistedTypeValidation {

        @Test
        @DisplayName("should throw error for Vertx field")
        fun entityWithBlacklistedTypeThrowsError() {
            val factory = createFactory("EntityWithBlacklistedType" to EntityWithBlacklistedType::class.java)

            assertThatThrownBy { validator.validate(factory) }
                .isInstanceOf(EntityFactoryException::class.java)
                .hasMessageContaining("Blacklisted type")
                .hasMessageContaining("Vertx")
        }

        @Test
        @DisplayName("should throw error for ExecutorService from blacklisted package")
        fun entityWithBlacklistedPackageThrowsError() {
            val factory = createFactory("EntityWithBlacklistedPackage" to EntityWithBlacklistedPackage::class.java)

            assertThatThrownBy { validator.validate(factory) }
                .isInstanceOf(EntityFactoryException::class.java)
                .hasMessageContaining("Blacklisted type")
                .hasMessageContaining("ExecutorService")
        }

        @Test
        @DisplayName("should throw error for Socket field")
        fun entityWithSocketThrowsError() {
            val factory = createFactory("EntityWithSocket" to EntityWithSocket::class.java)

            assertThatThrownBy { validator.validate(factory) }
                .isInstanceOf(EntityFactoryException::class.java)
                .hasMessageContaining("Blacklisted type")
                .hasMessageContaining("Socket")
        }

        @Test
        @DisplayName("should throw error for blacklisted type inside DTO")
        fun entityWithDtoContainingBlacklistThrowsError() {
            val factory = createFactory("EntityWithDtoContainingBlacklist" to EntityWithDtoContainingBlacklist::class.java)

            assertThatThrownBy { validator.validate(factory) }
                .isInstanceOf(EntityFactoryException::class.java)
                .hasMessageContaining("Blacklisted type")
                .hasMessageContaining("config.vertx")
                .hasMessageContaining("[depth: 1]")
        }
    }

    @Nested
    @DisplayName("Collection validation")
    inner class CollectionValidation {

        @Test
        @DisplayName("should throw error for nested collection")
        fun entityWithNestedCollectionThrowsError() {
            val factory = createFactory("EntityWithNestedCollection" to EntityWithNestedCollection::class.java)

            assertThatThrownBy { validator.validate(factory) }
                .isInstanceOf(EntityFactoryException::class.java)
                .hasMessageContaining("Nested collections are not supported")
                .hasMessageContaining("matrix")
        }

        @Test
        @DisplayName("should produce warning only for List of entities")
        fun entityWithEntityCollectionProducesWarningOnly() {
            val factory = createFactory("EntityWithEntityCollection" to EntityWithEntityCollection::class.java)

            val result = validator.validate(factory)

            assertThat(result).isTrue()
        }

        @Test
        @DisplayName("should produce warning only for Set of entities")
        fun entityWithEntitySetProducesWarningOnly() {
            val factory = createFactory("EntityWithEntitySet" to EntityWithEntitySet::class.java)

            val result = validator.validate(factory)

            assertThat(result).isTrue()
        }
    }

    @Nested
    @DisplayName("Recursion handling")
    inner class RecursionHandling {

        @Test
        @DisplayName("should stop at max depth and not throw error")
        fun entityWithDeepNestingStopsAtMaxDepth() {
            val factory = createFactory("EntityWithDeepNesting" to EntityWithDeepNesting::class.java)

            val result = validator.validate(factory)

            assertThat(result).isTrue()
        }

        @Test
        @DisplayName("should handle circular references gracefully")
        fun entityWithCircularReferenceHandlesGracefully() {
            val factory = createFactory("EntityWithCircularReference" to EntityWithCircularReference::class.java)

            val result = validator.validate(factory)

            assertThat(result).isTrue()
        }
    }

    @Nested
    @DisplayName("Error reporting")
    inner class ErrorReporting {

        @Test
        @DisplayName("should report all issues for entity with mixed problems")
        fun entityWithMixedIssuesReportsAllProblems() {
            val factory = createFactory("EntityWithMixedIssues" to EntityWithMixedIssues::class.java)

            assertThatThrownBy { validator.validate(factory) }
                .isInstanceOf(EntityFactoryException::class.java)
                .hasMessageContaining("Map types are not supported")
                .hasMessageContaining("badMap")
                .hasMessageContaining("Nested collections are not supported")
                .hasMessageContaining("matrix")
                .hasMessageContaining("allies")
        }

        @Test
        @DisplayName("should group issues by entity class")
        fun multipleEntitiesReportGroupedByEntity() {
            val factory = createFactory(
                "ValidEntity" to ValidEntity::class.java,
                "EntityWithMap" to EntityWithMap::class.java,
                "EntityWithBlacklistedType" to EntityWithBlacklistedType::class.java
            )

            assertThatThrownBy { validator.validate(factory) }
                .isInstanceOf(EntityFactoryException::class.java)
                .hasMessageContaining("Entity: EntityWithMap")
                .hasMessageContaining("Entity: EntityWithBlacklistedType")
                .hasMessageNotContaining("Entity: ValidEntity")
        }
    }
}