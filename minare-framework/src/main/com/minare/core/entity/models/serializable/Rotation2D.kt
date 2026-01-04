package com.minare.core.entity.models.serializable

import java.io.InputStream
import java.io.OutputStream
import java.io.Serializable

/**
 * Represents a discrete 2D rotation with 256 possible angles.
 * Provides deterministic serialization for distributed environments.
 */
class Rotation2D(val index: Int) : Serializable {

    init {
        require(index in 0..255) { "Rotation index must be between 0 and 255" }
    }

    /**
     * Writes this rotation to the given output stream.
     * Uses exactly 1 byte.
     */
    fun writeBytes(out: OutputStream) {
        out.write(index)
    }

    /**
     * Get the angle in radians (if needed for calculations)
     */
    fun toRadians(): Double = (index * Math.PI * 2) / 256.0

    /**
     * Get the angle in degrees (if needed for display)
     */
    fun toDegrees(): Int = (index * 360) / 256

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Rotation2D) return false
        return index == other.index
    }

    override fun hashCode(): Int = index

    override fun toString(): String = "DiscreteRotation(index=$index, degrees=${toDegrees()}°)"

    companion object {
        /**
         * Creates a rotation from bytes in an input stream.
         * Reads exactly 1 byte.
         */
        fun readBytes(input: InputStream): Rotation2D {
            val index = input.read()
            if (index == -1) throw IllegalStateException("End of stream reached while reading DiscreteRotation")
            return Rotation2D(index)
        }

        /**
         * Create from a continuous angle in radians
         */
        fun fromRadians(radians: Double): Rotation2D {
            val normalized = ((radians % (2 * Math.PI)) + 2 * Math.PI) % (2 * Math.PI)
            return Rotation2D((normalized * 256 / (2 * Math.PI)).toInt() % 256)
        }

        /**
         * Create from an angle in degrees
         */
        fun fromDegrees(degrees: Int): Rotation2D {
            val normalized = ((degrees % 360) + 360) % 360
            return Rotation2D((normalized * 256 / 360) % 256)
        }

        val ZERO = Rotation2D(0)             // 0°
        val RIGHT = ZERO                           // 0°
        val DOWN = Rotation2D(64)            // 90°
        val LEFT = Rotation2D(128)           // 180°
        val UP = Rotation2D(192)             // 270°
    }
}