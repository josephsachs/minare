package com.minare.core.entity.models.serializable

import java.io.DataInput
import java.io.DataOutput
import java.io.Serializable
import io.vertx.core.buffer.Buffer

data class Vector2(
    val x: Int,
    val y: Int
) : Serializable {
    companion object {
        /**
         * Writes the Vector2 instance data to the output stream.
         */
        fun writeBytes(output: DataOutput, vector: Vector2) {
            output.writeInt(vector.x)
            output.writeInt(vector.y)
        }

        /**
         * Reads the data from the input stream and reconstructs the Vector2.
         */
        fun readBytes(input: DataInput): Vector2 {
            val x = input.readInt()
            val y = input.readInt()
            return Vector2(x, y)
        }

        const val BYTE_SIZE = 8

        /**
         * Encodes the Vector2 into a Vert.x Buffer (8 bytes).
         */
        fun encodeToBuffer(vector: Vector2): Buffer {
            return Buffer.buffer(BYTE_SIZE)
                .appendInt(vector.x) // Writes the first 4 bytes (X)
                .appendInt(vector.y) // Writes the next 4 bytes (Y)
        }

        /**
         * Decodes a Vert.x Buffer (8 bytes) back into a Vector2.
         */
        fun decodeFromBuffer(buffer: Buffer): Vector2 {
            val x = buffer.getInt(0) // Read 4 bytes starting at index 0
            val y = buffer.getInt(4) // Read 4 bytes starting at index 4
            return Vector2(x, y)
        }
    }
}