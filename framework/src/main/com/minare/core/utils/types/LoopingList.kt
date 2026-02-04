package com.minare.core.utils.types

/**
 * A List with looping iterator
 */
class LoopingList<T>(
    private val sequence: List<T>,
    private val looping: Boolean
) {
    private var currentIndex: Int = -1

    /**
     * Checks if a next element exists.
     * Always returns true if looping is enabled and the sequence is not empty.
     */
    fun hasNext(): Boolean {
        if (sequence.isEmpty()) return false

        if (looping) return true

        return currentIndex < sequence.size - 1
    }

    /**
     * Advances the index and returns the next element in the sequence.
     * @throws NoSuchElementException if hasNext() is false (non-looping mode finished).
     */
    fun next(): T {
        if (!hasNext()) {
            throw NoSuchElementException("Sequence finished and is not set to loop.")
        }

        currentIndex++

        if (looping) {
            // Modulo operation handles wrap-around for circular iteration
            currentIndex %= sequence.size
        }

        // Sanity check to ensure index hasn't overshot in non-looping mode
        if (currentIndex >= sequence.size) {
            throw NoSuchElementException("Internal logic error: Index out of bounds in non-looping mode.")
        }

        return sequence[currentIndex]
    }

    /** Resets the tracker to the starting position. */
    fun reset() {
        currentIndex = -1
    }

    /** Returns the size of the underlying sequence. */
    fun size(): Int = sequence.size
}