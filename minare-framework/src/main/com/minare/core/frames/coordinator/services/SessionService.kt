package com.minare.core.frames.coordinator.services

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.application.config.FrameConfiguration
import com.minare.core.frames.coordinator.FrameCoordinatorState

@Singleton
class SessionService @Inject constructor(
    private val coordinatorState: FrameCoordinatorState
) {
    private val frameConfiguration: FrameConfiguration = FrameConfiguration()

    suspend fun needAutoSession(): Boolean {
        when (frameConfiguration.autoSession) {
            FrameConfiguration.Companion.AutoSession.NONE -> {
                return false
            }
            FrameConfiguration.Companion.AutoSession.FRAMES_PER_SESSION -> {
                if (coordinatorState.frameInProgress == frameConfiguration.framesPerSession) return true
            }
        }

        // Kotlin likes things clear
        return false
    }
}