package com.minare.core.config

import com.google.inject.Provider
import com.google.inject.Singleton
import com.minare.application.interfaces.AppState

/**
 * Provider for AppState that enables late binding.
 * This allows AppState to be injected even though it's created after the Guice injector.
 */
@Singleton
class AppStateProvider : Provider<AppState> {
    companion object {
        @Volatile
        private var instance: AppState? = null

        fun setInstance(appState: AppState) {
            instance = appState
        }
    }

    override fun get(): AppState {
        return instance ?: throw IllegalStateException(
            "AppState has not been initialized. This typically means the provider is being " +
                    "accessed before MinareApplication has completed initialization."
        )
    }
}