package com.minare.core.transport.services

class SocketEventRouter constructor(
    private val deploymentId: String
) {

    fun getDeploymentId(): String {
        return deploymentId
    }


}