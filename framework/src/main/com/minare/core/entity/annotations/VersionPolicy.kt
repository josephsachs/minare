package com.minare.core.entity.annotations

@Target(AnnotationTarget.CLASS)
annotation class VersionPolicy(val rule: VersionPolicyType) {
    companion object {
        enum class VersionPolicyType {
            NONE,
            MUST_MATCH,
            ONLY_NEXT,
            ALLOW_NEWER
        }
    }
}