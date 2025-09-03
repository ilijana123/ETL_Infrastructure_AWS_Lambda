package com.example.changedetector

data class ChangeDetectionRequest(
    val action: String,
    val newTimestamp: String? = null
)