package com.example.changedetector

data class ChangeDetectionResponse(
    val hasUpdates: Boolean,
    val downloadUrl: String? = null,
    val lastProcessedTimestamp: String? = null,
    val latestTimestamp: String? = null,
    val message: String
)