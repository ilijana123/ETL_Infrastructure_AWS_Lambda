package com.example.datasplitter

data class SplitterRequest(
    val downloadUrl: String,
    val lastProcessedTimestamp: String?
)