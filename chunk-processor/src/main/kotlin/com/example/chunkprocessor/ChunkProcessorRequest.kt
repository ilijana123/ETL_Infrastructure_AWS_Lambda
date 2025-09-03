package com.example.chunkprocessor

data class ChunkProcessorRequest(
    val bucketName: String,
    val chunkKey: String,
    val processingOptions: Map<String, String> = emptyMap()
)