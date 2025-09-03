package com.example.chunkprocessor

data class ChunkProcessorResponse(
    val processedItems: Int,
    val successfulItems: Int,
    val failedItems: Int,
    val processingTimeMs: Long,
    val message: String
)