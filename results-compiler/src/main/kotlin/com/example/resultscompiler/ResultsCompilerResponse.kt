package com.example.resultscompiler

data class ResultsCompilerResponse(
    val totalProcessed: Int,
    val successfulChunks: Int,
    val failedChunks: Int,
    val processingTimeMinutes: Double,
    val failedChunkIds: List<String>,
    val reportLocation: String
)