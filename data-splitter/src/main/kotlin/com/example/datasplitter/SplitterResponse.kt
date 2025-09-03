package com.example.datasplitter

data class SplitterResponse(
    val chunks: List<ChunkInfo>,
    val totalLines: Int,
    val processingTimestamp: String
)