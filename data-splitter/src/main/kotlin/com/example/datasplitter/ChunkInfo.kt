package com.example.datasplitter

data class ChunkInfo(
    val bucket: String,
    val key: String,
    val chunkId: String,
    val estimatedLines: Int
)