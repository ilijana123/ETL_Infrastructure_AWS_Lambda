package com.example.resultscompiler

data class ResultsCompilerRequest(
    val processingResults: List<Map<String, Any>>,
    val executionId: String
)