package com.example.resultscompiler

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.core.sync.RequestBody
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.time.LocalDateTime
import kotlin.collections.get

class ResultsCompilerLambda : RequestHandler<ResultsCompilerRequest, ResultsCompilerResponse> {

    private val s3 = S3Client.create()
    private val objectMapper = jacksonObjectMapper()

    private val bucketName = System.getenv("PROCESSING_BUCKET") ?: "nutritiveappbucket"

    override fun handleRequest(request: ResultsCompilerRequest, context: Context): ResultsCompilerResponse {
        context.logger.log("Compiling results for execution: ${request.executionId}\n")
        context.logger.log("Processing results count: ${request.processingResults.size}\n")

        var totalProcessed = 0
        var successfulChunks = 0
        var failedChunks = 0
        var totalProcessingTime = 0L
        val failedChunkIds = mutableListOf<String>()

        request.processingResults.forEach { result ->
            try {
                val payload = result["Payload"] as? Map<*, *>
                if (payload != null) {
                    val chunkId = payload["chunkId"] as? String ?: "unknown"
                    val processedRows = (payload["processedRows"] as? Number)?.toInt() ?: 0
                    val success = payload["success"] as? Boolean ?: false
                    val processingTimeMs = (payload["processingTimeMs"] as? Number)?.toLong() ?: 0L

                    totalProcessingTime += processingTimeMs

                    if (success) {
                        successfulChunks++
                        totalProcessed += processedRows
                        context.logger.log("✓ Chunk $chunkId: $processedRows rows processed\n")
                    } else {
                        failedChunks++
                        failedChunkIds.add(chunkId)
                        val errorMessage = payload["errorMessage"] as? String ?: "Unknown error"
                        context.logger.log("✗ Chunk $chunkId failed: $errorMessage\n")
                    }
                }
            } catch (e: Exception) {
                context.logger.log("Error processing result: ${e.message}\n")
                failedChunks++
            }
        }

        val processingTimeMinutes = totalProcessingTime / 60000.0

        val report = mapOf(
            "executionId" to request.executionId,
            "timestamp" to LocalDateTime.now().toString(),
            "summary" to mapOf(
                "totalProcessed" to totalProcessed,
                "successfulChunks" to successfulChunks,
                "failedChunks" to failedChunks,
                "processingTimeMinutes" to processingTimeMinutes
            ),
            "failedChunkIds" to failedChunkIds,
            "detailedResults" to request.processingResults
        )

        val reportKey = "etl/reports/${request.executionId}/processing-report.json"
        val reportJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(report)

        val putRequest = PutObjectRequest.builder()
            .bucket(bucketName)
            .key(reportKey)
            .contentType("application/json")
            .build()

        s3.putObject(putRequest, RequestBody.fromString(reportJson))

        val reportLocation = "s3://$bucketName/$reportKey"

        context.logger.log("""
            |ETL Processing Complete:
            |  Total Processed: $totalProcessed rows
            |  Successful Chunks: $successfulChunks
            |  Failed Chunks: $failedChunks
            |  Processing Time: ${"%.2f".format(processingTimeMinutes)} minutes
            |  Report: $reportLocation
        """.trimMargin())

        if (failedChunks == 0) {
            context.logger.log("All chunks processed successfully, cleaning up temporary files...\n")
        }

        return ResultsCompilerResponse(
            totalProcessed = totalProcessed,
            successfulChunks = successfulChunks,
            failedChunks = failedChunks,
            processingTimeMinutes = processingTimeMinutes,
            failedChunkIds = failedChunkIds,
            reportLocation = reportLocation
        )
    }
}