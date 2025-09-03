package com.example.datasplitter

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import software.amazon.awssdk.core.sync.RequestBody
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import java.io.ByteArrayOutputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

data class SplitterRequest(
    val downloadUrl: String,
    val lastProcessedTimestamp: String?
)

data class SplitterResponse(
    val chunks: List<ChunkInfo>,
    val totalLines: Int,
    val processingTimestamp: String
)

data class ChunkInfo(
    val bucket: String,
    val key: String,
    val chunkId: String,
    val estimatedLines: Int
)

class DataSplitterLambda : RequestHandler<Map<String, Any>, SplitterResponse> {

    private val s3 = S3Client.create()
    private val httpClient = HttpClient.newHttpClient()
    private val objectMapper = jacksonObjectMapper().apply {
        // Configure to be more lenient with JSON parsing
        configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
        configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_SINGLE_QUOTES, true)
        configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_COMMENTS, true)
        configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    }

    private val bucketName = System.getenv("PROCESSING_BUCKET") ?: "nutritiveappbucket"
    private val chunkSize = System.getenv("CHUNK_SIZE")?.toIntOrNull() ?: 5000 // lines per chunk

    // Handle incoming requests, adapting to the payload type
    override fun handleRequest(input: Map<String, Any>, context: Context): SplitterResponse {
        val request = if (input.containsKey("source") && input["source"] == "aws.events") {
            // It's a scheduled event, use environment variables or default values
            context.logger.log("Received a scheduled event. Using default values for SplitterRequest.\n")
            SplitterRequest(
                downloadUrl = System.getenv("DOWNLOAD_URL") ?: "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.jsonl.gz",
                lastProcessedTimestamp = System.getenv("LAST_PROCESSED_TIMESTAMP")
            )
        } else {
            // It's the expected SplitterRequest
            objectMapper.convertValue(input, SplitterRequest::class.java)
        }

        return processData(request, context)
    }

    private fun processData(request: SplitterRequest, context: Context): SplitterResponse {
        context.logger.log("Starting data download and splitting from: ${request.downloadUrl}\n")

        val processingTimestamp = LocalDateTime.now().toString()
        val chunks = mutableListOf<ChunkInfo>()
        var totalLines = 0
        var currentChunk = 1
        var skippedLines = 0

        try {
            val httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(request.downloadUrl))
                .GET()
                .build()

            context.logger.log("Downloading data from Open Food Facts...\n")

            val response = httpClient.send(httpRequest, HttpResponse.BodyHandlers.ofInputStream())

            if (response.statusCode() != 200) {
                throw RuntimeException("Failed to download data: HTTP ${response.statusCode()}")
            }

            context.logger.log("Download successful, processing stream...\n")

            GZIPInputStream(response.body()).bufferedReader().use { reader ->
                val currentChunkLines = mutableListOf<String>()
                var linesInCurrentChunk = 0

                reader.lineSequence().forEachIndexed { lineIndex, line ->
                    if (line.isBlank()) {
                        skippedLines++
                        return@forEachIndexed
                    }

                    // Parse JSON only once
                    val jsonNode = try {
                        objectMapper.readTree(line)
                    } catch (e: Exception) {
                        skippedLines++
                        if (lineIndex < 10) { // Log first few invalid lines for debugging
                            context.logger.log("Skipping invalid JSON at line $lineIndex: ${line.take(100)}...\n")
                        }
                        return@forEachIndexed
                    }

                    if (shouldProcessLine(jsonNode, request.lastProcessedTimestamp, context)) {
                        currentChunkLines.add(line)
                        linesInCurrentChunk++
                        totalLines++

                        if (linesInCurrentChunk >= chunkSize) {
                            val chunkInfo = saveChunkToS3(currentChunkLines, currentChunk, processingTimestamp, context)
                            chunks.add(chunkInfo)
                            currentChunkLines.clear()
                            linesInCurrentChunk = 0
                            currentChunk++

                            context.logger.log("Saved chunk ${currentChunk - 1}, total lines processed: $totalLines\n")
                        }
                    }
                }

                if (currentChunkLines.isNotEmpty()) {
                    val chunkInfo = saveChunkToS3(currentChunkLines, currentChunk, processingTimestamp, context)
                    chunks.add(chunkInfo)
                    context.logger.log("Saved final chunk $currentChunk\n")
                }
            }

            context.logger.log("Data splitting complete. Total chunks: ${chunks.size}, Total lines: $totalLines, Skipped lines: $skippedLines\n")

            return SplitterResponse(
                chunks = chunks,
                totalLines = totalLines,
                processingTimestamp = processingTimestamp
            )

        } catch (e: Exception) {
            context.logger.log("Error in data splitting: ${e.message}\n")
            context.logger.log("Stack trace: ${e.stackTraceToString()}\n")
            throw RuntimeException("Data splitting failed: ${e.message}", e)
        }
    }

    private fun shouldProcessLine(json: JsonNode, lastProcessedTimestamp: String?, context: Context): Boolean {
        if (lastProcessedTimestamp == null) return true

        val lastModified = json.get("last_modified_t")?.asLong()

        if (lastModified != null) {
            val productTimestamp = Instant.ofEpochSecond(lastModified)
                .atZone(ZoneOffset.UTC)
                .toLocalDateTime()
                .toString()
            return productTimestamp > lastProcessedTimestamp
        } else {
            // If no timestamp, include the line
            context.logger.log("No last_modified_t field found, including line\n")
            return true
        }
    }

    private fun saveChunkToS3(lines: List<String>, chunkNumber: Int, timestamp: String, context: Context): ChunkInfo {
        val key = "etl/chunks/$timestamp/chunk-${chunkNumber.toString().padStart(4, '0')}.jsonl.gz"

        return try {
            val compressedData = ByteArrayOutputStream().use { baos ->
                GZIPOutputStream(baos).bufferedWriter().use { writer ->
                    lines.forEach { line ->
                        writer.write(line)
                        writer.newLine()
                    }
                }
                baos.toByteArray()
            }

            val putRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .contentType("application/gzip")
                .build()

            s3.putObject(putRequest, RequestBody.fromBytes(compressedData))

            context.logger.log("Uploaded chunk to s3://$bucketName/$key (${compressedData.size} bytes, ${lines.size} lines)\n")

            ChunkInfo(
                bucket = bucketName,
                key = key,
                chunkId = "chunk-$chunkNumber",
                estimatedLines = lines.size
            )
        } catch (e: Exception) {
            context.logger.log("Error saving chunk to S3: ${e.message}\n")
            throw RuntimeException("Failed to save chunk $chunkNumber to S3", e)
        }
    }
}