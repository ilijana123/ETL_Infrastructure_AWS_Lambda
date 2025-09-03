package com.example.datasplitter

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
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

class DataSplitterLambda : RequestHandler<SplitterRequest, SplitterResponse> {

    private val s3 = S3Client.create()
    private val httpClient = HttpClient.newHttpClient()
    private val objectMapper = jacksonObjectMapper()

    private val bucketName = System.getenv("PROCESSING_BUCKET") ?: "nutritiveappbucket"
    private val chunkSize = System.getenv("CHUNK_SIZE")?.toIntOrNull() ?: 5000 // lines per chunk

    override fun handleRequest(request: SplitterRequest, context: Context): SplitterResponse {
        context.logger.log("Starting data download and splitting from: ${request.downloadUrl}\n")

        val processingTimestamp = LocalDateTime.now().toString()
        val chunks = mutableListOf<ChunkInfo>()
        var totalLines = 0
        var currentChunk = 1

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

                reader.lineSequence().forEach { line ->
                    if (line.isBlank()) return@forEach

                    if (shouldProcessLine(line, request.lastProcessedTimestamp, context)) {
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

            context.logger.log("Data splitting complete. Total chunks: ${chunks.size}, Total lines: $totalLines\n")

            return SplitterResponse(
                chunks = chunks,
                totalLines = totalLines,
                processingTimestamp = processingTimestamp
            )

        } catch (e: Exception) {
            context.logger.log("Error in data splitting: ${e.message}\n")
            e.printStackTrace()
            throw e
        }
    }

    private fun shouldProcessLine(line: String, lastProcessedTimestamp: String?, context: Context): Boolean {
        if (lastProcessedTimestamp == null) return true

        try {
            val json = objectMapper.readTree(line)
            val lastModified = json.get("last_modified_t")?.asLong()

            if (lastModified != null) {
                val productTimestamp = Instant.ofEpochSecond(lastModified)
                    .atZone(ZoneOffset.UTC)
                    .toLocalDateTime()
                    .toString()
                return productTimestamp > lastProcessedTimestamp
            }
        } catch (e: Exception) {
            context.logger.log("Warning: Could not parse timestamp for line, including it: ${e.message}\n")
        }

        return true
    }

    private fun saveChunkToS3(lines: List<String>, chunkNumber: Int, timestamp: String, context: Context): ChunkInfo {
        val key = "etl/chunks/$timestamp/chunk-${chunkNumber.toString().padStart(4, '0')}.jsonl.gz"
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

        return ChunkInfo(
            bucket = bucketName,
            key = key,
            chunkId = "chunk-$chunkNumber",
            estimatedLines = lines.size
        )
    }
}