package com.example.chunkprocessor

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.example.lambda.ProductImporter
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import java.io.BufferedInputStream
import java.io.InputStream
import java.util.zip.GZIPInputStream

class ChunkProcessorLambda : RequestHandler<ChunkProcessorRequest, ChunkProcessorResponse> {

    private val s3 = S3Client.create()
    private val importer = ProductImporter()

    override fun handleRequest(input: ChunkProcessorRequest, context: Context): ChunkProcessorResponse {
        context.logger.log("Processing chunk: ${input.chunkKey}\n")

        val startTime = System.currentTimeMillis()
        return try {
            val result = processChunk(input, context)
            val processingTime = System.currentTimeMillis() - startTime

            ChunkProcessorResponse(
                processedItems = result.first,
                successfulItems = result.second,
                failedItems = result.first - result.second,
                processingTimeMs = processingTime,
                message = "Processed ${result.first} items, ${result.second} successful"
            )
        } catch (e: Exception) {
            context.logger.log("Error processing chunk: ${e.message}\n")
            throw e
        }
    }

    private fun processChunk(request: ChunkProcessorRequest, context: Context): Pair<Int, Int> {
        val log = context.logger
        log.log("Downloading chunk from s3://${request.bucketName}/${request.chunkKey}\n")

        val req = GetObjectRequest.builder()
            .bucket(request.bucketName)
            .key(request.chunkKey)
            .build()

        s3.getObject(req).use { response ->
            val stream = wrapMaybeGzip(response, context)
            val imported = importer.importJsonl(stream, context)
            return Pair(imported, imported)
        }
    }

    private fun wrapMaybeGzip(input: InputStream, context: Context): InputStream {
        val mode = System.getenv("S3_READ_GZIP")?.lowercase() ?: "auto"
        context.logger.log("GZIP mode: $mode\n")

        return when (mode) {
            "true" -> {
                context.logger.log("Force GZIP decompression\n")
                GZIPInputStream(input)
            }
            "false" -> {
                context.logger.log("No GZIP decompression\n")
                input
            }
            else -> {
                context.logger.log("Auto-detecting GZIP\n")
                val bis = BufferedInputStream(input)
                bis.mark(2)
                val b1 = bis.read()
                val b2 = bis.read()
                bis.reset()

                if (b1 == 0x1f && b2 == 0x8b) {
                    context.logger.log("GZIP magic bytes detected, decompressing\n")
                    GZIPInputStream(bis)
                } else {
                    context.logger.log("No GZIP magic bytes, treating as plain text\n")
                    bis
                }
            }
        }
    }
}
