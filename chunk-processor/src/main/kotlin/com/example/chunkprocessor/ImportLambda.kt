package com.example.lambda

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import java.io.BufferedInputStream
import java.io.InputStream
import java.util.zip.GZIPInputStream
import java.net.URI

class ImportLambda : RequestHandler<Map<String, Any>, String> {

    private val s3 = S3Client.create()
    private val importer = ProductImporter()
    private val objectMapper = jacksonObjectMapper()

    override fun handleRequest(event: Map<String, Any>, context: Context): String {
        context.logger.log("=== LAMBDA START ===\n")
        context.logger.log("Full event received: ${objectMapper.writeValueAsString(event)}\n")

        var total = 0

        try {
            val records = event["Records"] as? List<*> ?: emptyList<Any>()
            context.logger.log("Found ${records.size} SQS records in event\n")

            if (records.isEmpty()) {
                context.logger.log("WARNING: No records found in event\n")
                return "OK: 0 - No records to process"
            }

            for ((index, record) in records.withIndex()) {
                context.logger.log("Processing SQS record $index: $record\n")

                val recordMap = record as? Map<*, *> ?: continue

                val eventSource = recordMap["eventSource"] as? String
                if (eventSource != "aws:sqs") {
                    context.logger.log("Skipping non-SQS record: $eventSource\n")
                    continue
                }

                val body = recordMap["body"] as? String
                context.logger.log("SQS message body: $body\n")

                if (body == null) {
                    context.logger.log("ERROR: No body in SQS message\n")
                    continue
                }

                if (!body.startsWith("s3://")) {
                    context.logger.log("ERROR: Body is not an S3 URI: $body\n")
                    continue
                }

                try {
                    val s3Uri = URI(body)
                    val bucket = s3Uri.host
                    val key = s3Uri.path.removePrefix("/")

                    context.logger.log("Extracted - Bucket: $bucket, Key: $key\n")

                    if (bucket == null || key.isEmpty()) {
                        context.logger.log("ERROR: Invalid S3 URI - bucket: $bucket, key: $key\n")
                        continue
                    }

                    context.logger.log("Processing s3://$bucket/$key\n")

                    val req = GetObjectRequest.builder().bucket(bucket).key(key).build()
                    context.logger.log("Getting object from S3...\n")

                    s3.getObject(req).use { response ->
                        context.logger.log("S3 object retrieved, content length: ${response.response().contentLength()}\n")

                        val stream = wrapMaybeGzip(response, context)
                        context.logger.log("Stream wrapped, calling importer...\n")

                        val imported = importer.importJsonl(stream, context)
                        total += imported
                        context.logger.log("Importer returned: $imported rows from $key\n")
                    }

                } catch (uriError: Exception) {
                    context.logger.log("ERROR parsing S3 URI '$body': ${uriError.message}\n")
                    continue
                }

            }
        } catch (e: Exception) {
            context.logger.log("FATAL ERROR in handleRequest: ${e.javaClass.simpleName}: ${e.message}\n")
            e.printStackTrace()
            throw e
        }

        context.logger.log("=== LAMBDA END ===\n")
        context.logger.log("Total imported: $total rows\n")
        return "OK: $total"
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