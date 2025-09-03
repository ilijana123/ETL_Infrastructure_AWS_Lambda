package com.example.changedetector

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import software.amazon.awssdk.services.s3.model.PutObjectRequest
import software.amazon.awssdk.core.sync.RequestBody
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.URI
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class ChangeDetectorLambda : RequestHandler<Map<String, Any>, ChangeDetectionResponse> {

    private val s3 = S3Client.create()
    private val httpClient = HttpClient.newHttpClient()
    private val bucketName = System.getenv("PROCESSING_BUCKET") ?: "nutritiveappbucket"
    private val timestampKey = "etl/last-processed-timestamp.txt"
    private val openFoodFactsUrl = "https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.jsonl.gz"

    override fun handleRequest(event: Map<String, Any>, context: Context): ChangeDetectionResponse {
        context.logger.log("Incoming event: $event\n")

        val payload = (event["Payload"] as? Map<*, *>) ?: event
        val action = payload["action"] as? String ?: "unknown"
        val newTimestamp = payload["newTimestamp"] as? String

        return when (action) {
            "check_for_updates" -> checkForUpdates(context)
            "update_timestamp" -> updateTimestamp(newTimestamp!!, context)
            else -> ChangeDetectionResponse(false, message = "Unknown action: $action")
        }
    }

    private fun checkForUpdates(context: Context): ChangeDetectionResponse {
        try {
            val lastProcessed = getLastProcessedTimestamp(context)
            context.logger.log("Last processed timestamp: $lastProcessed\n")

            val latestTimestamp = getLatestExportTimestamp(context)
            context.logger.log("Latest export timestamp: $latestTimestamp\n")

            val hasUpdates = lastProcessed == null || latestTimestamp > lastProcessed

            return if (hasUpdates) {
                ChangeDetectionResponse(
                    hasUpdates = true,
                    downloadUrl = openFoodFactsUrl,
                    lastProcessedTimestamp = lastProcessed,
                    latestTimestamp = latestTimestamp,
                    message = "New data available for processing"
                )
            } else {
                ChangeDetectionResponse(
                    hasUpdates = false,
                    message = "No new data available"
                )
            }

        } catch (e: Exception) {
            context.logger.log("Error checking for updates: ${e.message}\n")
            e.printStackTrace()
            throw e
        }
    }

    private fun updateTimestamp(newTimestamp: String, context: Context): ChangeDetectionResponse {
        try {
            val putRequest = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(timestampKey)
                .build()

            s3.putObject(putRequest, RequestBody.fromString(newTimestamp))

            context.logger.log("Updated last processed timestamp to: $newTimestamp\n")

            return ChangeDetectionResponse(
                hasUpdates = false,
                message = "Timestamp updated successfully"
            )

        } catch (e: Exception) {
            context.logger.log("Error updating timestamp: ${e.message}\n")
            e.printStackTrace()
            throw e
        }
    }

    private fun getLastProcessedTimestamp(context: Context): String? {
        return try {
            val getRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(timestampKey)
                .build()

            s3.getObjectAsBytes(getRequest).asUtf8String().trim()
        } catch (e: Exception) {
            context.logger.log("No previous timestamp found, treating as first run\n")
            null
        }
    }

    private fun getLatestExportTimestamp(context: Context): String {
        val request = HttpRequest.newBuilder()
            .uri(URI.create(openFoodFactsUrl))
            .method("HEAD", HttpRequest.BodyPublishers.noBody())
            .build()

        val response = httpClient.send(request, HttpResponse.BodyHandlers.discarding())

        val lastModified = response.headers().firstValue("Last-Modified")
            .orElse(LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME))

        context.logger.log("Export file Last-Modified: $lastModified\n")

        return try {
            val httpDate = DateTimeFormatter.RFC_1123_DATE_TIME.parse(lastModified)
            LocalDateTime.from(httpDate).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        } catch (e: Exception) {
            context.logger.log("Error parsing date, using current time: ${e.message}\n")
            LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
        }
    }
}