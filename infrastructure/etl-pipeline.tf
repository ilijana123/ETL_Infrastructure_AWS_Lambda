# Step Functions State Machine
resource "aws_sfn_state_machine" "openfoodfacts_etl" {
  name     = "openfoodfacts-etl"
  role_arn = aws_iam_role.stepfunctions_role.arn

  definition = jsonencode({
    Comment = "Open Food Facts ETL Pipeline"
    StartAt = "CheckForNewData"
    States = {
      CheckForNewData = {
        Type = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.change_detector.function_name
          # Instead of nesting under Payload, send directly
          "Payload" = {
            action = "check_for_updates"
          }
        }
        ResultPath = "$.changeDetection"
        Next = "HasNewData?"
      }
      "HasNewData?" = {
        Type = "Choice"
        Choices = [{
          Variable = "$.changeDetection.Payload.hasUpdates"
          BooleanEquals = true
          Next = "DownloadAndSplitData"
        }]
        Default = "NoUpdatesFound"
      }
      NoUpdatesFound = {
        Type = "Pass"
        Result = "No new data to process"
        End = true
      }
      DownloadAndSplitData = {
        Type = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.data_splitter.function_name
          Payload = {
            "downloadUrl.$" = "$.changeDetection.Payload.downloadUrl"
            "lastProcessedTimestamp.$" = "$.changeDetection.Payload.lastProcessedTimestamp"
          }
        }
        ResultPath = "$.splitResult"
        Next = "ProcessChunksInParallel"
        TimeoutSeconds = 900
        Retry = [{
          ErrorEquals = ["States.ALL"]
          IntervalSeconds = 30
          MaxAttempts = 3
          BackoffRate = 2.0
        }]
      }
      ProcessChunksInParallel = {
        Type = "Map"
        ItemsPath = "$.splitResult.Payload.chunks"
        MaxConcurrency = 10 # Free tier friendly
        Parameters = {
          "chunkInfo.$" = "$"
          "executionId.$" = "$.Execution.Name"
        }
        Iterator = {
          StartAt = "ProcessSingleChunk"
          States = {
            ProcessSingleChunk = {
              Type = "Task"
              Resource = "arn:aws:states:::lambda:invoke"
              Parameters = {
                FunctionName = aws_lambda_function.chunk_processor.function_name
                Payload = {
                  "bucket.$" = "$.chunkInfo.bucket"
                  "key.$" = "$.chunkInfo.key"
                  "chunkId.$" = "$.chunkInfo.chunkId"
                }
              }
              TimeoutSeconds = 300
              Retry = [{
                ErrorEquals = ["States.ALL"]
                IntervalSeconds = 60
                MaxAttempts = 3
                BackoffRate = 2.0
              }]
              End = true
            }
          }
        }
        ResultPath = "$.processingResults"
        Next = "CompileResults"
      }
      CompileResults = {
        Type = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.results_compiler.function_name
          Payload = {
            "processingResults.$" = "$.processingResults"
            "executionId.$" = "$.Execution.Name"
          }
        }
        ResultPath = "$.finalResults"
        Next = "UpdateLastProcessedTimestamp"
      }
      UpdateLastProcessedTimestamp = {
        Type = "Task"
        Resource = "arn:aws:states:::lambda:invoke"
        Parameters = {
          FunctionName = aws_lambda_function.change_detector.function_name
          "Payload" = {
            action       = "update_timestamp"
            "newTimestamp.$" = "$.changeDetection.Payload.latestTimestamp"
          }
        }
        ResultPath = "$.timestampUpdate"
        Next = "NotifyCompletion"
      }
      NotifyCompletion = {
        Type = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn = aws_sns_topic.etl_notifications.arn
          Message = {
            "executionId.$" = "$.Execution.Name"
            "totalProcessed.$" = "$.finalResults.Payload.totalProcessed"
            "successfulChunks.$" = "$.finalResults.Payload.successfulChunks"
            "failedChunks.$" = "$.finalResults.Payload.failedChunks"
            "processingTime.$" = "$.finalResults.Payload.processingTimeMinutes"
          }
        }
        End = true
      }
    }
  })
}

# Lambda Functions
resource "aws_lambda_function" "change_detector" {
  function_name = "openfoodfacts-change-detector"
  role          = aws_iam_role.lambda_role.arn
  handler       = "com.example.changedetector.ChangeDetectorLambda::handleRequest"
  runtime       = "java17"
  timeout       = 60
  memory_size   = 512

  filename         = "${path.module}/../change-detector/build/libs/change-detector-all.jar"
  source_code_hash = filebase64sha256("${path.module}/../change-detector/build/libs/change-detector-all.jar")

  environment {
    variables = {
      PROCESSING_BUCKET = aws_s3_bucket.processing_bucket.bucket
    }
  }
}

resource "aws_lambda_function" "data_splitter" {
  function_name = "openfoodfacts-splitter"
  role          = aws_iam_role.lambda_role.arn
  handler       = "com.example.datasplitter.DataSplitterLambda::handleRequest"
  runtime       = "java17"
  timeout       = 900
  memory_size   = 3008

  filename         = "${path.module}/../data-splitter/build/libs/data-splitter-all.jar"
  source_code_hash = filebase64sha256("${path.module}/../data-splitter/build/libs/data-splitter-all.jar")

  environment {
    variables = {
      PROCESSING_BUCKET = aws_s3_bucket.processing_bucket.bucket
      CHUNK_SIZE        = "5000"
    }
  }
}

resource "aws_lambda_function" "chunk_processor" {
  function_name = "openfoodfacts-chunk-processor"
  role          = aws_iam_role.lambda_role.arn
  handler       = "com.example.chunkprocessor.ChunkProcessorLambda::handleRequest"
  runtime       = "java17"
  timeout       = 300
  memory_size   = 1024

  filename         = "${path.module}/../chunk-processor/build/libs/chunk-processor-all.jar"
  source_code_hash = filebase64sha256("${path.module}/../chunk-processor/build/libs/chunk-processor-all.jar")

  environment {
    variables = {
      JDBC_URL    = var.database_url
      DB_USER     = var.database_user
      DB_PASSWORD = var.database_password
      BATCH_SIZE  = "200"
    }
  }
}

resource "aws_lambda_function" "results_compiler" {
  function_name = "openfoodfacts-results-compiler"
  role          = aws_iam_role.lambda_role.arn
  handler       = "com.example.resultscompiler.ResultsCompilerLambda::handleRequest"
  runtime       = "java17"
  timeout       = 60
  memory_size   = 512

  filename         = "${path.module}/../results-compiler/build/libs/results-compiler-all.jar"
  source_code_hash = filebase64sha256("${path.module}/../results-compiler/build/libs/results-compiler-all.jar")

  environment {
    variables = {
      PROCESSING_BUCKET = aws_s3_bucket.processing_bucket.bucket
    }
  }
}

# S3 Bucket for processing
resource "aws_s3_bucket" "processing_bucket" {
  bucket = "openfoodfacts-etl-processing-${random_id.bucket_suffix.hex}"
}

resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# SNS Topic for notifications
resource "aws_sns_topic" "etl_notifications" {
  name = "openfoodfacts-etl-notifications"
}

# EventBridge rule for daily execution
resource "aws_cloudwatch_event_rule" "daily_etl" {
  name                = "openfoodfacts-daily-etl"
  description         = "Trigger ETL pipeline daily"
  schedule_expression = "cron(0 2 * * ? *)" # 2 AM UTC daily
}

resource "aws_cloudwatch_event_target" "stepfunctions" {
  rule      = aws_cloudwatch_event_rule.daily_etl.name
  target_id = "StepFunctionsTarget"
  arn       = aws_sfn_state_machine.openfoodfacts_etl.arn
  role_arn  = aws_iam_role.eventbridge_role.arn
}

# IAM Roles
resource "aws_iam_role" "stepfunctions_role" {
  name = "openfoodfacts-stepfunctions-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "states.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role" "lambda_role" {
  name = "openfoodfacts-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })
}

resource "aws_iam_role" "eventbridge_role" {
  name = "openfoodfacts-eventbridge-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "events.amazonaws.com"
      }
    }]
  })
}

# IAM Policies
resource "aws_iam_role_policy" "stepfunctions_policy" {
  name = "stepfunctions-policy"
  role = aws_iam_role.stepfunctions_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "lambda:InvokeFunction"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "sns:Publish"
        ]
        Resource = aws_sns_topic.etl_notifications.arn
      }
    ]
  })
}

resource "aws_iam_role_policy" "lambda_policy" {
  name = "lambda-policy"
  role = aws_iam_role.lambda_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = "${aws_s3_bucket.processing_bucket.arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = aws_s3_bucket.processing_bucket.arn
      }
    ]
  })
}

resource "aws_iam_role_policy" "eventbridge_policy" {
  name = "eventbridge-policy"
  role = aws_iam_role.eventbridge_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "states:StartExecution"
      ]
      Resource = aws_sfn_state_machine.openfoodfacts_etl.arn
    }]
  })
}