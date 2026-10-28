# Part 4: producer -> Kinesis Data Stream -> (a) Firehose -> S3 raw/  (b) Lambda -> SNS email.

locals {
  name       = var.project_name # "ooad-lab8"
  bucket_arn = "arn:aws:s3:::${var.bucket}"
}

# ---------------------------------------------------------------- Kinesis

# One shard in PROVISIONED mode: $0.015 per shard-hour, the cheapest option.
# One shard takes up to 1,000 records/s or 1 MB/s; we send about 60 events/s.
resource "aws_kinesis_stream" "payments" {
  name             = "${local.name}-payments"
  shard_count      = 1
  retention_period = 24 # hours; the minimum (longer retention costs extra)

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}

# ---------------------------------------------------------------- Firehose -> S3

# Firehose writes its own errors here, so we can see why a delivery failed.
resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${local.name}-payments-raw"
  retention_in_days = 7
}

resource "aws_cloudwatch_log_stream" "firehose" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}

# The role Firehose assumes: read the stream, write to the bucket, write its logs.
resource "aws_iam_role" "firehose" {
  name = "${local.name}-firehose"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "firehose.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "firehose" {
  name = "read-stream-write-raw"
  role = aws_iam_role.firehose.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        # Read records from our stream only.
        Effect   = "Allow"
        Action   = ["kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:GetShardIterator", "kinesis:GetRecords", "kinesis:ListShards"]
        Resource = aws_kinesis_stream.payments.arn
      },
      {
        # Bucket-level calls Firehose needs before uploading.
        Effect   = "Allow"
        Action   = ["s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads"]
        Resource = local.bucket_arn
      },
      {
        # Write objects only under raw/ and raw-errors/.
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:GetObject", "s3:AbortMultipartUpload"]
        Resource = ["${local.bucket_arn}/raw/*", "${local.bucket_arn}/raw-errors/*"]
      },
      {
        Effect   = "Allow"
        Action   = "logs:PutLogEvents"
        Resource = "${aws_cloudwatch_log_group.firehose.arn}:*"
      }
    ]
  })
}

# Firehose reads the stream and writes a file to S3 every 60 s (or 5 MB, whichever comes first).
resource "aws_kinesis_firehose_delivery_stream" "raw" {
  name        = "${local.name}-payments-raw"
  destination = "extended_s3"

  # Source: our Kinesis stream (Firehose polls it; no producer change needed).
  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.payments.arn
    role_arn           = aws_iam_role.firehose.arn
  }

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose.arn
    bucket_arn = local.bucket_arn

    # dt = the day Firehose received the data (arrival time, UTC), Hive style for Athena.
    prefix              = "raw/dt=!{timestamp:yyyy-MM-dd}/"
    error_output_prefix = "raw-errors/!{firehose:error-output-type}/dt=!{timestamp:yyyy-MM-dd}/"

    buffering_interval = 60 # seconds
    buffering_size     = 5  # MB; at ~30 KB/s the 60 s interval always comes first

    # Plain JSON Lines, so the files can be opened and read as-is.
    compression_format = "UNCOMPRESSED"

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose.name
    }
  }
}

# ---------------------------------------------------------------- SNS

resource "aws_sns_topic" "alerts" {
  name = "${local.name}-failure-alerts"
}

# AWS sends a confirmation email; alerts arrive only after you click "Confirm subscription".
resource "aws_sns_topic_subscription" "email" {
  topic_arn = aws_sns_topic.alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

# ---------------------------------------------------------------- Lambda

# Zip ../lambda/alert_handler.py into build/alert_handler.zip.
data "archive_file" "alert_handler" {
  type        = "zip"
  source_file = "${path.module}/../lambda/alert_handler.py"
  output_path = "${path.module}/build/alert_handler.zip"
}

resource "aws_iam_role" "lambda" {
  name = "${local.name}-alert-lambda"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# AWS-managed policy: read from Kinesis and write CloudWatch logs.
resource "aws_iam_role_policy_attachment" "lambda_kinesis" {
  role       = aws_iam_role.lambda.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole"
}

# Publish to our alert topic only.
resource "aws_iam_role_policy" "lambda_sns" {
  name = "publish-alerts"
  role = aws_iam_role.lambda.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = "sns:Publish"
      Resource = aws_sns_topic.alerts.arn
    }]
  })
}

# Created here (not by Lambda on first run) so terraform destroy removes it too.
resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${local.name}-failure-alerts"
  retention_in_days = 7
}

resource "aws_lambda_function" "alerts" {
  function_name    = "${local.name}-failure-alerts"
  role             = aws_iam_role.lambda.arn
  runtime          = "python3.13"
  handler          = "alert_handler.handler" # file alert_handler.py, function handler()
  filename         = data.archive_file.alert_handler.output_path
  source_code_hash = data.archive_file.alert_handler.output_base64sha256 # redeploy when the code changes
  memory_size      = 128
  timeout          = 30

  environment {
    variables = {
      SNS_TOPIC_ARN = aws_sns_topic.alerts.arn
      THRESHOLD     = tostring(var.failure_threshold)
      MIN_PAYMENTS  = tostring(var.min_payments)
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda, aws_iam_role_policy_attachment.lambda_kinesis]
}

# The poller that reads the shard and invokes the Lambda.
# tumbling_window_in_seconds = 60: Lambda groups records into 60 s windows by ARRIVAL time
# and passes our running totals ("state") from one invocation to the next within a window.
resource "aws_lambda_event_source_mapping" "alerts" {
  event_source_arn                   = aws_kinesis_stream.payments.arn
  function_name                      = aws_lambda_function.alerts.arn
  starting_position                  = "LATEST" # only records written after the mapping exists
  batch_size                         = 500      # up to 500 records per invocation
  maximum_batching_window_in_seconds = 5        # or whatever arrived within 5 s
  tumbling_window_in_seconds         = 60
  maximum_retry_attempts             = 2 # a failing batch is retried twice, then skipped
}
