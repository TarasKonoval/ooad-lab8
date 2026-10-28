output "stream_name" {
  description = "Pass to producer_kinesis.py --stream"
  value       = aws_kinesis_stream.payments.name
}

output "firehose_name" {
  value = aws_kinesis_firehose_delivery_stream.raw.name
}

output "raw_location" {
  description = "Where Firehose writes; LOCATION of the Athena table"
  value       = "s3://${var.bucket}/raw/"
}

output "lambda_name" {
  value = aws_lambda_function.alerts.function_name
}

output "sns_topic_arn" {
  value = aws_sns_topic.alerts.arn
}
