# terraform/main.tf
terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# Variables
variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "prod"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "energy-data-pipeline"
}

# S3 Bucket for energy data
resource "aws_s3_bucket" "energy_data_bucket" {
  bucket = "s3-for-energy"
  
  tags = {
    Name        = "Energy Data Storage"
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_s3_bucket_versioning" "energy_data_bucket_versioning" {
  bucket = aws_s3_bucket.energy_data_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "energy_data_bucket_encryption" {
  bucket = aws_s3_bucket.energy_data_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# IAM Role for Lambda
resource "aws_iam_role" "lambda_execution_role" {
  name = "${var.project_name}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

# IAM Policy for Lambda
resource "aws_iam_policy" "lambda_policy" {
  name        = "${var.project_name}-lambda-policy"
  description = "IAM policy for energy data pipeline lambda"

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
        Resource = "${aws_s3_bucket.energy_data_bucket.arn}/*"
      },
      {
        Effect = "Allow"
        Action = [
          "s3:ListBucket"
        ]
        Resource = aws_s3_bucket.energy_data_bucket.arn
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda_policy_attachment" {
  role       = aws_iam_role.lambda_execution_role.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}

# Lambda function
resource "aws_lambda_function" "energy_data_extractor" {
  filename         = "lambda_deployment.zip"
  function_name    = "${var.project_name}-data-extractor"
  role            = aws_iam_role.lambda_execution_role.arn
  handler         = "lambda_function.lambda_handler"
  runtime         = "python3.9"
  timeout         = 300
  memory_size     = 512

  environment {
    variables = {
      S3_BUCKET_NAME = aws_s3_bucket.energy_data_bucket.bucket
    }
  }

  tags = {
    Name        = "Energy Data Extractor"
    Environment = var.environment
    Project     = var.project_name
  }
}

# EventBridge Rule for scheduled execution
resource "aws_cloudwatch_event_rule" "energy_data_schedule" {
  name        = "${var.project_name}-schedule"
  description = "Trigger energy data extraction hourly"
  
  schedule_expression = "rate(1 hour)"
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  rule      = aws_cloudwatch_event_rule.energy_data_schedule.name
  target_id = "EnergyDataLambdaTarget"
  arn       = aws_lambda_function.energy_data_extractor.arn
}

resource "aws_lambda_permission" "allow_eventbridge" {
  statement_id  = "AllowExecutionFromEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.energy_data_extractor.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.energy_data_schedule.arn
}

# Athena resources
resource "aws_s3_bucket" "athena_results" {
  bucket = "${var.project_name}-athena-results"
  
  tags = {
    Name        = "Athena Query Results"
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_athena_workgroup" "energy_data_workgroup" {
  name = "${var.project_name}-workgroup"

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics         = true

    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/query-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

resource "aws_athena_database" "energy_data_db" {
  name   = "energy_data_db"
  bucket = aws_s3_bucket.energy_data_bucket.bucket
}

# CloudWatch Log Group for Lambda
resource "aws_cloudwatch_log_group" "lambda_logs" {
  name              = "/aws/lambda/${aws_lambda_function.energy_data_extractor.function_name}"
  retention_in_days = 14
}

# Outputs
output "s3_bucket_name" {
  description = "Name of the S3 bucket for energy data"
  value       = aws_s3_bucket.energy_data_bucket.bucket
}

output "lambda_function_name" {
  description = "Name of the Lambda function"
  value       = aws_lambda_function.energy_data_extractor.function_name
}

output "athena_workgroup_name" {
  description = "Name of the Athena workgroup"
  value       = aws_athena_workgroup.energy_data_workgroup.name
}

output "athena_database_name" {
  description = "Name of the Athena database"
  value       = aws_athena_database.energy_data_db.name
}
