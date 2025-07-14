# Project Structure
energy-data-pipeline/
├── .github/
│   └── workflows/
│       ├── deploy.yml
│       └── manual-trigger.yml
├── src/
│   ├── lambda_function.py
│   ├── __init__.py
│   └── utils/
│       ├── __init__.py
│       ├── api_client.py
│       └── data_processor.py
├── tests/
│   ├── __init__.py
│   ├── test_lambda_function.py
│   ├── test_api_client.py
│   └── test_data_processor.py
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   ├── outputs.tf
│   └── terraform.tfvars
├── athena/
│   ├── create_table.sql
│   ├── create_view.sql
│   └── sample_queries.sql
├── scripts/
│   ├── deploy.sh
│   ├── test_local.py
│   └── setup_environment.sh
├── requirements.txt
├── requirements-dev.txt
├── README.md
├── .gitignore
├── .pre-commit-config.yaml
└── docker-compose.yml

---
# requirements.txt
pandas==2.0.3
requests==2.31.0
boto3==1.28.62
botocore==1.31.62

---
# requirements-dev.txt
pytest==7.4.2
pytest-cov==4.1.0
moto==4.2.5
black==23.7.0
isort==5.12.0
flake8==6.0.0
pre-commit==3.4.0

---
# .gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# Virtual environments
.env
.venv
env/
venv/
ENV/
env.bak/
venv.bak/

# IDEs
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
.DS_Store?
._*
.Spotlight-V100
.Trashes
ehthumbs.db
Thumbs.db

# Terraform
*.tfstate
*.tfstate.*
.terraform/
.terraform.lock.hcl
*.tfvars

# AWS
.aws/

# Project specific
lambda_deployment.zip
response.json
coverage.xml
.coverage
.pytest_cache/

---
# .pre-commit-config.yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.4.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-added-large-files
      - id: check-json
  
  - repo: https://github.com/psf/black
    rev: 23.7.0
    hooks:
      - id: black
        language_version: python3.9
  
  - repo: https://github.com/pycqa/isort
    rev: 5.12.0
    hooks:
      - id: isort
        args: ["--profile", "black"]
  
  - repo: https://github.com/pycqa/flake8
    rev: 6.0.0
    hooks:
      - id: flake8
        args: [--max-line-length=88, --extend-ignore=E203]

---
# terraform/variables.tf
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

variable "lambda_timeout" {
  description = "Lambda timeout in seconds"
  type        = number
  default     = 300
}

variable "lambda_memory_size" {
  description = "Lambda memory size in MB"
  type        = number
  default     = 512
}

variable "schedule_expression" {
  description = "EventBridge schedule expression"
  type        = string
  default     = "rate(1 hour)"
}

---
# terraform/outputs.tf
output "s3_bucket_name" {
  description = "Name of the S3 bucket for energy data"
  value       = aws_s3_bucket.energy_data_bucket.bucket
}

output "lambda_function_name" {
  description = "Name of the Lambda function"
  value       = aws_lambda_function.energy_data_extractor.function_name
}

output "lambda_function_arn" {
  description = "ARN of the Lambda function"
  value       = aws_lambda_function.energy_data_extractor.arn
}

output "athena_workgroup_name" {
  description = "Name of the Athena workgroup"
  value       = aws_athena_workgroup.energy_data_workgroup.name
}

output "athena_database_name" {
  description = "Name of the Athena database"
  value       = aws_athena_database.energy_data_db.name
}

output "eventbridge_rule_name" {
  description = "Name of the EventBridge rule"
  value       = aws_cloudwatch_event_rule.energy_data_schedule.name
}

---
# terraform/terraform.tfvars
aws_region = "us-east-1"
environment = "prod"
project_name = "energy-data-pipeline"
lambda_timeout = 300
lambda_memory_size = 512
schedule_expression = "rate(1 hour)"
