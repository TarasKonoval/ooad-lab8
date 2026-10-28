# Terraform and provider versions. State is kept locally in terraform.tfstate (gitignored).
terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
    # Zips the Lambda source code during plan/apply.
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }
  }
}

# Credentials come from the environment (source ../../scripts/aws-env.sh).
provider "aws" {
  region = var.region

  # Every resource that supports tags gets PROJECT_NAME=ooad-lab8.
  default_tags {
    tags = {
      PROJECT_NAME = var.project_name
    }
  }
}
