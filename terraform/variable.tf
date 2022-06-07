variable "region" {
  description = "aws region"
  default     = "us-east-2"
}

variable "account_id" {
  default = 777696598735
}

variable "prefix" {
  description = "objects prefix"
  default     = "owshq"
}

# Prefix configuration and project common tags
locals {
  glue_bucket = "${var.prefix}-${var.bucket_names[6]}-${var.account_id}"
  prefix      = var.prefix
  common_tags = {
    Project = "trn-cc-bg-aws"
  }
}

variable "bucket_names" {
  description = "s3 bucket names"
  type        = list(string)
  default = [
    "landing-zone",
    "bronze",
    "silver",
    "gold",
    "processing-zone",
    "consumer-zone",
    "aws-glue-scripts"
  ]
}

variable "glue_job_role_arn" {
  description = "The ARN of the IAM role associated with this job."
  default     = null
}