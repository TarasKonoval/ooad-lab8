variable "region" {
  description = "AWS region for every resource"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Value of the PROJECT_NAME tag and prefix of resource names"
  type        = string
  default     = "ooad-lab8"
}

variable "bucket" {
  description = "Existing lab8 bucket (created in Part 2, not managed here)"
  type        = string
  default     = "ooad-lab8-669977209400"
}

variable "alert_email" {
  description = "Email address that receives the SNS alerts (must confirm the subscription)"
  type        = string
}

variable "failure_threshold" {
  description = "Alert when the failure share of a payment method is above this"
  type        = number
  default     = 0.10
}

variable "min_payments" {
  description = "Alert only if the method had at least this many finished payments in the window"
  type        = number
  default     = 20
}
