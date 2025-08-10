variable "project_id" {
  description = "GCP project ID to create the service account in"
  type        = string
}

variable "region" {
  description = "Default region for the Google provider"
  type        = string
  default     = "us-central1"
}

variable "service_account_id" {
  description = "Service account ID (without domain), e.g. my-ci-bot"
  type        = string
}

variable "service_account_display_name" {
  description = "Display name for the service account"
  type        = string
  default     = "Terraform-created service account"
}

variable "service_account_description" {
  description = "Description for the service account"
  type        = string
  default     = ""
}

variable "key_output_path" {
  description = "Path on disk to write the service account key JSON"
  type        = string
  default     = "service-account-key.json"
}

variable "force_key_rotation_epoch" {
  description = "Change this value (e.g. to a timestamp) to force key rotation"
  type        = string
  default     = ""
}