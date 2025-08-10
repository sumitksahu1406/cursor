output "service_account_email" {
  description = "Email of the created service account"
  value       = google_service_account.service_account.email
}

output "service_account_unique_id" {
  description = "Unique ID of the created service account"
  value       = google_service_account.service_account.unique_id
}

output "service_account_key_name" {
  description = "Resource name of the created key"
  value       = google_service_account_key.service_account_key.name
}

output "key_file_path" {
  description = "Local filesystem path where the key JSON was written"
  value       = local_sensitive_file.service_account_key_json.filename
  sensitive   = true
}