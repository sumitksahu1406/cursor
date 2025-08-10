provider "google" {
  project = var.project_id
  region  = var.region
}

resource "google_service_account" "service_account" {
  account_id   = var.service_account_id
  display_name = var.service_account_display_name
  description  = var.service_account_description
}

resource "google_service_account_key" "service_account_key" {
  service_account_id = google_service_account.service_account.name

  # Changing this value forces key rotation by changing the resource ID via keepers
  keepers = {
    key_rotation_epoch = var.force_key_rotation_epoch
  }
}

# Write the key JSON to a local file with restrictive permissions
resource "local_sensitive_file" "service_account_key_json" {
  filename             = var.key_output_path
  content              = base64decode(google_service_account_key.service_account_key.private_key)
  file_permission      = "0600"
  directory_permission = "0700"
}