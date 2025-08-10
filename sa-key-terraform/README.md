# GCP Service Account + Key (Terraform)

This Terraform config creates a Google Cloud service account and one key, and writes the key JSON to a local file with restrictive permissions.

Important: Service account keys are embedded in Terraform state. Protect your state (use encrypted remote backend, locked access). Prefer Workload Identity over long‑lived keys whenever possible.

## Prerequisites
- Terraform >= 1.5
- Authenticated to GCP (ADC). Options:
  - `gcloud auth application-default login`, or
  - Set `GOOGLE_APPLICATION_CREDENTIALS` to a credentials JSON path.

## Usage
```bash
cd sa-key-terraform

# Initialize providers
terraform init

# Create a service account and key
terraform apply \
  -var project_id="YOUR_PROJECT_ID" \
  -var service_account_id="my-ci-bot" \
  -var service_account_display_name="CI Bot" \
  -var key_output_path="./service-account-key.json"

# Show outputs (key path is sensitive)
terraform output

# The key JSON will be at the specified path with 0600 permissions
ls -l ./service-account-key.json
```

## Rotating the key
- Update `-var force_key_rotation_epoch=$(date +%s)` and re-apply, or
- Taint the key and re-apply:
  ```bash
  terraform taint google_service_account_key.service_account_key
  terraform apply
  ```

## Deleting only the key
```bash
terraform destroy -target google_service_account_key.service_account_key
```

## Notes
- The service account remains if you only destroy the key. Destroy all to remove everything.
- Consider binding roles to the service account separately (IAM role bindings are not included here).