terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
   project = var.project
   region  = var.region
   zone    = var.zone
 }

data "google_service_account_access_token" "default" {
   provider               = google
   target_service_account = "test-service-account@axial-trail-485514-p5.iam.gserviceaccount.com"
   scopes                 = ["https://www.googleapis.com/auth/cloud-platform"]
   lifetime               = "3600s"
 }
 
 # This second provider block uses that temporary token and does the real work
provider "google" {
   alias        = "impersonated"
   access_token = data.google_service_account_access_token.default.access_token
   project      = var.project
   region       = var.region
   zone         = var.zone
 }



resource "google_storage_bucket" "data-lake-bucket" {
  name          = "axial-trail-485514-p5-bucket"
  location      = "US"

  # Optional, but recommended settings:
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30  // days
    }
  }

  force_destroy = true
}


resource "google_bigquery_dataset" "dataset" {
  dataset_id = "ny_taxi"
  project    = var.project
  location   = "US"
}