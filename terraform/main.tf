terraform {
  required_providers {
    minio = {
      source  = "aminueza/minio"
      version = ">= 1.0.0"
    }
  }
}

provider "minio" {
  minio_server = "localhost:9000"

  minio_user     = "minioadmin"
  minio_password = "minioadmin"
  minio_ssl      = false
}

locals {
  buckets = [
    "raw",
    "staging",
    "intermediate",
    "marts"
  ]
}

resource "minio_s3_bucket" "data_lake" {
  for_each = toset(local.buckets)

  bucket        = each.value
  acl           = "private"
  force_destroy = true
}
