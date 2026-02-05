resource "aws_s3_bucket" "artifacts" {
  bucket = var.artifacts_bucket

  tags = {
    Environment = var.environment
    Project     = var.app_name
  }
}

resource "aws_s3_bucket" "airflow" {
  bucket = var.airflow_bucket

  tags = {
    Environment = var.environment
    Project     = var.app_name
  }
}
