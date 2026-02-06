resource "aws_glue_job" "glue_job" {
  name = var.glue_job

  role_arn = "arn:aws:iam::${var.account}:role/${local.app_role_name}"
  glue_version = "4.0"
  worker_type = "G.1X"
  number_of_workers=2
  timeout=5

  default_arguments = {
    "--extra-files" = "s3://${var.artifacts_bucket}/glue_jobs/${var.app_name}/pyfiles.zip"
    "--enable-auto-scaling" = false
    "--enable-glue-datacatalog" = true
    "--additional-python-modules" = "s3://${var.artifacts_bucket}/glue_jobs/${var.app_name}/glue_job_whl_packages.zip"
    "--python-modules-installer-option" = "--no-index"
    "--ENV" = var.environment
    "--script_args" = "arg_1"
  }

  command {
    python_version = 3
    script_location = "s3://${var.artifacts_bucket}/glue_jobs/${var.app_name}/main.py"
  }

  execution_property {
    max_concurrent_runs = 2
  }

  tags = {
    Environment = var.environment
    Project     = var.app_name
  }
}