resource "aws_iam_role" "emr_serverless_role" {
  name = local.app_role_name

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "emr-serverless.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
  tags = {
    Name = local.app_role_name
  }
}

resource "aws_iam_policy" "emr_policy" {
  name = "emr-serverless-policy"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        "Sid": "bucketActions",
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket",
          "s3:PutObject"
        ]
        Resource = [
          "arn:aws:s3:::${var.source_bucket}/*",
          "arn:aws:s3:::${var.artifacts_bucket}/*",
          "arn:aws:s3:::${var.catalog_bucket}/*",
          "arn:aws:s3:::${var.catalog_bucket}"
        ]
      },
      {
        "Sid": "GlueCatalog",
        Effect = "Allow"
        Action = [
          "glue:*"
        ]
        Resource = "*"
      },
      {
        Sid    = "Logs"
        Effect = "Allow"
        Action = [
          "logs:*"
        ]
        Resource = "*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_policy" {
  role       = aws_iam_role.emr_serverless_role.name
  policy_arn = aws_iam_policy.emr_policy.arn
}