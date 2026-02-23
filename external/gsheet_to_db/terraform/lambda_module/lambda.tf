data "aws_iam_role" "system_admin_role" {
  name = "system_admin"
}

resource "aws_lambda_function" "gsheet_to_db" {
  function_name = "gsheet_to_db"
  description   = "Fetch Google Sheet data and full-load into Postgres"
  role          = data.aws_iam_role.system_admin_role.arn
  package_type  = "Image"
  timeout       = 60
  memory_size   = 128

  image_uri = "282654131095.dkr.ecr.us-east-1.amazonaws.com/gsheet_to_db:latest"

  # lifecycle {
  #   # Prevent accidental deletion
  #   prevent_destroy = true
  #   # Ignore changes to tags/metadata that might be modified outside Terraform
  #   # This ensures updates (like env vars) don't trigger recreation
  #   ignore_changes = [
  #     tags,
  #     last_modified,
  #   ]
  # }

  environment {
    variables = {
      PG_DB_NAME          = var.PG_DB_NAME
      PG_DB_USER          = var.PG_DB_USER
      PG_DB_PASSWORD      = var.PG_DB_PASSWORD
      PG_ENDPOINT         = var.PG_ENDPOINT
      PG_PORT             = var.PG_PORT
      GOOGLE_CLIENT_EMAIL = var.GOOGLE_CLIENT_EMAIL
      GOOGLE_CLIENT_ID    = var.GOOGLE_CLIENT_ID
      GOOGLE_PRIVATE_KEY  = var.GOOGLE_PRIVATE_KEY
      GOOGLE_PROJECT_ID   = var.GOOGLE_PROJECT_ID
      GOOGLE_SHEET_ID     = var.GOOGLE_SHEET_ID
      # WORKSHEET_NAME      = var.WORKSHEET_NAME
      # TARGET_SCHEMA       = var.SCHEMA
      # TARGET_TABLE        = var.TABLE
    }
  }
}

resource "aws_lambda_function_url" "gsheet_to_db" {
  function_name      = aws_lambda_function.gsheet_to_db.function_name
  authorization_type = "NONE"  # or "AWS_IAM" for auth
}
