provider "aws" {
  region = "us-east-1"
}

data "aws_iam_role" "system_admin_role" {
  name = "system_admin"
}

module "gsheet_to_db" {
  source = "./lambda_module"

  # Postgres
  PG_DB_NAME     = var.PG_DB_NAME
  PG_DB_USER     = var.PG_DB_USER
  PG_DB_PASSWORD = var.PG_DB_PASSWORD
  PG_ENDPOINT    = var.PG_ENDPOINT
  PG_PORT        = var.PG_PORT

  # Google Sheets
  GOOGLE_CLIENT_EMAIL = var.GOOGLE_CLIENT_EMAIL
  GOOGLE_CLIENT_ID    = var.GOOGLE_CLIENT_ID
  GOOGLE_PRIVATE_KEY  = var.GOOGLE_PRIVATE_KEY
  GOOGLE_PROJECT_ID   = var.GOOGLE_PROJECT_ID
  GOOGLE_SHEET_ID     = var.GOOGLE_SHEET_ID

  # Optional defaults
  # WORKSHEET_NAME = var.WORKSHEET_NAME
  # SCHEMA         = var.SCHEMA
  # TABLE          = var.TABLE
}

output "lambda_function_url" {
  description = "Lambda Function URL (stable across updates)"
  value       = module.gsheet_to_db.function_url
}
