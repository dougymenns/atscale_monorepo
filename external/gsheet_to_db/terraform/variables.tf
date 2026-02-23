# Postgres
variable "PG_DB_NAME" {
  description = "Database name"
  type        = string
  sensitive   = true
}

variable "PG_DB_USER" {
  description = "Database user"
  type        = string
  sensitive   = true
}

variable "PG_DB_PASSWORD" {
  description = "Database password"
  type        = string
  sensitive   = true
}

variable "PG_ENDPOINT" {
  description = "Database endpoint (host)"
  type        = string
  sensitive   = true
}

variable "PG_PORT" {
  description = "Database port"
  type        = string
  default     = "5432"
}

# Google Sheets (service account)
variable "GOOGLE_CLIENT_EMAIL" {
  description = "Google service account client email"
  type        = string
  sensitive   = true
}

variable "GOOGLE_CLIENT_ID" {
  description = "Google service account client ID"
  type        = string
  sensitive   = true
}

variable "GOOGLE_PRIVATE_KEY" {
  description = "Google service account private key (raw or \\n for newlines)"
  type        = string
  sensitive   = true
}

variable "GOOGLE_PROJECT_ID" {
  description = "Google cloud project ID"
  type        = string
  sensitive   = true
}

variable "GOOGLE_SHEET_ID" {
  description = "Google Sheet ID (from sheet URL)"
  type        = string
  sensitive   = true
}

# # Optional defaults when not passed in Lambda event
# variable "WORKSHEET_NAME" {
#   description = "Default worksheet/tab name (optional)"
#   type        = string
#   default     = ""
#   sensitive   = true
# }

# variable "SCHEMA" {
#   description = "Default Postgres schema (e.g. operations)"
#   type        = string
#   default     = "operations"
# }

# variable "TABLE" {
#   description = "Default Postgres table name (optional)"
#   type        = string
#   default     = ""
#   sensitive   = true
# }

# variable "TABLE_NEW" {
#   description = "Default Postgres table name (optional)"
#   type        = string
#   default     = ""
#   sensitive   = true
# }
