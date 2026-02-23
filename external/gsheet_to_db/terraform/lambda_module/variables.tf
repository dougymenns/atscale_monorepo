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
}

# Google Sheets
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
  description = "Google service account private key"
  type        = string
  sensitive   = true
}

variable "GOOGLE_PROJECT_ID" {
  description = "Google cloud project ID"
  type        = string
  sensitive   = true
}

variable "GOOGLE_SHEET_ID" {
  description = "Google Sheet ID"
  type        = string
  sensitive   = true
}

variable "WORKSHEET_NAME" {
  description = "Default worksheet name"
  type        = string
  default     = ""
  sensitive   = true
}

# variable "SCHEMA" {
#   description = "Default Postgres schema"
#   type        = string
#   default     = "operations"
# }

# variable "TABLE" {
#   description = "Default Postgres table name"
#   type        = string
#   default     = ""
#   sensitive   = true
# }