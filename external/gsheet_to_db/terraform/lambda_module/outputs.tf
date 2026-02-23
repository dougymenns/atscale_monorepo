output "function_url" {
  description = "Lambda Function URL (stable across updates)"
  value       = aws_lambda_function_url.gsheet_to_db.function_url
}
