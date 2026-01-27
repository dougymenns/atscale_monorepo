## AtScale Integration Codebase

This repository contains all data pipelines and integrations used by AtScale for connecting various external systems (Connecteam, Everee, Fountain) with internal data stores and services.  
Each sub-project is designed as an AWS Lambda–friendly service with its own `Dockerfile`, `requirements.txt`, and `main.py`.

## Directory Overview

### **connecteam/**
Connecteam-related data pipelines and integrations. See `connecteam/readme.md` for detailed documentation.

### **everee/**
Everee-related integrations for worker and timesheet management.

### **fountain/**
Fountain applicant tracking system integrations.

### **external/**
External integrations and utilities.



## Common Patterns

- **Entry point**: Each subdirectory exposes a `lambda_handler(event, context)` in `main.py`.
- **Environment configuration**: Credentials and endpoints (e.g. `PG_ENDPOINT`, `ENDPOINT`, `DB_USER`, `S3_BUCKET`, `API_KEY`) are read via `os.environ`.
- **DB access**: Uses internal helpers like `db_utils.py` and `utils.DB_QUERY_MANAGER`/`db_connection` to read/write Postgres/Redshift.
- **Data transformation**: Heavy use of `pandas` for normalizing JSON payloads and preparing DB/CSV outputs.
- **AWS integration**: Uses `boto3` for Lambda, S3, and sometimes other AWS services.
- **Lambda invocation**: Services often invoke other Lambda functions for orchestration (e.g., Connecteam services triggering Everee services).

## Local Development

- **Python version**: Match the version used in the virtualenv under each `env/` (typically Python 3.11 or 3.12 in lambdas).
- **Dependencies**:
  - From a given subdirectory (e.g. `connecteam/timesheets/webhook_ct_timesheet`):
    ```bash
    python -m venv env
    source env/bin/activate
    pip install -r requirements.txt
    ```

## Deployment Notes

- Each sub-project is structured to be containerized (via `Dockerfile`) for AWS Lambda.
- Typical deployment flow:
  - Build the image from the subdirectory.
  - Push to ECR using `startup.sh` in respective folders.
  - Point the corresponding Lambda to the new image.
- Ensure required environment variables (DB credentials, API keys, S3 bucket info, Slack webhooks, Google Sheets credentials, etc.) are set in the target environment before deploying.
