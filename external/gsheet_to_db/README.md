# `gsheet_to_db`

AWS Lambda (container image) that **reads a Google Sheet worksheet** and **full-loads it into Postgres** (create table if needed, then `TRUNCATE` + `INSERT`).

## What it does

- Authenticates to Google Sheets using a **service account** (read-only scope).
- Fetches all rows from a given `worksheet_name` in `GOOGLE_SHEET_ID`.
- Normalizes sheet column names for Postgres:
  - lowercased
  - spaces → `_`
  - non-alphanumeric → `_`
  - de-dupes duplicates (e.g. `col`, `col_1`, ...)
- Ensures the target table exists (infers column types from pandas dtypes) and ensures a `load_dt` column exists.
- Full-loads into `target_schema.target_table` by truncating then inserting.

## Required configuration (environment variables)

### Postgres

- `PG_ENDPOINT`
- `PG_DB_NAME`
- `PG_DB_USER`
- `PG_DB_PASSWORD`

### Google Sheets (service account)

- `GOOGLE_CLIENT_EMAIL`
- `GOOGLE_CLIENT_ID`
- `GOOGLE_PRIVATE_KEY` (if you store it with `\n` escapes, the code converts `\\n` → newline)
- `GOOGLE_PROJECT_ID`
- `GOOGLE_SHEET_ID`

## Invocation contract

The handler requires:

- `worksheet_name`
- `target_table`
- `target_schema`

Responses:
- `200`: load completed (or sheet empty → no load performed)
- `400`: missing required inputs / missing `GOOGLE_SHEET_ID`
- `500`: insert or connection failure


## Deploy (ECR + Lambda image update)

`startup.sh` contains the commands used to:

- login to ECR
- build/tag/push the image
- update the Lambda function to the latest image

Run from this folder:

```bash
bash startup.sh
```

## Troubleshooting

- **Empty worksheet**: if the worksheet has only headers or no rows, the function returns `200` and skips the load.
- **`schema` missing**: create the schema in Postgres first (e.g. `CREATE SCHEMA operations;`).
- **Private key formatting**: ensure `GOOGLE_PRIVATE_KEY` includes the full PEM including header/footer lines; when stored in env vars it typically needs `\n` escapes.
- **Sheet column types**: `pandas` inference may interpret mixed-type columns as `TEXT`; this is generally safe, but may not match an existing table’s column types.
