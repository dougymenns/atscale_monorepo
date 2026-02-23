"""
Lambda: fetch all columns from a Google Sheet and full-load into Postgres.
Pass worksheet_name and table_name in the event (or set env).
"""
import os
import json
import logging

from process_sheet import google_creds_auth, fetch_sheet_as_dataframe
from db_utils import db_connection
from utils import DB_QUERY_MANAGER

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Postgres
PG_ENDPOINT = os.getenv("PG_ENDPOINT")
PG_DB_NAME = os.getenv("PG_DB_NAME")
PG_DB_USER = os.getenv("PG_DB_USER")
PG_DB_PASSWORD = os.getenv("PG_DB_PASSWORD")

# Google Sheets
GOOGLE_CLIENT_EMAIL = os.getenv("GOOGLE_CLIENT_EMAIL")
GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID")
GOOGLE_PRIVATE_KEY = os.getenv("GOOGLE_PRIVATE_KEY", "").replace("\\n", "\n")
GOOGLE_PROJECT_ID = os.getenv("GOOGLE_PROJECT_ID")
GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID")

# Defaults when not passed in event
# WORKSHEET_NAME = os.getenv("WORKSHEET_NAME", "")
# TABLE_NAME = os.getenv("TARGET_TABLE", "")
# SCHEMA = os.getenv("TARGET_SCHEMA", "operations")


def lambda_handler(event, context):
    print(json.dumps(event))
    res_json = json.loads(event['body'])

    worksheet_name = res_json.get("worksheet_name", None)
    table_name = res_json.get("target_table", None)
    schema = res_json.get("target_schema", None)
    if not worksheet_name or not table_name or not schema:
        return {
            "statusCode": 400,
            "body": json.dumps({
                "ERROR": "worksheet_name, table_name, and schema required",
                "hint": "Pass in event or set worksheet_name, target_table, and target_schema in event body",
            }),
        }

    if not GOOGLE_SHEET_ID:
        return {
            "statusCode": 400,
            "body": json.dumps({"ERROR": "GOOGLE_SHEET_ID not set"}),
        }

    client = google_creds_auth(
        client_email=GOOGLE_CLIENT_EMAIL,
        client_id=GOOGLE_CLIENT_ID,
        private_key=GOOGLE_PRIVATE_KEY,
        project_id=GOOGLE_PROJECT_ID,
    )

    df = fetch_sheet_as_dataframe(
        client=client,
        sheet_id=GOOGLE_SHEET_ID,
        worksheet_name=worksheet_name,
    )

    if df.empty:
        logger.warning("No data from sheet; skipping load.")
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "No data in sheet; no load performed.",
                "worksheet_name": worksheet_name,
                "table": f"{schema}.{table_name}",
            }),
        }

    try:
        engine = db_connection(
            DB_USER=PG_DB_USER,
            DB_PASSWORD=PG_DB_PASSWORD,
            ENDPOINT=PG_ENDPOINT,
            DB_NAME=PG_DB_NAME,
            db_type="POSTGRESQL",
        )
        db_query_manager = DB_QUERY_MANAGER(engine=engine)
        db_query_manager.create_table_if_not_exists(
            df, schema=schema, table=table_name
        )
        success = db_query_manager.batch_insert(
            df,
            schema=schema,
            table=table_name,
            replace=True,
        )
        if success:
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "Full load completed.",
                    "worksheet_name": worksheet_name,
                    "table": f"{schema}.{table_name}",
                    "rows_loaded": len(df),
                }),
            }
        return {"statusCode": 500, "body": "batch_insert returned False."}
    except Exception as ex:
        logger.exception("Pipeline failed: %s", ex)
        return {"statusCode": 500, "body": f"Pipeline failed: {ex!s}"}
