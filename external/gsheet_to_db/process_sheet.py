"""
Fetch data from Google Sheets; all columns, no transformation.
"""
import re
import logging

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)


def _normalize_column_name(name: str) -> str:
    """Make column name safe for Postgres: lowercase, alphanumeric + underscore."""
    s = str(name).strip().lower().replace(" ", "_")
    s = re.sub(r"[^a-z0-9_]", "_", s)
    return s or "col"


def google_creds_auth(
    client_email: str,
    client_id: str,
    private_key: str,
    project_id: str,
) -> gspread.Client:
    """Authenticate for Google Sheets API."""
    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_info(
        info={
            "type": "service_account",
            "client_id": client_id,
            "client_email": client_email,
            "private_key": private_key,
            "project_id": project_id,
            "token_uri": "https://oauth2.googleapis.com/token",
        },
        scopes=scopes,
    )
    return gspread.authorize(creds)


def fetch_sheet_as_dataframe(
    client: gspread.Client,
    sheet_id: str,
    worksheet_name: str,
) -> pd.DataFrame:
    """
    Fetch a worksheet from a Google Sheet and return as DataFrame.
    First row is used as column headers.
    """
    spreadsheet = client.open_by_key(sheet_id)
    try:
        worksheet = spreadsheet.worksheet(worksheet_name)
    except gspread.exceptions.WorksheetNotFound:
        logger.error("Worksheet '%s' not found in sheet %s", worksheet_name, sheet_id)
        return pd.DataFrame()

    records = worksheet.get_all_records()
    if not records:
        logger.warning("No rows in worksheet '%s' (only header or empty).", worksheet_name)
        return pd.DataFrame()

    df = pd.DataFrame(records)
    # Normalize column names for Postgres (safe identifiers)
    df.columns = [_normalize_column_name(c) for c in df.columns]
    # Dedupe column names (e.g. two "col" -> col, col_1)
    cols = list(df.columns)
    seen = {}
    new_cols = []
    for c in cols:
        if c in seen:
            seen[c] += 1
            new_cols.append(f"{c}_{seen[c]}")
        else:
            seen[c] = 0
            new_cols.append(c)
    df.columns = new_cols
    logger.info(
        "Fetched %d rows, %d columns from worksheet '%s'.",
        len(df),
        len(df.columns),
        worksheet_name,
    )
    return df
