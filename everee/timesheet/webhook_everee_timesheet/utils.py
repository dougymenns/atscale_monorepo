
from typing import List, Optional
import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text
import logging
import datetime
import boto3
import os
import json


logger = logging.getLogger(__name__)


class SCD2Manager:
    """
    A reusable, production-grade SCD Type 2 handler for Postgres/Redshift.

    Supports:
    - Checking for existing business key
    - Closing old versions (curr_flg = 'N')
    - Inserting new versions
    - Custom surrogate key column
    - Fully parameterized SQL for safety
    """

    def __init__(
        self,
        engine: Engine,
        schema: str,
        table: str,
        business_key: str,
        surrogate_key: str,
        eff_strt_dt: str = "eff_strt_dt",
        eff_end_dt: str = "eff_end_dt",
        curr_flg: str = "curr_flg"
    ):
        self.engine = engine
        self.schema = schema
        self.table = table
        self.business_key = business_key
        self.surrogate_key = surrogate_key
        self.eff_strt_dt = eff_strt_dt
        self.eff_end_dt = eff_end_dt
        self.curr_flg = curr_flg

    # ----------------------------------------------------------------------
    # UTILITY QUERIES
    # ----------------------------------------------------------------------

    def _fetch_current_record(self, conn, bkey_value):
        """Fetch the existing current record for the business key."""
        query = f"""
            SELECT *
            FROM {self.schema}.{self.table}
            WHERE {self.business_key} = :bkey
              AND {self.curr_flg} = 'Y'
            ORDER BY {self.eff_end_dt} DESC
            LIMIT 1;
        """
        return conn.execute(text(query), {"bkey": bkey_value}).fetchone()

    # ----------------------------------------------------------------------
    # UPDATE OLD RECORD
    # ----------------------------------------------------------------------

    def _close_existing_record(self, conn, bkey_value):
        """Set curr_flg = N and eff_end_dt = now() for the existing record."""
        close_query = f"""
            UPDATE {self.schema}.{self.table}
            SET {self.curr_flg} = 'N',
                {self.eff_end_dt} = current_date
            WHERE {self.business_key} = :bkey
              AND {self.curr_flg} = 'Y';
        """
        conn.execute(text(close_query), {"bkey": bkey_value})

    # ----------------------------------------------------------------------
    # INSERT NEW RECORD
    # ----------------------------------------------------------------------

    def _insert_new_record(self, conn, row_dict: dict):
        """Insert a new version record."""
        # Add SCD2 system columns
        row_dict[self.eff_strt_dt] = datetime.datetime.now().strftime('%Y-%m-%d')
        row_dict[self.eff_end_dt] = "9999-12-31"
        row_dict[self.curr_flg] = "Y"

        cols = ",".join(row_dict.keys())
        params = ",".join([f":{c}" for c in row_dict.keys()])

        insert_sql = f"""
            INSERT INTO {self.schema}.{self.table} ({cols})
            VALUES ({params});
        """

        conn.execute(text(insert_sql), row_dict)

    # ----------------------------------------------------------------------
    # PUBLIC METHOD
    # ----------------------------------------------------------------------

    def apply_scd2(self, df: pd.DataFrame) -> bool:
        """
        Apply SCD2 logic to every row in the DataFrame.
        """

        if df is None or df.empty:
            logger.warning("SCD2 WARNING: Provided dataframe is empty—nothing to process.")
            return False

        try:
            # drop id column if exists before processing
            df.drop(columns=['id'], inplace=True, errors='ignore')
            with self.engine.begin() as conn:
                for _, row in df.iterrows():
                    row_data = row.to_dict()
                    bkey_val = row_data[self.business_key]

                    # STEP 1: Fetch current record
                    current = self._fetch_current_record(conn, bkey_val)

                    # STEP 2: If no record, insert as new
                    if current is None:
                        print(f"<===== SCD2: No existing record for business key {bkey_val}. Inserting with record {df.shape} =====>")
                        # del row_data["id"]  # Remove id if present
                        self._insert_new_record(conn, row_data)
                        continue

                    # STEP 3: If same SK, skip (no change)
                    if str(current[self.surrogate_key]) == str(row_data[self.surrogate_key]):
                        print(f"<===== SCD2: No change for business key {bkey_val}. Skipping. =====>")
                        continue

                    # STEP 4: Close old record
                    print(f"<===== SCD2: Change detected for business key {bkey_val}. Closing old record. =====>")
                    self._close_existing_record(conn, bkey_val)

                    # STEP 5: Insert new version
                    print(f"<===== SCD2: Inserting new version for business key {bkey_val} with record {df.shape} =====>")
                    # del row_data["id"]  # Remove id if present
                    self._insert_new_record(conn, row_data)

            return True

        except Exception as ex:
            logger.error(f"<xxxxx SCD2 ERROR: {ex} xxxxx>", exc_info=True)
            return False


class DB_QUERY_MANAGER:

    def __init__(
        self,
        engine: Engine
    ):
        self.engine = engine

    def fetch_from_db(self, query: str) -> pd.DataFrame:
        """
        Retrieve data from the database using SQLAlchemy engine.

        Parameters:
            query (str): SQL query string to execute.

        Returns:
                - DataFrame: Query results as a pandas DataFrame.
        """
        try:
            with self.engine.begin() as conn:
                # Execute query and fetch into DataFrame
                query_res = conn.execute(text(query)).fetchall()
                if query_res is not None:
                    df = pd.DataFrame(query_res)
                    print(f"<===== CUSTOM INFO: Data fetched successfully from database with shape {df.shape}. =====>")
                    return pd.DataFrame(query_res)
                else:
                    print(f"<===== CUSTOM INFO: No Data fetched from database. =====>")
                    df = pd.DataFrame()
                return df

        except Exception as ex:
            logger.error(f"CUSTOM INFO: <xxxxx Could not fetch from database due to : {ex} xxxxx>")
            return pd.DataFrame()

    def stored_procedure(self, query: str) -> bool:
        """
        Runs postgres stored procedure.

        Parameters:
            query (str): SQL query string to execute.

        Returns:
                - Boolean: True if successful, False otherwise.
        """
        try:
            with self.engine.begin() as conn:
                # Execute query and fetch into DataFrame
                query_res = conn.execute(text(query))
                if query_res is not None:
                    print("<===== Procedure: Record Inserted Successfully Into DB =====>")
                    return True
                else:
                    print("<xxxxx CUSTOM INFO: No Data fetched from database. xxxxx>")
                return False

        except Exception as ex:
            logger.error(f"CUSTOM INFO: <xxxxx Could not complete running stored procedure due to : {ex} xxxxx>")
            return False

    def execute_db_dml(self, query: str) -> bool:
        """
        Execute Data Manipulation Language (DML) queries (INSERT, UPDATE, DELETE).
        The transaction is managed by the 'begin()' context manager.

        Parameters:
            query (str): SQL query string to execute.

        Returns:
            bool: True if the query executed successfully, False otherwise.
        """
        try:
            with self.engine.begin() as conn:
                # Execute the DML query
                result = conn.execute(text(query))

                # Check if the execution was successful and get row count
                row_count = result.rowcount

                if row_count >= 0:
                    print(f"<===== CUSTOM INFO: DML executed successfully. Rows affected: {row_count}. =====>")
                    return True
                else:
                    print(f"<===== CUSTOM INFO: DML executed, but result was unexpected. =====>")
                    return False

        except Exception as ex:
            # The transaction is automatically rolled back upon exception
            logger.error(f"CUSTOM INFO: <xxxxx Could not execute DML in database due to : {ex} xxxxx>")
            return False

    def batch_upsert(self, df, schema: str, table: str, business_key: str):
        """
        Generic batch UPSERT for Postgres using ON CONFLICT DO UPDATE.
        No explicit column naming required.

        df: pandas DataFrame
        schema, table: target table
        business_key: column that is the unique constraint/index
        """

        if df.empty:
            logger.warning("batch_upsert called with empty DataFrame.")
            return False

        try:
            # Convert DF rows
            rows = df.to_dict(orient="records")

            # All columns
            columns = df.columns.tolist()

            # Build insert column list
            col_str = ",".join([f'"{c}"' for c in columns])
            param_str = ",".join([f":{c}" for c in columns])

            # Columns to update = all except business_key
            update_cols = [c for c in columns if c != business_key]

            # Dynamically create update SET clause: col = EXCLUDED.col
            update_str = ",".join([f'"{c}" = EXCLUDED."{c}"' for c in update_cols])

            # Final UPSERT SQL
            upsert_sql = text(
                f"""
                INSERT INTO {schema}.{table} ({col_str})
                VALUES ({param_str})
                ON CONFLICT ("{business_key}")
                DO UPDATE
                SET {update_str};
                """
            )

            # Execute in a single transaction
            with self.engine.begin() as conn:
                conn.execute(upsert_sql, rows)

            return True

        except Exception as ex:
            logger.error(
                f"CUSTOM INFO: <xxxxx Could not complete batch_upsert due to: {ex} xxxxx>"
            )
            return False


# function to invoke lambda function to update user details
def invoke_lambda_function(payload=None, FUNCTION_NAME=None):
    '''
    Invoke update connecteam user
    '''
    function_name = FUNCTION_NAME
    try:
        client = boto3.client('lambda')
        client.invoke(
            FunctionName=function_name,
            InvocationType='Event',
            Payload=json.dumps(payload),
            LogType='Tail'
        )
        print(f'{FUNCTION_NAME}: invoked with payload: {payload}')
    except Exception as ex:
        logger.error(ex)
        print(f'Could not pass {payload} due to ', ex)
    return