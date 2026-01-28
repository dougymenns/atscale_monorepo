import os
import requests
import base64
import hashlib
import pandas as pd
import re
from typing import Any
import logging
import warnings
from utils import DB_QUERY_MANAGER
from db_utils import db_connection
from typing import Optional

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

warnings.filterwarnings("ignore")

# POSTGRES CONFIG
PG_ENDPOINT = os.environ.get("PG_ENDPOINT")
PG_PORT = os.environ.get("PG_PORT")
PG_DB_NAME = os.environ.get("PG_DB_NAME")
PG_DB_USER = os.environ.get("PG_DB_USER")
PG_DB_PASSWORD = os.environ.get("PG_DB_PASSWORD")


# hash user data for sk
def compute_md5(row):
    concatenated_values = ''.join(str(val) for val in row)
    return hashlib.md5(concatenated_values.encode()).hexdigest()


def standardize_column_name(column_name):
    # Handle camelCase (like PostQueue -> post_queue)
    column_name = re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', column_name)

    # Replace any whitespace or hyphen with an underscore
    column_name = re.sub(r'[\s-]+', '_', column_name)

    # Replace brackets
    column_name = column_name.replace('(', '').replace(')', '')

    # Replace . wtih _
    column_name = column_name.replace('.', '_')

    # Convert to lowercase for standard database naming
    return column_name.lower()


def everee_create_shift(payload: dict) -> Any:
    """
    Create a timesheet shift in Everee via API

    Args:
        payload (dict): Dictionary containing shift details

    Returns:
        requests.Response: API response object

    Raises:
        ValueError: If required environment variables are missing
        requests.RequestException: If API request fails
    """
    try:
        # Get required environment variables
        api_token = os.environ.get('EVEREE_API_TOKEN', 'sk_sDxamCz6Ea5JZvmMKyhKkg0DxwsHcp8V')
        tenant_id = os.environ.get('TENANT_ID', '1503')

        if not api_token or not tenant_id:
            raise ValueError("Missing required environment variables: EVEREE_API_TOKEN and/or TENANT_ID")

        # Filter payload to only include valid keys
        everee_payload_keys = [
            'correctionPaymentTimeframe', 'externalWorkerId', 'workerId',
            'shiftStartEpochSeconds', 'shiftEndEpochSeconds', 'note', 'override_rate'
        ]
        timesheet_payload = {key: value for key, value in payload.items() if key in everee_payload_keys}

        if not timesheet_payload:
            logger.error("Custom ERROR: Empty payload")

        if timesheet_payload['override_rate'] is not None:
            override_rate = timesheet_payload['override_rate']
            timesheet_payload['effectiveHourlyPayRate'] = {
                "amount": override_rate,
                "currency": "USD"
            }
            del timesheet_payload['override_rate']

        # Set up API request
        url = "https://api.everee.com/api/v2/labor/timesheet/worked-shifts/epoch?correction-authorized=false"
        encoded_token = base64.b64encode(api_token.encode('utf8')).decode()
        headers = {
            "accept": "application/json",
            "x-everee-tenant-id": tenant_id,
            "Authorization": f"Basic {encoded_token}",
            "Content-Type": "application/json"
        }

        # Make API request with timeout and retries
        logger.info("Making Everee API request to create timesheet")
        response = requests.post(
            url,
            json=timesheet_payload,
            headers=headers,
            timeout=30
        )

        logger.info(f"Everee API request successful with status code: {response.status_code}")
        return response

    except Exception as ex:
        logger.exception('Custom ERROR(API): while updating timesheet through api: ', ex)


def process_res(response, ct_payload_df):
    try:
        # if create or update successful
        if response.status_code == 200:
            # Pass response json to function
            res_df = process_success_response(response, ct_payload_df)
            return res_df
        # if delete successful (204) then process delete response
        elif response.status_code == 204:
            # Pass response to function
            res_df = process_delete_response(response, ct_payload_df)
            return res_df
        else:
            res_df = process_failed_response(response, ct_payload_df)
            return res_df
    except Exception as ex:
        logger.exception(f'CUSTOM INFO: Unable to process response {ex}')


def process_success_response(response, ct_payload_df):
    try:
        logger.info('Processing success response')
        res_json = response.json()
        res_df = pd.json_normalize(res_json)

        # remove duplicate columns
        res_df = res_df.drop(columns=['worker.workerId', 'worker.externalWorkerId'], axis=1)

        # standardize column names
        res_df.columns = [standardize_column_name(col) for col in res_df.columns]

        # transfrom column names
        res_df.columns = res_df.columns.str.replace('worker_', '')
        rename_cols = {
            'id': 'worker_id',
            'external_id': 'external_worker_id',
            'shift_start_at_effective_punch_at': 'shift_start_at',
            'shift_end_at_effective_punch_at': 'shift_end_at',
            }
        res_df.rename(columns=rename_cols, inplace=True)

        # columns needed for db
        success_cols = ['worker_id', 'external_worker_id', 'full_name', 'worked_shift_id', 'user_id', 'pay_type',
                        'employee_id', 'company_id', 'legal_work_time_zone', 'verified_by_user_id',
                        'approval_group_name', 'shift_start_at', 'shift_end_at'
                        ]
        res_df = res_df[success_cols]

        res_df['timesheet_sk'] = res_df.apply(compute_md5, axis=1)
        res_df['int_message'] = 'success'
        res_df['status_code'] = 200
        ct_cols_needed = ['ct_time_activity_id','note','event_type','everee_action_type']
        df = pd.concat([res_df,ct_payload_df[ct_cols_needed]], axis=1)
        return df
    except Exception as ex:
        logger.exception('ERROR processing success response', ex)


def update_timesheet(worked_shift_id, everee_update_payload):
    try:
        logger.info('CUSTOM INFO: Action type/mehtod: update')
        api_token = os.environ.get('EVEREE_API_TOKEN', 'sk_sDxamCz6Ea5JZvmMKyhKkg0DxwsHcp8V')
        tenant_id = os.environ.get('TENANT_ID', '1503')

        # encoded api token
        encoded_token = base64.b64encode(api_token.encode('utf8')).decode()

        url = f"https://api.everee.com/api/v2/labor/timesheet/worked-shifts/epoch/{worked_shift_id}?correction-authorized=false"

        headers = {
                    "accept": "application/json",
                    "x-everee-tenant-id": tenant_id,
                    "Authorization": f"Basic {encoded_token}"
                }

        response = requests.put(url, json=everee_update_payload, headers=headers)
        logger.info('CUSTOM INFO: API response: ', response.status_code)
    except Exception as ex:
        logger.exception('Custom ERROR(API): while updating timesheet through api:  ', ex)
    return response


def process_update(ct_payload_df) -> dict:
    """
    Update timesheet through API
    """
    try:
        logger.info("CUSTOM INFO: Processing Update")
        # extract the payload keys needed to update everee timesheet
        ct_time_activity_id = str(ct_payload_df['ct_time_activity_id'][0])
        worker_id = str(ct_payload_df['worker_id'][0])
        note = str(ct_payload_df['note'][0])
        override_rate = str(ct_payload_df['override_rate'][0])
        shiftStartEpochSeconds = str(ct_payload_df['shiftStartEpochSeconds'][0])
        shiftEndEpochSeconds = str(ct_payload_df['shiftEndEpochSeconds'][0])

        query = f"""
            SELECT distinct worker_id, ct_time_activity_id, worked_shift_id
            FROM operations.webhook_everee_timesheet
            where ct_time_activity_id = '{ct_time_activity_id}'
            """
        everee_df = retrieve_from_db(query)
        if not everee_df.empty:
            worked_shift_id = str(everee_df['worked_shift_id'][0])
        else:
            worked_shift_id = None

        # if no everee timesheet found in db, and initial action type was update then create a new one
        if everee_df.empty or worked_shift_id is None:
            logger.info(f"Custom INFO(DB): No Everee timesheet found for CT time activity id: {ct_time_activity_id} in database")
            logger.info(f"CUSTOM INFO: Re-trying: create everee timesheet")
            everee_create_payload = {
                "workerId": worker_id,
                "correctionPaymentTimeframe": "NEXT_PAYROLL_PAYMENT",
                "shiftStartEpochSeconds": shiftStartEpochSeconds,
                "shiftEndEpochSeconds": shiftEndEpochSeconds,
                "note": note,
                "override_rate": override_rate
            }
            create_shift_res = everee_create_shift(everee_create_payload)
            return create_shift_res
        # if everee timesheet found in db, then update the timesheet in everee
        else:
            everee_update_payload = {
                "correctionPaymentTimeframe": "NEXT_PAYROLL_PAYMENT",
                "shiftStartEpochSeconds": shiftStartEpochSeconds,
                "shiftEndEpochSeconds": shiftEndEpochSeconds,
                "note": note
            }
            print(everee_update_payload)
            update_response = update_timesheet(worked_shift_id, everee_update_payload)
            return update_response
    except Exception as ex:
        logger.exception(ex)


def process_failed_response(response, ct_payload_df):
    try:
        res_df = pd.json_normalize(response.json())

        # standardize column names
        res_df.columns = [standardize_column_name(col) for col in res_df.columns]
        error_cols = ['error_code', 'error_message']
        res_df = res_df[error_cols]
        res_df['timesheet_sk'] = res_df.apply(compute_md5, axis=1)

        # rename error_code to int_message
        res_df.rename(columns={
                'error_message': 'int_message',
                'error_code': 'status_code'
                }, inplace=True)
        ct_cols_needed = ['worker_id','external_worker_id', 'full_name', 'ct_time_activity_id',
                'everee_action_type', 'event_type', 'note']
        df = pd.concat([res_df,ct_payload_df[ct_cols_needed]], axis=1)

        # handle case where worked_shift_id might not be present in payload
        if 'worked_shift_id' not in ct_payload_df.columns:
            df['worked_shift_id'] = ct_payload_df['ct_time_activity_id'].item() + '_' + 'Null'
        else:
            df['worked_shift_id'] = ct_payload_df['worked_shift_id'].item()
        return df
    except Exception as ex:
        logger.exception('ERROR processing failed response', ex)


def transform_ct_payload(event):
    '''
    Convert CT payload to DataFrame that will feed into Everee table.

    Args:
        event (dict): Input event from Lambda trigger

    Returns:
        pd.DataFrame: Transformed DataFrame with required columns

    Raises:
        ValueError: If required fields are missing from input
        Exception: For other processing errors
    '''
    if not event:
        logger.error("Custom ERROR: Empty event received")

    try:
        ct_payload_df = pd.json_normalize(event)
        if (ct_payload_df['event_type'].str.contains('delete').any() == True) or (ct_payload_df['event_type'].str.contains('declined').any() == True):
            ct_payload_df.columns = [standardize_column_name(col) for col in ct_payload_df.columns]
            ct_cols_needed = [
                'worker_id','external_worker_id','ct_time_activity_id','event_type',
                'full_name', 'everee_action_type'
                ]
            ct_payload_df = ct_payload_df[ct_cols_needed]
            ct_payload_df['note'] = None
            return ct_payload_df
        else:
            ct_payload_df.rename(columns={
                'workerId': 'worker_id',
                'externalWorkerId': 'external_worker_id'
                }, inplace=True)
            ct_cols_needed = [
                'worker_id','external_worker_id','ct_time_activity_id','note','event_type',
                'full_name', 'shiftStartEpochSeconds','shiftEndEpochSeconds','everee_action_type',
                'override_rate'
                ]
            ct_payload_df = ct_payload_df[ct_cols_needed]
            return ct_payload_df
    except Exception as ex:
        logger.error('Custom ERROR: CT event transformation error ', ex)


def delete_timesheet(worked_shift_id):
    """
    Delete a timesheet from Everee API.

    Args:
        worked_shift_id: The Everee worked shift ID to delete

    Returns:
        requests.Response: The API response object
    """
    try:
        api_token = os.environ.get('EVEREE_API_TOKEN', 'sk_sDxamCz6Ea5JZvmMKyhKkg0DxwsHcp8V')
        tenant_id = os.environ.get('TENANT_ID', '1503')

        # encoded api token
        encoded_token = base64.b64encode(api_token.encode('utf8')).decode()

        url = f"https://api.everee.com/api/v2/labor/timesheet/worked-shifts/{worked_shift_id}"
        headers = {
                    "accept": "application/json",
                    "x-everee-tenant-id": tenant_id,
                    "Authorization": f"Basic {encoded_token}"
                }

        response = requests.delete(url, headers=headers)
        logger.info("CUSTOM INFO: API response status: %s", response.status_code)
        if response.status_code == 204:
            logger.info("Custom INFO: Everee timesheet deleted successfully for %s", worked_shift_id)
        elif response.status_code == 404:
            logger.warning("Custom INFO: Everee timesheet not found for worked_shift_id: %s", worked_shift_id)
        elif response.status_code == 400:
            logger.warning("Custom INFO: Response for Everee timesheet for %s", response.json().get('error_message'))
        return response
    except Exception as ex:
        logger.exception('Custom ERROR(API): while deleting timesheet through api:  ', ex)
        raise


def process_delete(ct_payload_df):
    """
    Delete timesheet through API.

    Args:
        ct_payload_df: DataFrame containing timesheet information including ct_time_activity_id

    Returns:
        requests.Response or None: Response object if deletion attempted, None if no record found in DB
    """
    try:
        logger.info("CUSTOM INFO: Processing Delete")

        # extract ct time activity id from ct payload
        ct_time_activity_id = ct_payload_df.get('ct_time_activity_id').item()
        if ct_time_activity_id is not None:
            query = f"""
                SELECT distinct worker_id, ct_time_activity_id, worked_shift_id, load_dt
                FROM operations.webhook_everee_timesheet
                where ct_time_activity_id = '{ct_time_activity_id}'
                ORDER BY load_dt DESC LIMIT 1
                """
            everee_df = retrieve_from_db(query)

            if not everee_df.empty:
                worked_shift_id = everee_df.get('worked_shift_id').item()
                delete_response = delete_timesheet(worked_shift_id)
                ct_payload_df['worked_shift_id'] = worked_shift_id
                return delete_response, ct_payload_df
            else:
                logger.info("Custom Info(DB): No Everee timesheet found for ct_time_activity_id: %s in database. Skipping...", ct_time_activity_id)

                return None
    except Exception as ex:
        logger.exception(ex)
        raise


def process_delete_response(response, ct_payload_df):
    """
    Process delete response and return DataFrame with required columns.
    Handles cases where worked_shift_id might not be present in the payload.
    """
    # Add worked_shift_id column if it doesn't exist (set to ct_time_activity_id + '_Null')
    if 'worked_shift_id' not in ct_payload_df.columns:
        ct_payload_df['worked_shift_id'] = ct_payload_df['ct_time_activity_id'].item() + '_' + 'Null'

    ct_cols_needed = ['worked_shift_id','worker_id','external_worker_id', 'full_name', 'ct_time_activity_id',
                      'everee_action_type', 'event_type', 'note']
    df = ct_payload_df[ct_cols_needed]
    return df


def handle_delete_action(ct_payload_df: pd.DataFrame):
    """
    Handle delete action for Everee timesheet from API to Database processing.
    """
    try:
        delete_shift_res = process_delete(ct_payload_df)
        # retrieve response data
        delete_res = delete_shift_res[0]
        # retrieve processed df
        delete_df = delete_shift_res[1]
        if delete_res.status_code == 204:
            # process and store in db to track status of deletion attempt
            everee_timesheet = process_res(delete_res, delete_df)
            # standardize column names
            everee_timesheet.columns = [standardize_column_name(col) for col in everee_timesheet.columns]
            # insert into db
            everee_timesheet_sts = insert_to_db(everee_timesheet, schema="operations", table_name="webhook_everee_timesheet", business_key="worked_shift_id")
            return everee_timesheet_sts
        elif delete_res.status_code != 204:
            # process and store in db to track status of deletion attempt
            everee_timesheet = process_res(delete_res, delete_df)
            # standardize column names
            everee_timesheet.columns = [standardize_column_name(col) for col in everee_timesheet.columns]
            # insert into db
            everee_timesheet_sts = insert_to_db(everee_timesheet, schema="operations", table_name="webhook_everee_timesheet", business_key="worked_shift_id")
            return everee_timesheet_sts
        else:
            return None
    except Exception as ex:
        logger.exception(ex)


def handle_create_action(payload: dict, ct_payload_df: pd.DataFrame):
    """
    Handle create action for Everee timesheet from API to Database processing.
    """
    try:
        logger.info("CUSTOM INFO: Processing create timesheet")
        create_shift_res = everee_create_shift(payload=payload)
        everee_timesheet = process_res(create_shift_res, ct_payload_df)
        everee_timesheet_sts = insert_to_db(everee_timesheet, schema="operations", table_name="webhook_everee_timesheet", business_key="worked_shift_id")
        if everee_timesheet_sts is not None:
            return everee_timesheet_sts
        else:
            return None
    except Exception as ex:
        logger.exception(ex)


def insert_to_db(df, schema=None, table_name=None, business_key=None) -> Any:
    """
    Load a DataFrame to a specified database table.

    Parameters:
        df (DataFrame): The DataFrame to be loaded into the database.
        schema (str): Database schema name.
        table_name (str): Target table name in the database.
        replace_db (bool): If True, replaces the table contents; if False, appends data.

    Returns:
        bool: True if upsert succeeded, False if skipped or error.
    """
    try:
        # Connect to database
        engine = db_connection(DB_USER=PG_DB_USER, DB_PASSWORD=PG_DB_PASSWORD, ENDPOINT=PG_ENDPOINT, DB_NAME=PG_DB_NAME, db_type='POSTGRESQL', PORT=PG_PORT)

        # Execute the query
        db_query_manager = DB_QUERY_MANAGER(engine=engine)

        # Perform a batch upsert using business_key as the unique identifier.
        df_status = db_query_manager.batch_upsert(df=df, schema=schema, table=table_name, business_key=business_key)

        # Log success message
        if df_status:
            logger.info("<===== Data Upserted Successfully Into DB =====>")
        else:
            logger.error("<xxxxx DB Upsert Operation Unsuccessful xxxxx>")

        return df_status
    except Exception as ex:
        logger.exception("Connection to database could not be made due to the following error: \n", ex)
        return False


def retrieve_from_db(query) -> Any:
    '''
    Retrieve data from a specified database table using SQL query.
    '''
    try:
        # Connect to database
        engine = db_connection(DB_USER=PG_DB_USER, DB_PASSWORD=PG_DB_PASSWORD, ENDPOINT=PG_ENDPOINT, DB_NAME=PG_DB_NAME, db_type='POSTGRESQL', PORT=PG_PORT)

        # Execute the query
        db_query_manager = DB_QUERY_MANAGER(engine=engine)

        df = db_query_manager.fetch_from_db(query)
        # Check if at least one record was fetched
        if not df.empty:
            return df
        else:
            return pd.DataFrame()  # No records found, set df to None
    except Exception as ex:
        logger.exception(ex)
        return pd.DataFrame()


def update_sync_state(ct_timesheet_id: str) -> Optional[str]:
    """
    Update the everee_sync_state to 'SENT' for the given ct_timesheet_id
    """
    try:
        # Create a SQLAlchemy engine
        engine = db_connection(DB_USER=PG_DB_USER, DB_PASSWORD=PG_DB_PASSWORD, ENDPOINT=PG_ENDPOINT, DB_NAME=PG_DB_NAME, db_type='POSTGRESQL', PORT=PG_PORT)
        # Execute the query
        db_query_manager = DB_QUERY_MANAGER(engine=engine)
        ct_timesheet_update_query = f"""
            UPDATE operations.webhook_ct_timesheet
            SET everee_sync_state = 'SENT'
            WHERE time_activity_id = '{ct_timesheet_id}';
            """
        user_updt_sts = db_query_manager.execute_db_dml(ct_timesheet_update_query)
        if user_updt_sts:
            logger.info('<===== Record Updated Successfully Into DB =====>')
            return ct_timesheet_id
        else:
            logger.error('<xxxxx DB Update Operation Unsuccessful xxxxx>')
            return None
    except Exception as ex:
        logger.exception(ex)
        return None