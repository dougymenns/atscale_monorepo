import os
import datetime
import hashlib
import json
import pandas as pd
import pytz
import re
import boto3
import warnings
import logging
from sqlalchemy import text
from typing import Any
from zoneinfo import ZoneInfo
warnings.filterwarnings('ignore')
from utils import DB_QUERY_MANAGER
from db_utils import db_connection

logger = logging.getLogger(__name__)

# POSTGRES CONFIG
PG_ENDPOINT = os.getenv("PG_ENDPOINT")
PG_PORT = os.getenv("PG_PORT")
PG_DB_NAME = os.getenv("PG_DB_NAME")
PG_DB_USER = os.getenv("PG_DB_USER")
PG_DB_PASSWORD = os.getenv("PG_DB_PASSWORD")


# uct timestamp for load dt in db
def utc_timestamp():
    # Get the current UTC time
    current_datetime_utc = datetime.datetime.now(pytz.utc)

    # Format the datetime as a string
    load_dt = current_datetime_utc.strftime('%Y-%m-%dT%H:%M:%S %Z%z UTC')
    return load_dt


# hash user data for sk
def compute_md5(row):
    concatenated_values = ''.join(str(val) for val in row)
    return hashlib.md5(concatenated_values.encode()).hexdigest()


def retrieve_from_db(query) -> Any:
    '''
    Retrieve data from a specified database table using SQL query.
    '''
    try:
        # Connect to database
        engine = db_connection(DB_USER=PG_DB_USER, DB_PASSWORD=PG_DB_PASSWORD, ENDPOINT=PG_ENDPOINT, DB_NAME=PG_DB_NAME, db_type='POSTGRESQL', PORT=PG_PORT)

        # Execute the query
        db_query_manager = DB_QUERY_MANAGER(engine=engine)
        # Fetch all rows from the query result
        df = db_query_manager.fetch_from_db(query)
        # Check if at least one record was fetched
        if not df.empty:
            return df
        else:
            return pd.DataFrame()  # No records found, set df to None
    except Exception as ex:
        logger.exception(ex)
        return pd.DataFrame()


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


# round time to nearest 5 minutes
def round_to_nearest_5_minutes(utc_timestamp):
    utc_timestamp = datetime.datetime.utcfromtimestamp(utc_timestamp)
    minutes = utc_timestamp.minute
    seconds = utc_timestamp.second

    if seconds > 0 or minutes % 5 > 0:
        if (minutes % 5) * 60 + seconds < 150:
            rounded_minutes = (minutes // 5) * 5
        else:
            rounded_minutes = ((minutes // 5) + 1) * 5

        rounded_timestamp = utc_timestamp.replace(minute=rounded_minutes, second=0, microsecond=0)
        rounded_timestamp = int(rounded_timestamp.timestamp())
    else:
        rounded_timestamp = int(utc_timestamp.timestamp())

    return rounded_timestamp


def process_timesheet_data(response, round_time=True):
    '''
    Transform connecteam timesheet webhook data
    '''
    try:
        # Convert json to dataframe
        df = pd.json_normalize(response)

        # Create a DataFrame from the updated dictionary
        df.columns = [standardize_column_name(col) for col in df.columns]
        df.columns = df.columns.str.replace('time_activity_', '')

        # rename the columns
        df.rename(columns={
            'id': 'time_activity_id',
            'user_id': 'connecteam_user_id',
            'event_timestamp': 'event_timestamp',
            'duration_value': 'time_off_duration',
            'duration_units': 'time_off_duration_units',
            'is_all_day': 'time_off_is_all_day',
            'policy_type_id': 'time_off_policy_type_id',
            }, inplace=True)
        # transform data if delete
        if (df['event_type'].str.contains('delete').any() == True) or (df['event_type'].str.contains('declined').any() == True):
            df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], unit='s')
            ct_timesheet_sk_cols = [
                'activity_type', 'event_type','connecteam_user_id',
                'time_clock_id', 'time_activity_id'
                ]
            df['timesheet_sk'] = df[ct_timesheet_sk_cols].apply(compute_md5, axis=1)
            db_cols_needed = [
                'request_id', 'company', 'activity_type', 'event_timestamp',
                'event_type', 'connecteam_user_id', 'time_clock_id', 'time_activity_id',
                'timesheet_sk'
                ]
            df = df[db_cols_needed]

        # transforming time_off event
        elif df.get('activity_type').item() == "time_off":
            # round timestamp to the nearest 5 minutes is not needed for time_off since it's entered manually on allowed/scheduled time
            round_time = False
            logger.info('CUSTOM INFO: Processing Time Off')
            start_timezone = df['start_timezone'][0]
            end_timezone = df['end_timezone'][0]
            df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], unit='s')
            df['created_at'] = pd.to_datetime(df['created_at'], unit='s')
            if 'modified_at' in df.columns:
                df['modified_at'] = pd.to_datetime(df['modified_at'], unit='s')
            df['shift_start_date'] = df['start_timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x, tz=ZoneInfo(start_timezone)).date())
            df['shift_end_date'] = df['end_timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x, tz=ZoneInfo(end_timezone)).date())
            df['shift_start_time'] = df['start_timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x, tz=ZoneInfo(start_timezone)).strftime('%H:%M:%S'))
            df['shift_end_time'] = df['end_timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x, tz=ZoneInfo(end_timezone)).strftime('%H:%M:%S'))
            if round_time is True:
                start_timestamp = int(df['start_timestamp'][0])
                end_timestamp = int(df['end_timestamp'][0])
                df['start_timestamp'] = round_to_nearest_5_minutes(start_timestamp)
                df['end_timestamp'] = round_to_nearest_5_minutes(end_timestamp)

            ct_timesheet_sk_cols = [
                'activity_type', 'event_type','connecteam_user_id',
                'time_clock_id', 'time_activity_id', 'start_timestamp',
                'start_timezone', 'end_timestamp', 'end_timezone',
                'created_at', 'time_off_policy_type_id'
                ]
            df['timesheet_sk'] = df[ct_timesheet_sk_cols].apply(compute_md5, axis=1)
            db_cols_needed = ['request_id', 'company', 'activity_type', 'event_timestamp',
                'event_type', 'connecteam_user_id', 'time_clock_id', 'time_activity_id',
                'start_timestamp', 'start_timezone', 'end_timestamp', 'end_timezone',
                'created_at', 'time_off_policy_type_id', 'shift_start_date',
                'shift_end_date', 'shift_start_time', 'shift_end_time','timesheet_sk', 
                ]
            df = df[db_cols_needed]

        # transform data if update or create
        else:
            start_timezone = df['start_timezone'][0]
            end_timezone = df['end_timezone'][0]
            df['event_timestamp'] = pd.to_datetime(df['event_timestamp'], unit='s')
            df['created_at'] = pd.to_datetime(df['created_at'], unit='s')
            if 'modified_at' in df.columns:
                df['modified_at'] = pd.to_datetime(df['modified_at'], unit='s')
            df['shift_start_date'] = df['start_timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x, tz=ZoneInfo(start_timezone)).date())
            df['shift_end_date'] = df['end_timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x, tz=ZoneInfo(end_timezone)).date())
            df['shift_start_time'] = df['start_timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x, tz=ZoneInfo(start_timezone)).strftime('%H:%M:%S'))
            df['shift_end_time'] = df['end_timestamp'].apply(lambda x: datetime.datetime.fromtimestamp(x, tz=ZoneInfo(end_timezone)).strftime('%H:%M:%S'))
            # round timestamp to the nearest 5 minutes
            if round_time is True:
                start_timestamp = int(df['start_timestamp'][0])
                end_timestamp = int(df['end_timestamp'][0])
                df['start_timestamp'] = round_to_nearest_5_minutes(start_timestamp)
                df['end_timestamp'] = round_to_nearest_5_minutes(end_timestamp)

            ct_timesheet_sk_cols = [
                'activity_type', 'event_type','connecteam_user_id',
                'time_clock_id', 'time_activity_id', 'start_timestamp',
                'start_timezone', 'end_timestamp', 'end_timezone',
                'created_at','job_id', 'sub_job_id',
                ]
            df['timesheet_sk'] = df[ct_timesheet_sk_cols].apply(compute_md5, axis=1)
            db_cols_needed = ['request_id', 'company', 'activity_type', 'event_timestamp',
                'event_type', 'connecteam_user_id', 'time_clock_id', 'time_activity_id',
                'start_timestamp', 'start_timezone', 'end_timestamp', 'end_timezone',
                'created_at', 'job_id', 'sub_job_id', 'is_auto_clock_out', 'shift_start_date',
                'shift_end_date', 'shift_start_time', 'shift_end_time','timesheet_sk', 
                ]
            df = df[db_cols_needed]

        return df
    except Exception as ex:
        logger.exception('Could not transform response into dataframe ', ex)


def insert_ct_timesheet_to_db(ct_timesheet_df):
    """Insert the processed timesheet dataframe into the PostgreSQL database."""
    engine = db_connection(DB_USER=PG_DB_USER, DB_PASSWORD=PG_DB_PASSWORD, ENDPOINT=PG_ENDPOINT, DB_NAME=PG_DB_NAME, db_type='POSTGRESQL')

    # Execute the query
    db_query_manager = DB_QUERY_MANAGER(engine=engine)

    # Perform a batch upsert using 'time_activity_id' as the unique identifier.
    df_status = db_query_manager.batch_upsert(df=ct_timesheet_df, schema="operations", table="webhook_ct_timesheet", business_key="time_activity_id")
    return df_status


def everee_timesheet_payload(df):
    '''
    Function to convert ct time sheet data into everee time sheet payload format
    '''
    everee_payload = df
    if 'external_worker_id' not in everee_payload.columns:
        everee_payload['external_worker_id'] = None

    # set action_type to delete if event_type is delete
    if (df['event_type'].str.contains('delete').any() == True) or (df['event_type'].str.contains('declined').any() == True):
        logger.info("Custom INFO: Setting everee payload to delete/decline type for delete event_type")
        # extract the cols needed for everee payload
        everee_cols = [
        'worker_id', 'external_worker_id', 'event_type', 'activity_type',
        'full_name', 'time_activity_id','everee_action_type', 'everee_sync_state'
        ]
        everee_keys = {
            "worker_id": "workerId",
            "external_worker_id": "externalWorkerId",
            "time_activity_id": "ct_time_activity_id"
        }
        everee_payload = df[everee_cols]

        # add correction payment to next payroll payment
        everee_payload['correctionPaymentTimeframe'] = 'NEXT_PAYROLL_PAYMENT'
    else:
        logger.info("Custom INFO: Setting everee payload to create/edit type for create/edit event_type")
        everee_cols = [
        'worker_id', 'external_worker_id', 'start_timestamp', 'end_timestamp',
        'event_type', 'activity_type', 'full_name', 'time_activity_id', 'note',
        'everee_action_type', 'override_rate', 'everee_sync_state'
        ]
        # rename columns to everee timesheet api keys
        everee_keys = {
            "worker_id": "workerId",
            "external_worker_id": "externalWorkerId",
            "start_timestamp": "shiftStartEpochSeconds",
            "end_timestamp": "shiftEndEpochSeconds",
            "time_activity_id": "ct_time_activity_id",
            "override_rate": "override_rate",
            "note": "note"
        }
        everee_payload = df[everee_cols]

        # add correction payment to next payroll payment
        everee_payload['correctionPaymentTimeframe'] = 'NEXT_PAYROLL_PAYMENT'
    everee_payload.rename(columns=everee_keys, inplace=True)
    everee_payload = everee_payload.to_json(orient="records")
    return everee_payload


def retrieve_worker_and_pay_details(df):
    """
    Retrieve worker details from database

    Args:
        df: DataFrame containing worker data
        DB_USER: Database username
        DB_PASSWORD: Database password
        ENDPOINT: Database endpoint
        DB_NAME: Database name

    Returns:
        DataFrame with worker details or None if error occurs
    """
    try:
        if df is None or df.empty:
            logger.error("Custom ERROR(DataFrame): Input DataFrame is empty or None")
            return None

        # if event_type is delete then retrieve worker without pay and note details
        if (df['event_type'].str.contains('delete').any() == True) or (df['event_type'].str.contains('declined').any() == True):
            ct_user_id = str(df['connecteam_user_id'][0])
            if ct_user_id is not None:
                query = f"""
                        select
                            distinct a.first_name || ' ' ||  a.last_name as full_name,
                            a.worker_id,
                            a.connecteam_id::integer as connecteam_user_id,
                            a.title,
                            a.approval_group,
                            a.ftn_id as external_worker_id
                        from operations.all_workers a
                        where a.connecteam_id = '{ct_user_id}'
                        """
                worker_details_df = retrieve_from_db(query)
                if not worker_details_df.empty:
                    return worker_details_df
                else:
                    logger.info(f"Custom WARNING(DB): No worker details found for user_id: {ct_user_id}, job_id: {job_id}")
                    return pd.DataFrame()

        # handling time off events
        elif df.get('activity_type').item().lower() == "time_off":
            ct_user_id = str(df['connecteam_user_id'][0])
            time_off_id = str(df['time_off_policy_type_id'][0])
            if ct_user_id is not None and time_off_id is not None:
                query = f"""
                        select
                            distinct a.first_name || ' ' ||  a.last_name as full_name,
                            a.worker_id,
                            a.connecteam_id::integer as connecteam_user_id,
                            a.title,
                            a.approval_group,
                            a.ftn_id as external_worker_id,
                            p.override_rate,
                            p.time_off AS note
                        from operations.all_workers a
                        LEFT JOIN operations.time_off_rates p ON lower(trim(a.approval_group)) = lower(trim(p.current_approval_group))
                        where a.connecteam_id = '{ct_user_id}' AND p.time_off_id = '{time_off_id}'
                        """
                worker_details_df = retrieve_from_db(query)
                if not worker_details_df.empty:
                    return worker_details_df
                else:
                    logger.warning('CUSTOM WARNING(DB): Connecteam user_id or time_off_policy not found in database. Please check if the time_off policy ids and user_id in the databse are the latest with what is Connecteam')
                    return pd.DataFrame()
        else:
            ct_user_id = str(df['connecteam_user_id'][0])
            job_id = str(df['job_id'][0])
            sub_job_id = str(df['sub_job_id'][0])
            if ct_user_id is not None and job_id is not None and sub_job_id is not None:
                query = f"""
                        select
                            distinct a.first_name || ' ' ||  a.last_name as full_name,
                            a.worker_id,
                            a.connecteam_id::integer as connecteam_user_id,
                            a.title,
                            a.approval_group,
                            a.ftn_id as external_worker_id,
                            p.override_rate,
                            c.job_title || ',' || c.subjob_title AS note
                        from operations.all_workers a
                        LEFT JOIN operations.hourly_pay_rates p ON lower(trim(a.approval_group)) = lower(trim(p.current_approval_group))
                        LEFT JOIN operations.ct_jobs c ON p.job_id = c.job_id OR lower(trim(p.job)) = lower(trim(c.job_title))
                        where a.connecteam_id = '{ct_user_id}' AND c.job_id = '{job_id}' AND c.subjob_id = '{sub_job_id}'
                        """
                worker_details_df = retrieve_from_db(query)
                if not worker_details_df.empty:
                    return worker_details_df
                else:
                    print(f"Custom WARNING(DB): No worker details found for user_id: {ct_user_id}, job_id: {job_id}")
                    pd.DataFrame()
    except Exception as ex:
        logger.exception('Custom ERROR(DB): ', ex)
        return pd.DataFrame()


def check_if_ct_exist(df):
    """Check if the Connecteam timesheet already exists in the database."""
    # Connect to database
    engine = db_connection(DB_USER=PG_DB_USER, DB_PASSWORD=PG_DB_PASSWORD, ENDPOINT=PG_ENDPOINT, DB_NAME=PG_DB_NAME, db_type='POSTGRESQL')

    # Execute the query
    db_query_manager = DB_QUERY_MANAGER(engine=engine)

    ct_user_id = str(df['connecteam_user_id'][0])
    time_activity_id = str(df['time_activity_id'][0])

    query = f"""
        select
            distinct connecteam_user_id,
            time_activity_id,
            everee_sync_state,
            timesheet_sk
        from operations.webhook_ct_timesheet
        where connecteam_user_id = '{ct_user_id}' and time_activity_id = '{time_activity_id}'
        """
    check_ct_timesheet_df = db_query_manager.fetch_from_db(query)
    return check_ct_timesheet_df


def everee_timesheet_exist(everee_payload: dict) -> pd.DataFrame:
    """Check if the Everee timesheet already exists in the database."""
    # Connect to database
    engine = db_connection(DB_USER=PG_DB_USER, DB_PASSWORD=PG_DB_PASSWORD, ENDPOINT=PG_ENDPOINT, DB_NAME=PG_DB_NAME, db_type='POSTGRESQL', PORT=PG_PORT)

    # Execute the query
    db_query_manager = DB_QUERY_MANAGER(engine=engine)

    worker_id = everee_payload.get("workerId", None)
    ct_time_activity_id = everee_payload.get("ct_time_activity_id", None)

    query = f"""
        select
            *
        from operations.webhook_everee_timesheet
        where worker_id = '{worker_id}' and ct_time_activity_id = '{ct_time_activity_id}'
        """
    check_everee_timesheet_df = db_query_manager.fetch_from_db(query)
    if not check_everee_timesheet_df.empty:
        return check_everee_timesheet_df
    else:
        return pd.DataFrame()


def derive_everee_action_type(df):
    """
    Determine the action type for Everee based on the webhook data
    """
    ct_extist_df = check_if_ct_exist(df)
    if not ct_extist_df.empty:
        everee_sync_state = ct_extist_df.get('everee_sync_state', None)
        # if ct_extist_df and event type = delete then set action_type is delete
        if (df['event_type'].str.contains('delete').any() == True) or (df['event_type'].str.contains('declined').any() == True):
            logger.info("Custom INFO: Setting everee action type to delete since time has been deleted or declined in Connecteam")
            df['everee_action_type'] = 'delete'
            df['everee_sync_state'] = everee_sync_state
        # if ct_extist_df and event type = edit and timesheet_sk then set action_type is update
        elif ((df['event_type'].str.contains('delete').any() == False) or (df['event_type'].str.contains('declined').any() == False)):
            logger.info("Custom INFO: Record found - Setting everee action type to update")
            df['everee_action_type'] = 'update'
            df['everee_sync_state'] = everee_sync_state
    else:
        logger.info("Custom INFO: Record not found - Setting everee action type to create")
        df['everee_action_type'] = 'create'
        df['everee_sync_state'] = None
    return df


def determine_everee_sync_state(row: pd.DataFrame) -> str:
    """
    Determine the Everee sync state based on the end timestamp.
    Used identify events to be scheduled for future clock-outs.
    """
    try:
        everee_action_type = row['everee_action_type']
        everee_sync_state = row.get('everee_sync_state', None)
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        end_timestamp = row.get("end_timestamp", None)
        # check if it's a clock out event and process scheduler accordingly
        if end_timestamp is not None:
            # Convert the epoch time to a timezone-aware UTC datetime object
            clock_out = datetime.datetime.fromtimestamp(end_timestamp, tz=datetime.timezone.utc)
            if (clock_out > now_utc) and everee_action_type != 'delete':
                return "SCHEDULED"
            if everee_action_type == 'delete':
                return "DELETE"
            return "SENT"

        # handle time off events and delete events
        if everee_action_type in ['create', 'delete'] and everee_sync_state == 'SCHEDULED':
            return "DELETE"
    except Exception as ex:
        logger.exception(f"CUSTOM ERROR: Could not determine everee sync state due to: {ex}")
        return None


# function to invoke lambda function to update user details
def invoke_lambda(event_payload, FUNCTION_NAME):
    '''
    Invoke lambda fuction
    '''
    function_name = FUNCTION_NAME
    try:
        client = boto3.client('lambda')
        client.invoke(
            FunctionName=function_name,
            InvocationType='Event',
            Payload=json.dumps(event_payload),
            LogType='Tail'
        )
        print(f'Payload passed to other function {event_payload}')
    except Exception as ex:
        logger.exception(ex)
        print(f'Could not pass {event_payload} due to ', ex)
    return