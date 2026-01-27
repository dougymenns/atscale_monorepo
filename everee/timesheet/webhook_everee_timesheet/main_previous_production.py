import os
import pandas as pd
import json
import logging
from process_timesheet import everee_create_shift
from process_timesheet import process_res
from process_timesheet import transform_ct_payload
from process_timesheet import insert_to_db
from process_timesheet import process_update
from process_timesheet import standardize_column_name
from process_timesheet import process_delete


logger = logging.getLogger(__name__)


# REDSHIFT CREDENTIALS
ENDPOINT = os.environ.get('ENDPOINT')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_PORT = os.environ.get('DB_PORT')
DB_NAME = os.environ.get('DB_NAME')

TENANT_ID = os.environ.get('TENANT_ID')
API_TOKEN = os.environ.get('API_TOKEN')
FUNCTION_NAME = os.environ.get('FUNCTION_NAME')
GSHEETS_FUNCTION = os.environ.get('GSHEETS_FUNCTION')


def lambda_handler(event, context):
    print(json.dumps(event))
    payload = event
    ct_payload_df = transform_ct_payload(event)
    try:
        if payload.get('everee_action_type') == 'create':
            create_shift_res = everee_create_shift(payload=payload)
            everee_timesheet = process_res(create_shift_res, ct_payload_df)
            insert_to_db(everee_timesheet, DB_USER, DB_PASSWORD, ENDPOINT, DB_NAME, schema="public", table_name="webhook_everee_timesheet")
        elif payload.get('everee_action_type') == 'update':
            # update_shift_res = process_update(ct_payload_df)
            # everee_timesheet = process_res(update_shift_res, ct_payload_df)
            # everee_timesheet.columns = [standardize_column_name(col) for col in everee_timesheet.columns]
            # insert_to_db(everee_timesheet, DB_USER, DB_PASSWORD, ENDPOINT, DB_NAME, schema="public", table_name="webhook_everee_timesheet")
            # due to update with override pay in update method. Delete entry first and then re-submit
            delete_shift_res = process_delete(ct_payload_df)
            everee_timesheet = process_res(delete_shift_res, ct_payload_df)
            everee_timesheet.columns = [standardize_column_name(col) for col in everee_timesheet.columns]
            insert_to_db(everee_timesheet, DB_USER, DB_PASSWORD, ENDPOINT, DB_NAME, schema="public", table_name="webhook_everee_timesheet")
            # re-submit
            create_shift_res = everee_create_shift(payload=payload)
            everee_timesheet = process_res(create_shift_res, ct_payload_df)
            insert_to_db(everee_timesheet, DB_USER, DB_PASSWORD, ENDPOINT, DB_NAME, schema="public", table_name="webhook_everee_timesheet")
        # handle time off rejection or timesheet rejection
        elif (payload.get('everee_action_type') == 'update') and 'declined' in payload.get('event_type', ''):
            delete_shift_res = process_delete(ct_payload_df)
            everee_timesheet = process_res(delete_shift_res, ct_payload_df)
            everee_timesheet.columns = [standardize_column_name(col) for col in everee_timesheet.columns]
            insert_to_db(everee_timesheet, DB_USER, DB_PASSWORD, ENDPOINT, DB_NAME, schema="public", table_name="webhook_everee_timesheet")
        elif payload.get('everee_action_type') == 'delete':
            delete_shift_res = process_delete(ct_payload_df)
            everee_timesheet = process_res(delete_shift_res, ct_payload_df)
            everee_timesheet.columns = [standardize_column_name(col) for col in everee_timesheet.columns]
            insert_to_db(everee_timesheet, DB_USER, DB_PASSWORD, ENDPOINT, DB_NAME, schema="public", table_name="webhook_everee_timesheet")
    except Exception as ex:
        logger.error(ex)
    return {
        'statusCode': 200,
        'body': 'lambda successfully executed'
    }