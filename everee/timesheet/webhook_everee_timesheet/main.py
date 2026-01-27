import os
import pandas as pd
import json
import logging
from process_timesheet import everee_create_shift
from process_timesheet import process_res
from process_timesheet import transform_ct_payload
from process_timesheet import insert_to_db
from process_timesheet import process_update
from process_timesheet import update_timesheet
from process_timesheet import standardize_column_name
from process_timesheet import process_delete
from process_timesheet import update_sync_state
from utils import invoke_lambda_function


logger = logging.getLogger(__name__)


TENANT_ID = os.environ.get('TENANT_ID')
API_TOKEN = os.environ.get('API_TOKEN')
FUNCTION_NAME = os.environ.get('FUNCTION_NAME')
GSHEETS_FUNCTION = os.environ.get('GSHEETS_FUNCTION')
EVENTBRIDGE_FUNCTION_NAME = os.environ.get('EVENTBRIDGE_FUNCTION_NAME')


def lambda_handler(event, context):
    print(json.dumps(event))
    payload = event
    ct_payload_df = transform_ct_payload(event)
    everee_sync_state = payload.get('everee_sync_state', None)
    ct_timesheet_id = payload.get('ct_timesheet_id', None)
    try:
        if payload.get('everee_action_type') == 'create':
            create_shift_res = everee_create_shift(payload=payload)
            everee_timesheet = process_res(create_shift_res, ct_payload_df)
            insert_to_db(everee_timesheet, schema="operations", table_name="webhook_everee_timesheet", business_key="worked_shift_id")
        elif payload.get('everee_action_type') == 'update':
            print("Updating shift in Everee")
            delete_shift_res = process_delete(ct_payload_df)
            if delete_shift_res in ["success", "bad request"]:
                everee_timesheet = process_res(delete_shift_res, ct_payload_df)
                everee_timesheet.columns = [standardize_column_name(col) for col in everee_timesheet.columns]
                insert_to_db(everee_timesheet, schema="operations", table_name="webhook_everee_timesheet", business_key="worked_shift_id")
                # re-submit
                print("Resubmitting shift after deletion")
                create_shift_res = everee_create_shift(payload=payload)
                everee_timesheet = process_res(create_shift_res, ct_payload_df)
                insert_to_db(everee_timesheet, schema="operations", table_name="webhook_everee_timesheet", business_key="worked_shift_id")
            else:
                create_shift_res = everee_create_shift(payload=payload)
                everee_timesheet = process_res(create_shift_res, ct_payload_df)
                insert_to_db(everee_timesheet, schema="operations", table_name="webhook_everee_timesheet", business_key="worked_shift_id")
        # handle time off rejection or timesheet rejection
        elif (payload.get('everee_action_type') == 'update') and 'declined' in payload.get('event_type', ''):
            delete_shift_res = process_delete(ct_payload_df)
            if delete_shift_res == "success":
                everee_timesheet = process_res(delete_shift_res, ct_payload_df)
                everee_timesheet.columns = [standardize_column_name(col) for col in everee_timesheet.columns]
                insert_to_db(everee_timesheet, schema="operations", table_name="webhook_everee_timesheet", business_key="worked_shift_id")
        # handle delete action in everee
        elif payload.get('everee_action_type') == 'delete':
            delete_shift_res = process_delete(ct_payload_df)
            if delete_shift_res == "success":
                everee_timesheet = process_res(delete_shift_res, ct_payload_df)
                everee_timesheet.columns = [standardize_column_name(col) for col in everee_timesheet.columns]
                insert_to_db(everee_timesheet, schema="operations", table_name="webhook_everee_timesheet", business_key="worked_shift_id")
        else:
            print("No valid everee_action_type found in payload")

        # if the sync state is scheduled and ct_timesheet_id is present, update the sync state and invoke eventbridge lambda to delete schedule
        if everee_sync_state in ["SCHEDULED", "DELETE"] and ct_timesheet_id is not None:
            sync_sts = update_sync_state(ct_timesheet_id, )
            eventbridge_payload = {"schedule_action": "DELETE"}
            eventbridge_payload["schedule_name"] = payload["schedule_name"]
            if sync_sts is not None:
                logger.info("CUSTOM INFO: Sync state updated successfully")
                invoke_lambda_function(eventbridge_payload, EVENTBRIDGE_FUNCTION_NAME)

        return {
            'statusCode': 200,
            'body': 'lambda successfully executed'
        }
    except Exception as ex:
        logger.error(ex)
    return {
            'statusCode': 500,
            'body': "lambda didn't finish running"
        }