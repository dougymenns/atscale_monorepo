import os
import pandas as pd
import json
import logging
from process_timesheet import transform_ct_payload
from process_timesheet import handle_delete_action
from process_timesheet import handle_create_action
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
        # handle create time sheet
        if payload.get('everee_action_type') == 'create':
            everee_timesheet_create_sts = handle_create_action(payload, ct_payload_df)
            if everee_timesheet_create_sts is None:
                logger.warning("Couldn't complete delete action processing")
        # handle update timesheet
        elif payload.get('everee_action_type') == 'update':
            logger.info("Updating shift in Everee")
            # handle timesheet deletion process from API to DB
            everee_timesheet_del_sts = handle_delete_action(ct_payload_df)
            # re-submit timesheet
            if everee_timesheet_del_sts is not None:
                logger.info("Resubmitting shift after deletion")
                everee_timesheet_create_sts = handle_create_action(payload)
                if everee_timesheet_create_sts is None:
                    logger.warning("Couldn't complete delete action processing")
            else:
                everee_timesheet_create_sts = handle_create_action(payload, ct_payload_df)
                if everee_timesheet_create_sts is None:
                    logger.warning("Couldn't complete delete action processing")
        # handle time off rejection or timesheet rejection
        elif (payload.get('everee_action_type') == 'update') and 'declined' in payload.get('event_type', ''):
            # handle timesheet deletion process from API to DB
            everee_timesheet_del_sts = handle_delete_action(ct_payload_df)
        # handle delete action in everee
        elif payload.get('everee_action_type') == 'delete':
            # handle timesheet deletion process from API to DB
            everee_timesheet_del_sts = handle_delete_action(ct_payload_df)
            if everee_timesheet_del_sts is None:
                logger.warning("Couldn't complete delete action processing")
        else:
            logger.warning("No valid everee_action_type found in payload")

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