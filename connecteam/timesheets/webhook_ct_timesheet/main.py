import os
import json
import logging
from process_timesheet import process_timesheet_data
from process_timesheet import insert_ct_timesheet_to_db
from process_timesheet import everee_timesheet_payload
from process_timesheet import retrieve_worker_and_pay_details
from process_timesheet import derive_everee_action_type
from process_timesheet import determine_everee_sync_state
from process_timesheet import everee_timesheet_exist
from utils import invoke_lambda_function

logger = logging.getLogger(__name__)


# REDSHIFT CREDENTIALS
ENDPOINT = os.environ.get('ENDPOINT')
DB_USER = os.environ.get('DB_USER')
DB_PASSWORD = os.environ.get('DB_PASSWORD')
DB_PORT = os.environ.get('DB_PORT')
DB_NAME = os.environ.get('DB_NAME')


FUNCTION_NAME = os.environ.get('FUNCTION_NAME')
EVENTBRIDGE_FUNCTION_NAME = os.environ.get('EVENTBRIDGE_FUNCTION_NAME')


def lambda_handler(event, context):
    print(json.dumps(event))
    try:
        # transform and process ct timesheet webhook
        df = process_timesheet_data(event)

        # retrieve other necessary worker details from db
        worker_details_df = retrieve_worker_and_pay_details(df)

        # check if worker_details_df has data
        if not worker_details_df.empty:
            ct_df = derive_everee_action_type(df)
            ct_timesheet_df = ct_df.merge(worker_details_df, how='left', on='connecteam_user_id')
            # determine everee sync state
            ct_timesheet_df['everee_sync_state'] = ct_timesheet_df.apply(lambda x: determine_everee_sync_state(x), axis=1)
            # insert ct timesheet dataframe to db
            df_status = insert_ct_timesheet_to_db(ct_timesheet_df)
            check_everee_timesheet_df = everee_timesheet_exist(ct_timesheet_df)
            if df_status:
                # check if worker_details_df worker id for everee payload
                if ct_timesheet_df['worker_id'].astype(bool).any() or ct_timesheet_df['external_worker_id'].astype(bool).any():
                    everee_payload_df = everee_timesheet_payload(ct_timesheet_df)
                    everee_sync_state = ct_timesheet_df.get('everee_sync_state').item()
                    # convert dataframe to json
                    everee_payload = json.loads(everee_payload_df)[0]

                    # add everee_sync_state to payload and trigger appropriate lambda function
                    if (everee_sync_state in ['SCHEDULED', 'DELETE']) and (not check_everee_timesheet_df.empty):
                        # schedule action based on everee_sync_state
                        if everee_sync_state == "DELETE":
                            everee_payload["schedule_action"] = "DELETE"
                            everee_payload["schedule_name"] = f"submit_timesheet_{everee_payload['ct_time_activity_id']}"
                        invoke_lambda_function(everee_payload, FUNCTION_NAME)
                        invoke_lambda_function(everee_payload, EVENTBRIDGE_FUNCTION_NAME)
                        # delete scheduled timesheet if everee_sync_state is DELETE and no everee_timesheet exist in db
                    elif everee_sync_state in ['SCHEDULED', 'DELETE'] and  (check_everee_timesheet_df.empty):
                        # schedule action based on everee_sync_state
                        if everee_sync_state == "DELETE":
                            everee_payload["schedule_action"] = "DELETE"
                            everee_payload["schedule_name"] = f"submit_timesheet_{everee_payload['ct_time_activity_id']}"
                        invoke_lambda_function(everee_payload, EVENTBRIDGE_FUNCTION_NAME)
                    else:
                        invoke_lambda_function(everee_payload, FUNCTION_NAME)
                    return {
                            'statusCode': 200,
                            'body': 'lambda successfully executed'
                        }
    except Exception as ex:
        logger.exception(ex)
        return {
            'statusCode': 500,
            'body': "lambda didn't finish running"
        }