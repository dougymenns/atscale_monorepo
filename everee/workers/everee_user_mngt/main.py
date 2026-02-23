import os
import json
import logging
from utils import SCD2Manager
from utils import DB_QUERY_MANAGER
from utils import invoke_lambda_function
from process_users import transfrom_user
from process_users import everee_api_request
from db_utils import db_connection


logger = logging.getLogger(__name__)


# POSTGRES CONFIG
PG_ENDPOINT = os.getenv("PG_ENDPOINT")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB_NAME = os.getenv("PG_DB_NAME",)
PG_DB_USER = os.getenv("PG_DB_USER",)
PG_DB_PASSWORD = os.getenv("PG_DB_PASSWORD")

# REDSHIFT CREDENTIALS
# ENDPOINT = os.environ.get('ENDPOINT')
# DB_USER = os.environ.get('DB_USER')
# DB_PASSWORD = os.environ.get('DB_PASSWORD')
# DB_PORT = os.environ.get('DB_PORT')
# DB_NAME = os.environ.get('DB_NAME')

TENANT_ID = os.environ.get('TENANT_ID')
API_TOKEN = os.environ.get('API_TOKEN')
FUNCTION_NAME = os.environ.get('FUNCTION_NAME')
GSHEETS_FUNCTION = os.environ.get('GSHEETS_FUNCTION')


def lambda_handler(event, context):
    print(json.dumps(event))
    try:
        event_res = event.get('body').strip("'")
        # Convert the JSON string to a Python dictionary
        res_body = json.loads(event_res)
        # retrive tenant id
        tenant_id = str(res_body.get('data').get('object').get('companyId'))
        # pass worker_id json to invoke function
        worker_id = str(res_body.get('data').get('object').get('workerId'))
        ext_id = str(res_body.get('data').get('object').get('externalWorkerId'))

        # json to pass to other lambda function
        worker_id_json = {
            "worker_id": worker_id,
            "ext_id": ext_id
            }

        engine = db_connection(DB_USER=PG_DB_USER, DB_PASSWORD=PG_DB_PASSWORD, ENDPOINT=PG_ENDPOINT, DB_NAME=PG_DB_NAME, db_type='POSTGRESQL')

        # handling profile updated event and onboard complete
        if res_body.get('type') == 'worker.profile-updated' or res_body.get('type') == 'worker.onboarding-completed' or res_body.get('type') == 'worker.deleted':
            # retrieve user data from everee api
            user_api_data = everee_api_request(worker_id, API_TOKEN, tenant_id)
            # transform user data
            worker_api_df = transfrom_user(user_api_data)
            print(f'Record Info: {worker_api_df.shape}')

            # apply scd2 everee user table
            scd2 = SCD2Manager(
                engine=engine,
                schema="operations",
                table="dim_everee_users",
                business_key="worker_id",
                surrogate_key="everee_sk"
                )
            scd2_status = scd2.apply_scd2(worker_api_df)
            if scd2_status:
                db_query_manager = DB_QUERY_MANAGER(engine=engine)
                ct_user_df = db_query_manager.fetch_from_db(f"""
                                    SELECT distinct worker_id,ext_id FROM operations.dim_ct_users where everee_wkr_id = '{worker_id}' and ftn_id = '{ext_id}';
                                    """)
                ct_user_df = ct_user_df.drop_duplicates(subset=['worker_id', 'ext_id'], keep='last')
                if not ct_user_df.empty:
                    stored_proc_query = f"""CALL operations.sync_single_worker('{worker_id}');"""
                    db_query_manager.stored_procedure(stored_proc_query)
                    invoke_lambda_function(payload=worker_id_json, FUNCTION_NAME=FUNCTION_NAME)
                    invoke_lambda_function(payload=worker_id_json, FUNCTION_NAME=GSHEETS_FUNCTION)
        else:
            # if event type is not profile updated or onboard completed or deleted
            print(f'CUSTOM INFO: Event type {res_body.get("type")} not supposed to be handled in this lambda. Event processing skipped.')

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