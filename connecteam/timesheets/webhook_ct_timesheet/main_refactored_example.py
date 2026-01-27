"""
Refactored main.py example for better testability.

This demonstrates:
- Configuration extraction
- Dependency injection
- Service classes for external dependencies
- Separation of concerns
- Test-friendly structure
"""
import os
import json
import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass
import pandas as pd

from process_timesheet import (
    process_timesheet_data,
    insert_ct_timesheet_to_db,
    everee_timesheet_payload,
    retrieve_worker_and_pay_details,
    derive_everee_action_type,
    determine_everee_sync_state,
    everee_timesheet_exist,
)
from utils import invoke_lambda_function

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION
# ============================================================================

@dataclass
class Config:
    """Configuration class for database and Lambda settings."""
    endpoint: Optional[str] = None
    db_user: Optional[str] = None
    db_password: Optional[str] = None
    db_port: Optional[str] = None
    db_name: Optional[str] = None
    function_name: Optional[str] = None
    eventbridge_function_name: Optional[str] = None
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Create configuration from environment variables."""
        return cls(
            endpoint=os.environ.get('ENDPOINT'),
            db_user=os.environ.get('DB_USER'),
            db_password=os.environ.get('DB_PASSWORD'),
            db_port=os.environ.get('DB_PORT'),
            db_name=os.environ.get('DB_NAME'),
            function_name=os.environ.get('FUNCTION_NAME'),
            eventbridge_function_name=os.environ.get('EVENTBRIDGE_FUNCTION_NAME'),
        )
    
    @classmethod
    def for_testing(cls) -> 'Config':
        """Create a test configuration with dummy values."""
        return cls(
            endpoint='test-endpoint',
            db_user='test-user',
            db_password='test-password',
            db_port='5432',
            db_name='test-db',
            function_name='test-function',
            eventbridge_function_name='test-eventbridge-function',
        )


# ============================================================================
# SERVICE CLASSES (Wrappers for External Dependencies)
# ============================================================================

class LambdaService:
    """Service for invoking Lambda functions."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def invoke_function(self, payload: Dict[str, Any], function_name: str) -> None:
        """Invoke a Lambda function with the given payload."""
        invoke_lambda_function(payload, function_name)
    
    def invoke_main_function(self, payload: Dict[str, Any]) -> None:
        """Invoke the main Lambda function."""
        if self.config.function_name:
            self.invoke_function(payload, self.config.function_name)
    
    def invoke_eventbridge_function(self, payload: Dict[str, Any]) -> None:
        """Invoke the EventBridge Lambda function."""
        if self.config.eventbridge_function_name:
            self.invoke_function(payload, self.config.eventbridge_function_name)


class DatabaseService:
    """Service for database operations."""
    
    def __init__(self, config: Config):
        self.config = config
    
    def insert_timesheet(self, timesheet_df: pd.DataFrame) -> bool:
        """Insert timesheet data into the database."""
        return insert_ct_timesheet_to_db(timesheet_df)
    
    def retrieve_worker_details(self, df: pd.DataFrame) -> pd.DataFrame:
        """Retrieve worker and pay details from the database."""
        return retrieve_worker_and_pay_details(df)
    
    def check_everee_timesheet_exists(self, payload: Dict[str, Any]) -> pd.DataFrame:
        """Check if Everee timesheet exists in the database."""
        return everee_timesheet_exist(payload)


# ============================================================================
# BUSINESS LOGIC PROCESSOR
# ============================================================================

class TimesheetProcessor:
    """Main processor for handling timesheet webhook events."""
    
    def __init__(
        self,
        config: Config,
        db_service: Optional[DatabaseService] = None,
        lambda_service: Optional[LambdaService] = None,
    ):
        self.config = config
        self.db_service = db_service or DatabaseService(config)
        self.lambda_service = lambda_service or LambdaService(config)
    
    def process(self, event: Dict[str, Any]) -> Dict[str, int]:
        """
        Main processing method for timesheet webhook events.
        
        Args:
            event: The webhook event payload
            
        Returns:
            Dictionary with statusCode and body
        """
        try:
            # Step 1: Transform and process CT timesheet webhook
            df = process_timesheet_data(event)
            
            # Step 2: Retrieve worker details from database
            worker_details_df = self.db_service.retrieve_worker_details(df)
            
            # Step 3: Process if worker details exist
            if worker_details_df.empty:
                logger.warning("No worker details found for timesheet event")
                return {
                    'statusCode': 404,
                    'body': 'No worker details found'
                }
            
            # Step 4: Determine action type and merge data
            ct_df = derive_everee_action_type(df)
            ct_timesheet_df = ct_df.merge(
                worker_details_df,
                how='left',
                on='connecteam_user_id'
            )
            
            # Step 5: Determine Everee sync state
            ct_timesheet_df['everee_sync_state'] = ct_timesheet_df.apply(
                lambda x: determine_everee_sync_state(x),
                axis=1
            )
            
            # Step 6: Insert timesheet to database
            df_status = self.db_service.insert_timesheet(ct_timesheet_df)
            
            if not df_status:
                logger.error("Failed to insert timesheet to database")
                return {
                    'statusCode': 500,
                    'body': 'Failed to insert timesheet to database'
                }
            
            # Step 7: Process Everee payload if worker IDs exist
            if not (ct_timesheet_df['worker_id'].astype(bool).any() or 
                    ct_timesheet_df['external_worker_id'].astype(bool).any()):
                return {
                    'statusCode': 200,
                    'body': 'Timesheet processed but no worker IDs found'
                }
            
            # Step 8: Create and send Everee payload
            return self._process_everee_payload(ct_timesheet_df)
            
        except Exception as ex:
            logger.error(f"Error processing timesheet: {ex}", exc_info=True)
            return {
                'statusCode': 500,
                'body': "lambda didn't finish running"
            }
    
    def _process_everee_payload(self, ct_timesheet_df: pd.DataFrame) -> Dict[str, int]:
        """Process Everee payload and invoke appropriate Lambda functions."""
        # Create Everee payload
        everee_payload_df = everee_timesheet_payload(ct_timesheet_df)
        everee_sync_state = ct_timesheet_df.get('everee_sync_state').item()
        everee_payload = json.loads(everee_payload_df)[0]
        
        # Check if Everee timesheet exists
        everee_exists = self.db_service.check_everee_timesheet_exists(ct_timesheet_df)
        
        # Prepare schedule action if needed
        if everee_sync_state == "DELETE":
            everee_payload["schedule_action"] = "DELETE"
            everee_payload["schedule_name"] = f"submit_timesheet_{everee_payload['ct_time_activity_id']}"
        
        # Invoke Lambda functions based on sync state
        if everee_sync_state in ['SCHEDULED', 'DELETE'] and not everee_exists.empty:
            self.lambda_service.invoke_main_function(everee_payload)
            self.lambda_service.invoke_eventbridge_function(everee_payload)
        elif everee_sync_state in ['SCHEDULED', 'DELETE'] and everee_exists.empty:
            self.lambda_service.invoke_eventbridge_function(everee_payload)
        else:
            self.lambda_service.invoke_main_function(everee_payload)
        
        return {
            'statusCode': 200,
            'body': 'lambda successfully executed'
        }


# ============================================================================
# LAMBDA HANDLER (Thin Wrapper)
# ============================================================================

def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    
    This is kept thin - it just creates dependencies and delegates to the processor.
    """
    print(json.dumps(event))
    
    config = Config.from_env()
    processor = TimesheetProcessor(config)
    
    return processor.process(event)
