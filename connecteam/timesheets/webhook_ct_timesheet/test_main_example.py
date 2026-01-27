"""
Example test file demonstrating how to test the refactored main.py structure.

This shows how the dependency injection pattern makes testing much easier.
"""
import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
import json

# Assuming you have the refactored main.py
from main_refactored_example import (
    Config,
    TimesheetProcessor,
    DatabaseService,
    LambdaService,
)


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def test_config():
    """Create a test configuration."""
    return Config.for_testing()


@pytest.fixture
def sample_event():
    """Sample webhook event for testing."""
    return {
        "event_type": "create",
        "activity_type": "timesheet",
        "connecteam_user_id": "12345",
        "time_activity_id": "67890",
        "start_timestamp": 1234567890,
        "end_timestamp": 1234571490,
    }


@pytest.fixture
def mock_db_service():
    """Mock database service."""
    service = Mock(spec=DatabaseService)
    service.retrieve_worker_details.return_value = pd.DataFrame({
        'connecteam_user_id': [12345],
        'worker_id': ['worker-123'],
        'full_name': ['Test User'],
    })
    service.insert_timesheet.return_value = True
    service.check_everee_timesheet_exists.return_value = pd.DataFrame()
    return service


@pytest.fixture
def mock_lambda_service():
    """Mock Lambda service."""
    service = Mock(spec=LambdaService)
    return service


# ============================================================================
# UNIT TESTS
# ============================================================================

class TestConfig:
    """Test configuration class."""
    
    def test_from_env(self, monkeypatch):
        """Test creating config from environment variables."""
        monkeypatch.setenv('ENDPOINT', 'test-endpoint')
        monkeypatch.setenv('DB_USER', 'test-user')
        
        config = Config.from_env()
        
        assert config.endpoint == 'test-endpoint'
        assert config.db_user == 'test-user'
    
    def test_for_testing(self):
        """Test creating test configuration."""
        config = Config.for_testing()
        
        assert config.endpoint == 'test-endpoint'
        assert config.db_user == 'test-user'


class TestTimesheetProcessor:
    """Test the main TimesheetProcessor class."""
    
    def test_process_success(self, test_config, sample_event, mock_db_service, mock_lambda_service):
        """Test successful processing of a timesheet event."""
        # Arrange
        processor = TimesheetProcessor(
            test_config,
            db_service=mock_db_service,
            lambda_service=mock_lambda_service
        )
        
        # Mock the process_timesheet_data function
        with patch('main_refactored_example.process_timesheet_data') as mock_process:
            mock_df = pd.DataFrame({
                'connecteam_user_id': [12345],
                'time_activity_id': [67890],
            })
            mock_process.return_value = mock_df
            
            # Mock derive_everee_action_type
            with patch('main_refactored_example.derive_everee_action_type') as mock_derive:
                mock_derive.return_value = mock_df
                
                # Mock determine_everee_sync_state
                with patch('main_refactored_example.determine_everee_sync_state') as mock_determine:
                    mock_determine.return_value = 'SENT'
                    
                    # Mock everee_timesheet_payload
                    with patch('main_refactored_example.everee_timesheet_payload') as mock_payload:
                        mock_payload.return_value = json.dumps([{'workerId': 'worker-123'}])
                        
                        # Act
                        result = processor.process(sample_event)
                        
                        # Assert
                        assert result['statusCode'] == 200
                        mock_db_service.retrieve_worker_details.assert_called_once()
                        mock_db_service.insert_timesheet.assert_called_once()
    
    def test_process_no_worker_details(self, test_config, sample_event, mock_db_service, mock_lambda_service):
        """Test processing when no worker details are found."""
        # Arrange
        mock_db_service.retrieve_worker_details.return_value = pd.DataFrame()
        processor = TimesheetProcessor(
            test_config,
            db_service=mock_db_service,
            lambda_service=mock_lambda_service
        )
        
        with patch('main_refactored_example.process_timesheet_data') as mock_process:
            mock_df = pd.DataFrame({'connecteam_user_id': [12345]})
            mock_process.return_value = mock_df
            
            # Act
            result = processor.process(sample_event)
            
            # Assert
            assert result['statusCode'] == 404
            assert 'No worker details found' in result['body']
            mock_db_service.insert_timesheet.assert_not_called()
    
    def test_process_database_insert_failure(self, test_config, sample_event, mock_db_service, mock_lambda_service):
        """Test processing when database insert fails."""
        # Arrange
        mock_db_service.insert_timesheet.return_value = False
        processor = TimesheetProcessor(
            test_config,
            db_service=mock_db_service,
            lambda_service=mock_lambda_service
        )
        
        with patch('main_refactored_example.process_timesheet_data') as mock_process:
            mock_df = pd.DataFrame({
                'connecteam_user_id': [12345],
                'time_activity_id': [67890],
            })
            mock_process.return_value = mock_df
            
            with patch('main_refactored_example.derive_everee_action_type') as mock_derive:
                mock_derive.return_value = mock_df
                
                with patch('main_refactored_example.determine_everee_sync_state') as mock_determine:
                    mock_determine.return_value = 'SENT'
                    
                    # Act
                    result = processor.process(sample_event)
                    
                    # Assert
                    assert result['statusCode'] == 500
                    assert 'Failed to insert' in result['body']
    
    def test_process_exception_handling(self, test_config, sample_event, mock_db_service, mock_lambda_service):
        """Test exception handling during processing."""
        # Arrange
        mock_db_service.retrieve_worker_details.side_effect = Exception("Database error")
        processor = TimesheetProcessor(
            test_config,
            db_service=mock_db_service,
            lambda_service=mock_lambda_service
        )
        
        with patch('main_refactored_example.process_timesheet_data') as mock_process:
            mock_df = pd.DataFrame({'connecteam_user_id': [12345]})
            mock_process.return_value = mock_df
            
            # Act
            result = processor.process(sample_event)
            
            # Assert
            assert result['statusCode'] == 500
            assert "lambda didn't finish running" in result['body']
    
    def test_process_everee_payload_scheduled_with_existing(self, test_config, mock_db_service, mock_lambda_service):
        """Test processing Everee payload when sync state is SCHEDULED and timesheet exists."""
        # Arrange
        processor = TimesheetProcessor(
            test_config,
            db_service=mock_db_service,
            lambda_service=mock_lambda_service
        )
        
        ct_timesheet_df = pd.DataFrame({
            'everee_sync_state': ['SCHEDULED'],
            'worker_id': ['worker-123'],
            'external_worker_id': [None],
            'ct_time_activity_id': [67890],
        })
        
        mock_db_service.check_everee_timesheet_exists.return_value = pd.DataFrame({
            'worker_id': ['worker-123']
        })
        
        with patch('main_refactored_example.everee_timesheet_payload') as mock_payload:
            mock_payload.return_value = json.dumps([{
                'workerId': 'worker-123',
                'ct_time_activity_id': 67890
            }])
            
            # Act
            result = processor._process_everee_payload(ct_timesheet_df)
            
            # Assert
            assert result['statusCode'] == 200
            mock_lambda_service.invoke_main_function.assert_called_once()
            mock_lambda_service.invoke_eventbridge_function.assert_called_once()


# ============================================================================
# INTEGRATION TESTS (Optional - use real services)
# ============================================================================

@pytest.mark.integration
class TestTimesheetProcessorIntegration:
    """Integration tests using real services."""
    
    def test_process_with_real_services(self, sample_event):
        """Test with real database and Lambda services."""
        # Note: This would use actual services, so you'd need to set up
        # test databases and mock AWS services
        config = Config.for_testing()
        processor = TimesheetProcessor(config)
        
        # This would make real calls - typically disabled in CI/CD
        # result = processor.process(sample_event)
        # assert result['statusCode'] in [200, 404, 500]
        pass


# ============================================================================
# LAMBDA HANDLER TESTS
# ============================================================================

def test_lambda_handler(test_config, sample_event):
    """Test the lambda_handler function."""
    with patch('main_refactored_example.Config.from_env', return_value=test_config):
        with patch('main_refactored_example.TimesheetProcessor') as mock_processor_class:
            mock_processor = Mock()
            mock_processor.process.return_value = {'statusCode': 200, 'body': 'success'}
            mock_processor_class.return_value = mock_processor
            
            from main_refactored_example import lambda_handler
            
            # Act
            result = lambda_handler(sample_event, None)
            
            # Assert
            assert result['statusCode'] == 200
            mock_processor.process.assert_called_once_with(sample_event)
