# Main.py Refactoring Recommendations for Better Testing

## Current Issues

1. **Monolithic lambda_handler**: All business logic is embedded directly in the handler
2. **Module-level environment variables**: Hard to override in tests
3. **Tight coupling**: Direct imports and calls make mocking difficult
4. **No dependency injection**: External services (DB, Lambda) are instantiated inline
5. **Hard to test in isolation**: Can't test individual steps without running the entire flow

## Recommended Structure

### Option 1: Service Class Pattern (Recommended)

Create a `TimesheetProcessor` service class that encapsulates all business logic:

```python
# main.py structure
class TimesheetProcessor:
    def __init__(self, config, db_service=None, lambda_service=None):
        self.config = config
        self.db_service = db_service or DatabaseService(config)
        self.lambda_service = lambda_service or LambdaService(config)
    
    def process(self, event):
        # All business logic here
        pass

def lambda_handler(event, context):
    config = Config.from_env()
    processor = TimesheetProcessor(config)
    return processor.process(event)
```

### Option 2: Function-Based with Dependency Injection

Extract business logic into pure functions that accept dependencies:

```python
# main.py structure
def process_timesheet_webhook(event, config, db_service, lambda_service):
    # All business logic here
    pass

def lambda_handler(event, context):
    config = Config.from_env()
    db_service = DatabaseService(config)
    lambda_service = LambdaService(config)
    return process_timesheet_webhook(event, config, db_service, lambda_service)
```

### Option 3: Hybrid Approach (Best Balance)

Combine service classes for complex flows with dependency injection:

```python
# main.py structure
class Config:
    @classmethod
    def from_env(cls):
        return cls(
            endpoint=os.environ.get('ENDPOINT'),
            db_user=os.environ.get('DB_USER'),
            # ... etc
        )

class TimesheetProcessor:
    def __init__(self, config, db_service=None, lambda_service=None):
        self.config = config
        self.db_service = db_service
        self.lambda_service = lambda_service
    
    def process(self, event):
        # Main orchestration logic
        pass

def lambda_handler(event, context):
    config = Config.from_env()
    processor = TimesheetProcessor(config)
    return processor.process(event)
```

## Key Refactoring Steps

### 1. Extract Configuration
- Create a `Config` class to hold all environment variables
- Make it easy to create test configs

### 2. Create Service Classes
- `DatabaseService`: Wraps all DB operations
- `LambdaService`: Wraps Lambda invocations
- `TimesheetProcessor`: Orchestrates the main flow

### 3. Separate Concerns
- **Handler**: Thin wrapper that creates dependencies and calls processor
- **Processor**: Orchestrates business logic flow
- **Services**: Handle external interactions (DB, Lambda)
- **Utilities**: Pure functions for transformations

### 4. Enable Dependency Injection
- Pass services as constructor parameters
- Use default parameters for production, allow override in tests

## Testing Benefits

With this structure, you can:
- **Unit test** the `TimesheetProcessor.process()` method with mocked services
- **Test individual steps** by testing processor methods in isolation
- **Mock external dependencies** easily (DB, Lambda)
- **Test configuration** separately from business logic
- **Integration test** with real services by creating them normally

## Example Test Structure

```python
def test_process_timesheet_webhook():
    # Arrange
    mock_db = Mock()
    mock_lambda = Mock()
    config = Config.test_config()
    processor = TimesheetProcessor(config, mock_db, mock_lambda)
    
    # Act
    result = processor.process(test_event)
    
    # Assert
    assert result['statusCode'] == 200
    mock_db.insert_timesheet.assert_called_once()
```

## Migration Path

1. Start by extracting configuration into a `Config` class
2. Create service wrappers for external dependencies
3. Move business logic from `lambda_handler` into a `TimesheetProcessor` class
4. Update `lambda_handler` to use the new structure
5. Add tests incrementally as you refactor
