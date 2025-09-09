# EventQueue Test Suite

This directory contains comprehensive test cases for all EventQueue implementations in the eventy project.

## Test Structure

### Abstract Test Base
- **File**: `abstract_event_queue_base.py`
- **Class**: `AbstractEventQueueTestBase`
- **Purpose**: Contains comprehensive tests for all EventQueue methods
- **Note**: This file is intentionally named to avoid automatic test discovery by pytest

### Concrete Test Implementations

1. **MemoryEventQueue Tests**
   - **File**: `test_memory_event_queue.py`
   - **Class**: `TestMemoryEventQueue`
   - **Tests**: In-memory implementation with isolation and context manager tests

2. **PollingFileEventQueue Tests**
   - **File**: `test_polling_file_event_queue.py`
   - **Class**: `TestPollingFileEventQueue`
   - **Tests**: File-based implementation with polling, persistence, and directory isolation tests

3. **WatchdogFileEventQueue Tests**
   - **File**: `test_watchdog_file_event_queue.py`
   - **Class**: `TestWatchdogFileEventQueue`
   - **Tests**: File-based implementation with real-time file system monitoring tests

## Test Coverage

The abstract test base covers all exposed EventQueue methods:

### Context Manager
- `__aenter__()` and `__aexit__()`

### Worker Management
- `get_worker_id()`
- `get_payload_type()`

### Subscription Management
- `subscribe()`
- `unsubscribe()`
- `get_subscriber()`
- `batch_get_subscriptions()`
- `search_subscriptions()`
- `count_subscriptions()`

### Event Management
- `publish()`
- `get_event()`
- `batch_get_events()`
- `search_events()`
- `count_events()`

### Result Management
- `get_result()`
- `batch_get_results()`
- `search_results()`
- `count_results()`

### Claim Management
- `create_claim()`
- `get_claim()`
- `batch_get_claims()`
- `search_claims()`
- `count_claims()`

## Running Tests

```bash
# Run all tests
python -m pytest tests/

# Run tests for a specific implementation
python -m pytest tests/test_memory_event_queue.py

# Run with verbose output
python -m pytest tests/ -v

# Run a specific test
python -m pytest tests/test_memory_event_queue.py::TestMemoryEventQueue::test_get_worker_id
```

## Dependencies

The tests require:
- `pytest`
- `pytest-asyncio`
- `watchdog` (for file-based implementations)

Install with:
```bash
pip install pytest pytest-asyncio watchdog
```