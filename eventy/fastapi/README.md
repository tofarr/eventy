# Eventy FastAPI Integration

This module provides FastAPI integration for the eventy event queue system.

## Files

- `app.py` - Main FastAPI application with queue manager lifecycle management
- `fastapi.py` - Utility functions for adding eventy endpoints to FastAPI apps
- `websocket_subscriber.py` - WebSocket subscriber implementation

## Usage

### Running the App

The simplest way to run the FastAPI app is using the eventy module directly:

```bash
# Run with default settings (host: 0.0.0.0, port: 8000)
python -m eventy

# Run with custom host and port
python -m eventy --host localhost --port 8080

# Run with auto-reload for development
python -m eventy --reload
```

### Using Uvicorn Directly

You can also run the app directly with uvicorn:

```bash
uvicorn eventy.fastapi.app:app --host 0.0.0.0 --port 8000 --reload
```

### Endpoints

The app provides the following endpoints:

- `GET /` - Hello world endpoint with queue manager status
- `GET /health` - Health check endpoint
- `GET /docs` - Interactive API documentation (Swagger UI)
- `GET /redoc` - Alternative API documentation

### Queue Manager Integration

The app automatically:

1. Initializes the default queue manager on startup
2. Properly enters the queue manager context
3. Exits the queue manager context on shutdown

This ensures proper resource management and cleanup of the eventy queue system.

### Example Response

```json
{
    "message": "Hello, World!",
    "status": "Queue manager is running", 
    "queue_manager_type": "MemoryQueueManager"
}
```

The `queue_manager_type` will vary depending on your eventy configuration:
- `MemoryQueueManager` - In-memory queue (default)
- `FileEventQueueManager` - File-based queue (when `EVENTY_ROOT_DIR` is set)
- Custom queue manager (when `EVENTY_QUEUE_MANAGER` is set)