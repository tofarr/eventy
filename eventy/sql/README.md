# SQL Event Queue Implementation

This module provides SQL-based implementations of EventQueue and QueueManager using SQLAlchemy for database operations.

## Installation

Install the SQL dependencies:

```bash
pip install eventy[sql]
```

This will install SQLAlchemy and aiosqlite as optional dependencies.

## Quick Start

### Basic Usage

```python
import asyncio
from eventy.sql import SqlEventQueue, SqlQueueManager
from dataclasses import dataclass

@dataclass
class MyPayload:
    message: str
    value: int

async def main():
    # Using SqlEventQueue directly
    queue = SqlEventQueue(
        database_url="sqlite:///./eventy.db",
        payload_type=MyPayload
    )
    
    async with queue:
        # Publish an event
        event = await queue.publish(MyPayload(message="Hello", value=42))
        print(f"Published event {event.id}")
        
        # Retrieve the event
        retrieved = await queue.get_event(event.id)
        print(f"Retrieved: {retrieved.payload.message}")

asyncio.run(main())
```

### Using SqlQueueManager

```python
import asyncio
from eventy.sql import SqlQueueManager
from dataclasses import dataclass

@dataclass
class TaskPayload:
    task_name: str
    priority: int

async def main():
    manager = SqlQueueManager(database_url="sqlite:///./eventy.db")
    
    async with manager:
        # Register a payload type
        await manager.register(TaskPayload)
        
        # Get the queue for this payload type
        queue = await manager.get_event_queue(TaskPayload)
        
        # Use the queue
        event = await queue.publish(TaskPayload(task_name="process_data", priority=1))
        print(f"Published task event {event.id}")

asyncio.run(main())
```

## Database Support

The SQL implementation supports any database that SQLAlchemy supports:

- **SQLite** - Great for development and testing
- **PostgreSQL** - Recommended for production
- **MySQL/MariaDB** - Also supported
- **SQL Server** - Supported via pyodbc

### Database URLs

```python
# SQLite (file-based)
database_url = "sqlite:///./eventy.db"

# SQLite (in-memory)
database_url = "sqlite:///:memory:"

# PostgreSQL
database_url = "postgresql://user:password@localhost/eventy"

# MySQL
database_url = "mysql+pymysql://user:password@localhost/eventy"

# SQL Server
database_url = "mssql+pyodbc://user:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"
```

## Database Schema

The SQL implementation creates four main tables:

### eventy_events
Stores published events with their serialized payloads.

| Column | Type | Description |
|--------|------|-------------|
| id | Integer (PK) | Auto-incrementing event ID |
| payload_type | String(255) | Fully qualified payload type name |
| payload_data | LargeBinary | Serialized event data |
| created_at | DateTime | Event creation timestamp |

### eventy_results
Stores event processing results.

| Column | Type | Description |
|--------|------|-------------|
| id | UUID (PK) | Unique result ID |
| worker_id | UUID | ID of the worker that processed the event |
| event_id | Integer | Reference to the event |
| success | Boolean | Whether processing succeeded |
| details | Text | Optional error details or additional info |
| created_at | DateTime | Result creation timestamp |

### eventy_subscribers
Stores event subscribers.

| Column | Type | Description |
|--------|------|-------------|
| id | UUID (PK) | Unique subscriber ID |
| payload_type | String(255) | Payload type this subscriber handles |
| subscriber_data | LargeBinary | Serialized subscriber data |
| created_at | DateTime | Subscription creation timestamp |

### eventy_claims
Stores worker claims for distributed processing.

| Column | Type | Description |
|--------|------|-------------|
| id | String(255) (PK) | Claim identifier |
| worker_id | UUID | ID of the worker making the claim |
| payload_type | String(255) | Payload type for this claim |
| data | Text | Optional claim data |
| created_at | DateTime | Claim creation timestamp |

## Table Creation

The SQL implementation automatically creates database tables when the SqlQueueManager is entered. This behavior can be controlled via environment variable or constructor parameter.

### Environment Variables

- `EVENTY_SQL_CREATE_TABLES` - Set to "false" to disable automatic table creation (default: "true")

```bash
# Disable automatic table creation
export EVENTY_SQL_CREATE_TABLES="false"
```

### Manual Table Creation Control

```python
# Disable table creation via constructor
manager = SqlQueueManager(
    database_url="postgresql://user:password@localhost/eventy",
    create_tables=False
)

# Or enable it explicitly
manager = SqlQueueManager(
    database_url="postgresql://user:password@localhost/eventy", 
    create_tables=True
)
```

## Configuration

### Environment Variables

The SQL implementation supports the following environment variables (defined in `eventy.constants`):

- `EVENTY_DATABASE_URL` - Database connection URL (default: `sqlite:///./eventy.db`)
- `EVENTY_SQL_CREATE_TABLES` - Set to "false" to disable automatic table creation (default: "true")
- `EVENTY_QUEUE_MANAGER` - Set to `eventy.sql.SqlQueueManager` to use SQL as default

These constants should be imported from `eventy.constants` for consistency:

```python
from eventy.constants import EVENTY_DATABASE_URL, EVENTY_SQL_CREATE_TABLES, EVENTY_QUEUE_MANAGER
import os

# Set environment variables using constants
os.environ[EVENTY_DATABASE_URL] = "postgresql://user:pass@localhost/eventy"
os.environ[EVENTY_SQL_CREATE_TABLES] = "false"
os.environ[EVENTY_QUEUE_MANAGER] = "eventy.sql.sql_queue_manager.SqlQueueManager"
```

### Integration with Default Queue Manager

```python
import os
from eventy.constants import EVENTY_DATABASE_URL, EVENTY_QUEUE_MANAGER
from eventy.queue_manager import get_default_queue_manager

# Set environment variable to use SQL queue manager
os.environ[EVENTY_QUEUE_MANAGER] = "eventy.sql.sql_queue_manager.SqlQueueManager"
os.environ[EVENTY_DATABASE_URL] = "postgresql://user:password@localhost/eventy"

# Now get_default_queue_manager() will return SqlQueueManager
manager = await get_default_queue_manager()
```

## Performance Considerations

### Indexing
The implementation creates indexes on frequently queried columns:
- `payload_type` columns for filtering by event type
- `worker_id` and `event_id` columns for result queries
- `created_at` columns for time-based queries

### Connection Pooling
SQLAlchemy handles connection pooling automatically. For high-throughput applications, consider tuning pool settings:

```python
from sqlalchemy import create_engine

engine = create_engine(
    database_url,
    pool_size=20,
    max_overflow=30,
    pool_pre_ping=True
)

queue = SqlEventQueue(
    database_url=database_url,
    payload_type=MyPayload
)
queue._engine = engine  # Override default engine
```

### Serialization
The implementation uses the configured serializer (default: pickle) for storing payloads and subscribers. For better performance and cross-language compatibility, consider using JSON serialization:

```python
from eventy.serializers import JsonSerializer

queue = SqlEventQueue(
    database_url=database_url,
    payload_type=MyPayload,
    event_serializer=JsonSerializer(),
    subscriber_serializer=JsonSerializer()
)
```

## Error Handling

The SQL implementation handles common database errors:

- **Connection errors** - Automatically retried by SQLAlchemy
- **Constraint violations** - Handled gracefully (e.g., duplicate claims)
- **Serialization errors** - Logged and skipped during iteration
- **Transaction errors** - Automatically rolled back

## Testing

Run the SQL-specific tests:

```bash
# Install test dependencies
pip install eventy[sql] pytest pytest-asyncio

# Run SQL tests
pytest tests/test_sql_event_queue.py -v
```

The tests use temporary SQLite databases and clean up automatically.

## Migration from File-based Queues

To migrate from file-based queues to SQL:

1. Install SQL dependencies: `pip install eventy[sql]`
2. Set up your database (tables will be created automatically)
3. Update your configuration to use `SqlQueueManager`
4. Optionally migrate existing data using custom scripts

The SQL implementation provides the same API as file-based queues, so no code changes are required beyond configuration.