# Redis-based Queue Manager Implementation

This document describes the Redis-based queue manager implementation that extends the AbstractFileEventQueue with distributed coordination features.

## Overview

The `RedisFileEventQueue` class provides distributed coordination capabilities for file-based event queues using Redis as a coordination layer. It maintains all the functionality of the base file-based queue while adding distributed features for multi-worker environments.

## Key Features

### 1. Redis Integration
- **Connection Management**: Automatic Redis connection setup and cleanup
- **Database Selection**: Configurable Redis database for isolation
- **Connection Pooling**: Uses Redis connection pooling for efficiency
- **Error Handling**: Graceful handling of Redis connection failures

### 2. Distributed Event ID Management
- **Centralized Counter**: Event IDs are stored in Redis for consistency across workers
- **Filesystem Regeneration**: Can regenerate event counter from existing filesystem events
- **Atomic Operations**: Uses Redis atomic operations for thread-safe ID generation

### 3. Claim Management with Expiration
- **Exclusive Claims**: Uses Redis `SET` with `NX` (not exists) for exclusive claims
- **Automatic Expiration**: Claims automatically expire after configurable timeout
- **Worker Identification**: Each worker has a unique ID for claim ownership tracking
- **Conflict Resolution**: Prevents duplicate claims across distributed workers

### 4. Redis Pub/Sub Coordination
- **Event Broadcasting**: Publishes events to Redis channels for real-time coordination
- **Subscriber Management**: Broadcasts subscriber additions/removals to other workers
- **Background Monitoring**: Continuous monitoring of Redis pub/sub messages
- **Message Types**: Supports various message types (events, subscriber changes, resync)

### 5. Resync Mechanism
- **Exclusive Locking**: Uses Redis locks to prevent concurrent resync operations
- **Full State Publishing**: Publishes complete queue state during resync
- **Conflict Prevention**: Ensures only one worker performs resync at a time
- **State Consistency**: Maintains consistency across all distributed workers

## Architecture

### Class Structure
```python
@dataclass
class RedisFileEventQueue(AbstractFileEventQueue[T]):
    redis_url: str = "redis://localhost:6379"
    redis_db: int = 0
    redis_prefix: str = "eventy"
    claim_expiration_seconds: int = 300
    resync_lock_timeout_seconds: int = 30
```

### Key Methods
- `create_claim()`: Create exclusive claims with Redis locks
- `get_claim()`: Retrieve claims from Redis or filesystem
- `_publish_to_redis()`: Publish coordination messages
- `_start_pubsub_monitor()`: Background Redis monitoring
- `_handle_redis_message()`: Process incoming Redis messages
- `_perform_resync()`: Execute resync with exclusive locking

### Redis Key Structure
- `{prefix}:{queue_name}:next_event_id` - Event counter
- `{prefix}:{queue_name}:claim:{claim_id}` - Claim data
- `{prefix}:{queue_name}:resync_lock` - Resync coordination lock
- `{prefix}:{queue_name}:events` - Pub/sub channel

## Configuration

### Redis Connection
```python
queue = RedisFileEventQueue(
    root_dir=Path("/path/to/queue"),
    payload_type=MyPayload,
    redis_url="redis://localhost:6379",
    redis_db=1,  # Use separate database
    redis_prefix="myapp",  # Custom prefix
    claim_expiration_seconds=600,  # 10 minute claims
)
```

### Optional Dependencies
Add to your requirements:
```
eventy[redis]  # Installs redis>=4.0.0
```

## Usage Examples

### Basic Usage
```python
import asyncio
from pathlib import Path
from eventy.redis import RedisFileEventQueue

async def main():
    queue = RedisFileEventQueue(
        root_dir=Path("./my_queue"),
        payload_type=dict,
        redis_url="redis://localhost:6379"
    )
    
    async with queue:
        # Publish events (distributed coordination)
        event = await queue.publish({"message": "Hello World"})
        
        # Create exclusive claims
        success = await queue.create_claim("task_1", "worker_data")
        if success:
            claim = await queue.get_claim("task_1")
            print(f"Claimed by worker: {claim.worker_id}")

asyncio.run(main())
```

### Multi-Worker Environment
```python
# Worker 1
async def worker_1():
    queue = RedisFileEventQueue(
        root_dir=Path("./shared_queue"),
        payload_type=dict,
        redis_url="redis://localhost:6379"
    )
    
    async with queue:
        # This worker will coordinate with others via Redis
        await queue.subscribe(MySubscriber())

# Worker 2  
async def worker_2():
    queue = RedisFileEventQueue(
        root_dir=Path("./shared_queue"),  # Same queue
        payload_type=dict,
        redis_url="redis://localhost:6379"
    )
    
    async with queue:
        # Automatic coordination with worker 1
        event = await queue.publish({"from": "worker_2"})
```

## Testing

The implementation includes comprehensive tests with mocked Redis operations:

```bash
# Run Redis queue tests
pytest tests/test_redis_file_event_queue.py -v

# Test with real Redis (requires Redis server)
python -c "
import asyncio
from eventy.redis import RedisFileEventQueue
# ... test code
"
```

## Error Handling

### Redis Connection Failures
- Graceful degradation when Redis is unavailable
- Clear error messages for connection issues
- Automatic cleanup of resources

### Claim Conflicts
- Returns `False` for duplicate claim attempts
- Automatic expiration prevents stuck claims
- Worker ID tracking for debugging

### Network Partitions
- Claims expire automatically during network issues
- Resync mechanism restores consistency
- Background monitoring detects disconnections

## Performance Considerations

### Redis Operations
- Uses pipelining where possible
- Atomic operations for consistency
- Connection pooling for efficiency

### Memory Usage
- Claims stored in Redis with expiration
- Event counter cached locally
- Pub/sub messages are lightweight

### Scalability
- Supports multiple workers per queue
- Redis handles coordination overhead
- Filesystem remains authoritative for events

## Limitations

1. **Redis Dependency**: Requires Redis server for distributed features
2. **Network Latency**: Redis operations add network overhead
3. **Claim Expiration**: Claims must be renewed for long-running tasks
4. **Memory Usage**: Redis stores coordination data in memory

## Future Enhancements

1. **Claim Renewal**: Automatic renewal for long-running claims
2. **Health Monitoring**: Redis connection health checks
3. **Metrics Collection**: Performance and coordination metrics
4. **Sharding Support**: Multiple Redis instances for scaling
5. **Compression**: Message compression for large payloads