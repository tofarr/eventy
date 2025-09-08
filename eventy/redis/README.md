# Redis-based Eventy Implementations

This module provides Redis-backed implementations of EventQueue and QueueManager for distributed event processing.

## Features

- **Distributed Processing**: Multiple workers can share the same Redis backend
- **Pub/Sub Notifications**: Real-time event notifications using Redis pub/sub
- **Automatic Cleanup**: Events expire automatically using Redis TTL
- **Persistent Storage**: Events survive application restarts
- **Scalable**: Handles high-throughput event processing

## Installation

Install the Redis extra dependency:

```bash
pip install eventy[redis]
```

Or install Redis directly:

```bash
pip install redis>=5.0.0
```

## Requirements

- Redis server (version 5.0 or higher recommended)
- Python 3.12+

## Usage

### Basic Event Queue

```python
import asyncio
from dataclasses import dataclass
from datetime import datetime, UTC
from eventy.redis import RedisEventQueue

@dataclass
class OrderEvent:
    order_id: str
    amount: float
    timestamp: datetime

async def main():
    # Create Redis event queue
    queue = RedisEventQueue[OrderEvent](
        event_type=OrderEvent,
        key_prefix="myapp",
        event_ttl=3600  # 1 hour TTL
    )
    
    async with queue:
        # Publish an event
        event = await queue.publish(OrderEvent(
            order_id="ORD-123",
            amount=99.99,
            timestamp=datetime.now(UTC)
        ))
        
        print(f"Published event {event.id}")
        
        # Retrieve the event
        retrieved = await queue.get_event(event.id)
        print(f"Retrieved: {retrieved.payload.order_id}")

asyncio.run(main())
```

### Queue Manager

```python
import asyncio
from eventy.redis import RedisQueueManager

async def main():
    manager = RedisQueueManager(
        key_prefix="myapp",
        event_ttl=3600
    )
    
    async with manager:
        # Register event types
        await manager.register(OrderEvent)
        
        # Get queue for specific type
        queue = await manager.get_event_queue(OrderEvent)
        
        # Use the queue...
        event = await queue.publish(OrderEvent(...))

asyncio.run(main())
```

### Custom Redis Configuration

```python
import redis.asyncio as redis
from eventy.redis import RedisEventQueue

# Custom Redis client
redis_client = redis.Redis(
    host='redis.example.com',
    port=6380,
    password='secret',
    db=1
)

queue = RedisEventQueue[OrderEvent](
    event_type=OrderEvent,
    redis_client=redis_client,
    key_prefix="production",
    event_ttl=86400  # 24 hours
)
```

## Configuration Options

### RedisEventQueue

- `event_type`: The Python type for events in this queue
- `redis_client`: Optional Redis client (creates default if not provided)
- `serializer`: Optional custom serializer (uses default if not provided)
- `key_prefix`: Redis key prefix (default: "eventy")
- `event_ttl`: Event time-to-live in seconds (default: 86400)

### RedisQueueManager

- `redis_client`: Optional Redis client (creates default if not provided)
- `serializer`: Optional custom serializer (uses default if not provided)
- `key_prefix`: Redis key prefix (default: "eventy")
- `event_ttl`: Event time-to-live in seconds (default: 86400)

## Redis Key Structure

The implementations use the following Redis key patterns:

- Events: `{prefix}:events:{type}:{id}`
- Subscribers: `{prefix}:subscribers:{type}:{uuid}`
- Event Counter: `{prefix}:counter:{type}`
- Event Index: `{prefix}:index:{type}` (sorted set)
- Pub/Sub Channel: `{prefix}:channel:{type}`
- Queue Registry: `{prefix}:queue_types`

## Distributed Processing

Multiple application instances can share the same Redis backend:

```python
# Worker 1 - Publisher
queue1 = RedisEventQueue[OrderEvent](
    event_type=OrderEvent,
    key_prefix="shared"
)

# Worker 2 - Subscriber  
queue2 = RedisEventQueue[OrderEvent](
    event_type=OrderEvent,
    key_prefix="shared"
)

# Both workers see the same events
async with queue1, queue2:
    await queue2.subscribe(my_subscriber)
    await queue1.publish(OrderEvent(...))  # Worker 2 receives notification
```

## Error Handling

The implementations handle various error conditions:

- Redis connection failures
- Serialization errors
- Expired events
- Network timeouts

Events that fail to process are marked with `EventStatus.ERROR`.

## Performance Considerations

- Use appropriate `event_ttl` values to balance storage and data retention
- Consider Redis memory limits when setting TTL values
- Use connection pooling for high-throughput scenarios
- Monitor Redis memory usage and configure appropriate eviction policies

## Examples

See `examples/redis_example.py` for comprehensive usage examples including:

- Basic event publishing and subscribing
- Queue manager usage
- Distributed processing scenarios
- Error handling patterns