# File-Based Event Queue Implementation

This document describes the new file-based event queue implementations that provide persistent, distributed event processing using the filesystem as storage.

## Overview

The file-based event queue system consists of:

1. **AbstractFileEventQueue** - Abstract base class implementing the EventQueue interface
2. **PollingFileEventQueue** - Concrete implementation using periodic polling
3. **WatchdogFileEventQueue** - Concrete implementation using filesystem monitoring (with polling fallback)

## Architecture

### Directory Structure

Each event queue creates the following directory structure:

```
root_dir/
├── events/          # Event files, one per event (named by event ID)
├── results/         # Result files, one per result (named by result ID)  
└── subscriptions/   # Subscriber files, one per subscriber (named by subscriber ID)
```

### Serialization

The implementation uses three separate serializers:

- **event_serializer**: Serializes QueueEvent objects for storage in the events directory
- **result_serializer**: Serializes EventResult objects for storage in the results directory  
- **subscriber_serializer**: Serializes Subscriber objects for storage in the subscriptions directory

By default, all serializers use the pickle serializer, but can be customized.

## Usage

### Basic Usage

```python
import asyncio
from pathlib import Path
from dataclasses import dataclass

from eventy.fs.polling_file_event_queue import PollingFileEventQueue
from eventy.subscriber.subscriber import Subscriber
from eventy.queue_event import QueueEvent

@dataclass
class MyEvent:
    message: str
    value: int

class MySubscriber(Subscriber[MyEvent]):
    payload_type = MyEvent
    
    async def on_event(self, event: QueueEvent[MyEvent]) -> None:
        print(f"Received: {event.payload.message}")

async def main():
    queue = PollingFileEventQueue(
        root_dir=Path("/tmp/my_queue"),
        payload_type=MyEvent
    )
    
    async with queue:
        # Subscribe to events
        subscriber = MySubscriber()
        subscription = await queue.subscribe(subscriber)
        
        # Publish an event
        event = await queue.publish(MyEvent("Hello", 42))
        
        # Wait for processing
        await asyncio.sleep(1.0)

asyncio.run(main())
```

### Polling vs Watchdog

**PollingFileEventQueue**:
- Uses periodic polling to check for new events
- Configurable polling interval (default: 1.0 seconds)
- Lower resource usage, higher latency
- Always available

**WatchdogFileEventQueue**:
- Uses filesystem monitoring for immediate event detection
- Falls back to polling if watchdog library is not available
- Higher resource usage, lower latency
- Requires `watchdog` library for optimal performance

## Features

### Implemented EventQueue Methods

The abstract base class implements all required EventQueue methods:

- ✅ `publish(payload)` - Write events to files
- ✅ `subscribe(subscriber)` - Write subscribers to files  
- ✅ `unsubscribe(subscriber_id)` - Remove subscriber files
- ✅ `get_subscriber(subscriber_id)` - Read subscriber from file
- ✅ `search_subscriptions(page_id, limit)` - Paginated subscriber listing
- ✅ `get_result(result_id)` - Read result from file
- ✅ `search_results(...)` - Paginated result listing with filtering
- ✅ `count_results(...)` - Count results matching criteria
- ✅ `get_worker_id()` - Return worker UUID
- ✅ `get_payload_type()` - Return payload type

### Running State Management

All API methods check that the queue is running and throw `RuntimeError` if not:

```python
queue = PollingFileEventQueue(...)

# This will raise RuntimeError
await queue.publish(event)  # ❌ Queue not started

async with queue:
    # This works
    await queue.publish(event)  # ✅ Queue is running
```

### Duplicate Event Prevention

The implementation prevents duplicate event processing by tracking processed event IDs in memory.

### Error Handling

- Invalid event files (non-numeric names) are skipped with warnings
- Serialization errors are logged but don't stop processing
- Subscriber errors are logged but don't affect other subscribers

## Configuration

### PollingFileEventQueue Parameters

```python
queue = PollingFileEventQueue(
    root_dir=Path("/path/to/queue"),           # Required: Storage directory
    payload_type=MyEventType,                  # Required: Event payload type
    polling_interval=1.0,                      # Optional: Seconds between polls
    event_serializer=my_event_serializer,      # Optional: Custom event serializer
    result_serializer=my_result_serializer,    # Optional: Custom result serializer  
    subscriber_serializer=my_sub_serializer    # Optional: Custom subscriber serializer
)
```

### WatchdogFileEventQueue Parameters

```python
queue = WatchdogFileEventQueue(
    root_dir=Path("/path/to/queue"),           # Required: Storage directory
    payload_type=MyEventType,                  # Required: Event payload type
    polling_interval=1.0,                      # Optional: Fallback polling interval
    event_serializer=my_event_serializer,      # Optional: Custom event serializer
    result_serializer=my_result_serializer,    # Optional: Custom result serializer
    subscriber_serializer=my_sub_serializer    # Optional: Custom subscriber serializer
)
```

## Limitations

1. **No built-in result storage**: The `publish()` method doesn't automatically store results. Results must be stored manually using `_store_result()`.

2. **Memory-based duplicate prevention**: Processed event tracking is in-memory only and resets when the queue restarts.

3. **No event ordering guarantees**: Events are processed in filesystem iteration order, which may not match creation order.

4. **No distributed coordination**: Multiple queue instances will process the same events independently.

## Example

See `example_file_event_queue.py` for a complete working example demonstrating both implementations with multiple subscribers.

## Dependencies

- **Required**: Standard library only for PollingFileEventQueue
- **Optional**: `watchdog` library for WatchdogFileEventQueue (falls back to polling if not available)