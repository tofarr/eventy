# Eventy Queue Examples

This directory contains example scripts demonstrating how to use the eventy queue system with custom dataclass payloads.

## Examples

### 1. Basic Example (`eventy_example.py`)

A simple example that demonstrates the core functionality:

- **MyMessage dataclass**: Custom message type with a `msg` string attribute
- **Queue Manager**: Memory-based queue manager for managing event queues
- **Event Queue**: Retrieves event queue for MyMessage payloads from the manager
- **Subscriber**: Simple subscriber that prints received events to console
- **Publisher**: Publishes a new message with current timestamp every second

**Features demonstrated:**
- Creating a custom dataclass for event payloads
- Using a queue manager to register and manage event queues
- Subscribing to events with a custom subscriber
- Publishing events at regular intervals
- Graceful shutdown and cleanup with proper deregistration

**Usage:**
```bash
cd /workspace/project/eventy/examples
PYTHONPATH=/workspace/project/eventy python eventy_example.py
```

### 2. Advanced Example (`eventy_advanced_example.py`)

A more comprehensive example showcasing advanced features:

- **Multiple Subscribers**: Different types of subscribers with various behaviors
- **Event Filtering**: Subscriber that only processes events with specific keywords
- **Event Logging**: Subscriber that maintains a log of all events
- **Statistics**: Queue statistics and subscriber metrics
- **Signal Handling**: Proper shutdown handling with SIGINT/SIGTERM

**Features demonstrated:**
- Multiple subscriber types working simultaneously
- Event filtering based on message content
- Event logging and statistics collection
- Graceful shutdown with signal handling
- Queue manager usage and proper cleanup

**Usage:**
```bash
cd /workspace/project/eventy/examples
PYTHONPATH=/workspace/project/eventy python eventy_advanced_example.py
```

### 3. Webhook Example (`webhook_example.py`)

An existing example demonstrating webhook subscriber functionality with HTTP endpoints.

**Usage:**
```bash
cd /workspace/project/eventy/examples
PYTHONPATH=/workspace/project/eventy python webhook_example.py
```

## Key Components

### MyMessage Dataclass
```python
@dataclass
class MyMessage:
    """Custom message dataclass with a string msg attribute."""
    msg: str
```

### Custom Subscriber
```python
class PrintSubscriber(Subscriber[MyMessage]):
    """Simple subscriber that prints received events to the console."""
    
    payload_type = MyMessage
    
    async def on_event(self, event: QueueEvent[MyMessage]) -> None:
        """Print the received event details."""
        print(f"📨 Received event #{event.id}")
        print(f"   Message: {event.payload.msg}")
```

### Queue Manager Setup
```python
# Create a memory queue manager and register message type
queue_manager = MemoryQueueManager()
await queue_manager.register(MyMessage)

# Get the event queue for MyMessage payloads
queue = await queue_manager.get_event_queue(MyMessage)

# Subscribe to the queue
subscriber = PrintSubscriber()
subscriber_id = await queue.subscribe(subscriber)

# Publish events
message = MyMessage(msg="Hello from eventy!")
event = await queue.publish(message)

# Clean up
await queue.unsubscribe(subscriber_id)
await queue_manager.deregister(MyMessage)
```

## Requirements

- Python 3.12+
- eventy library (included in this repository)
- asyncio support

## Running the Examples

1. Make sure you're in the project directory
2. Set the PYTHONPATH to include the eventy library
3. Run the desired example script

The examples will run continuously, publishing messages every second until interrupted with Ctrl+C.

## Output

Both examples provide rich console output showing:
- Event publishing notifications
- Event reception by subscribers
- Event details (ID, timestamp, status, message content)
- Statistics and cleanup information

The advanced example also shows:
- Multiple subscriber behaviors
- Event filtering in action
- Logging statistics
- Graceful shutdown procedures