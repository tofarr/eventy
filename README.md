# Eventy

Eventy is a distributed task processing framework designed for simplicity and flexibility. It works out of the box without any dependency on SQL or Redis, though these are optional for enhanced functionality. For simple use cases, Eventy can use memory for non-clustered deployments or the filesystem for local process clustering. It also contains an integrated FastAPI server if required.

## Features

- **Zero Dependencies**: Works with memory (single process) or filesystem (multi-process) storage by default
- **Optional Backends**: Support for Redis and SQL databases when needed
- **Integrated Web Server**: Built-in FastAPI server for web-based interfaces
- **Distributed Processing**: Designed for scalable, distributed task processing
- **Type-Safe**: Full Python type hints and generic support
- **Async/Await**: Built on modern Python async patterns

## Protocol Overview

Eventy uses a distributed protocol with several key components that work together to enable reliable task processing:

### Core Components

- **Managers**: Coordinate access to event queues and handle queue lifecycle management. Queue managers are typically global within an application and provide the entry point for accessing queues.

- **Queues**: Handle the storage and distribution of events. A queue typically exists outside the scope of a single process, enabling distributed processing across multiple workers and applications. Queues are add-only - items may be added but never updated or removed (except subscribers which may be deleted). This add-only design allows for easy synchronization across distributed systems.

- **Events**: Represent individual tasks or messages to be processed. Each event contains a payload (the actual data) and metadata like creation time and unique identifiers.

- **Results**: Track the outcome of event processing, including success/failure status and optional details. Results provide feedback on whether events were processed successfully.

- **Claims**: Represent a worker's claim on a specific event or resource. Claims prevent duplicate processing and enable coordination between distributed workers.

- **Subscribers**: Define the logic for processing events. Subscribers are stateless components that receive events and perform the actual work.

- **Workers**: Execute subscriber logic and manage the processing lifecycle. Each worker has a unique identifier that differentiates it from other workers in the system.

### Architecture

The protocol is designed around the concept that **queues typically exist outside the scope of a single process**. This enables:

- Multiple processes to share the same queue
- Horizontal scaling across different machines
- Fault tolerance through distributed processing
- Load balancing across available workers

Events flow through the system as follows:
1. Events are published to queues
2. Workers claim events to prevent duplicate processing
3. Subscribers process the claimed events
4. Results are recorded to track processing outcomes
5. The system can handle failures and retries as needed

## Installation

```bash
pip install eventy
```

### Optional Dependencies

For Redis support:
```bash
pip install eventy[redis]
```

For SQL database support:
```bash
pip install eventy[sql]
```

For FastAPI server features:
```bash
pip install eventy[server]
```

## Examples

The `examples/` directory contains several demonstrations of Eventy's capabilities:

- **`a_cli_example.py`**: Basic CLI example showing queue setup, subscription, and message publishing
- **`b_fastapi_example.py`**: FastAPI integration with WebSocket support for real-time messaging
- **`c_distributed_task_manager.py`**: Advanced example demonstrating distributed task processing

To run an example:

```bash
python examples/a_cli_example.py
```

Each example includes detailed comments explaining the setup and usage patterns.

## Quick Start

Here's a simple example to get you started:

```python
import asyncio
from dataclasses import dataclass
from eventy.queue_manager import get_default_queue_manager
from eventy.subscribers.subscriber import Subscriber
from eventy.queue_event import QueueEvent

@dataclass
class MyMessage:
    content: str

class MySubscriber(Subscriber[MyMessage]):
    async def on_event(self, event: QueueEvent[MyMessage], event_queue):
        print(f"Processing: {event.payload.content}")

async def main():
    async with get_default_queue_manager() as manager:
        queue = await manager.get_event_queue(MyMessage)
        
        # Subscribe to process events
        subscription = await queue.subscribe(MySubscriber())
        
        # Publish an event
        await queue.publish(MyMessage(content="Hello, Eventy!"))
        
        # Let it process
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.