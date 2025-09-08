#!/usr/bin/env python3
"""
Advanced example script demonstrating eventy queue usage with multiple subscribers.

This script demonstrates:
1. Custom MyMessage dataclass with a string msg attribute
2. Multiple subscribers with different behaviors
3. Event filtering and statistics
4. Graceful shutdown handling
5. Queue event inspection
"""
import asyncio
import signal
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from eventy.mem.memory_queue_manager import MemoryQueueManager
from eventy.subscriber.subscriber import Subscriber
from eventy.queue_event import QueueEvent
from eventy.event_status import EventStatus


@dataclass
class MyMessage:
    """Custom message dataclass with a string msg attribute."""
    msg: str


class PrintSubscriber(Subscriber[MyMessage]):
    """Simple subscriber that prints received events to the console."""
    
    payload_type = MyMessage
    
    def __init__(self, name: str = "PrintSubscriber"):
        self.name = name
    
    async def on_event(self, event: QueueEvent[MyMessage]) -> None:
        """Print the received event details."""
        print(f"📨 [{self.name}] Event #{event.id} at {event.created_at.strftime('%H:%M:%S')}")
        print(f"   Status: {event.status.name}")
        print(f"   Message: {event.payload.msg}")
        print()


class LoggingSubscriber(Subscriber[MyMessage]):
    """Subscriber that logs events to a list for later analysis."""
    
    payload_type = MyMessage
    
    def __init__(self, name: str = "LoggingSubscriber"):
        self.name = name
        self.logged_events = []
    
    async def on_event(self, event: QueueEvent[MyMessage]) -> None:
        """Log the event for later analysis."""
        self.logged_events.append({
            'id': event.id,
            'timestamp': event.created_at,
            'status': event.status,
            'message': event.payload.msg
        })
        print(f"📝 [{self.name}] Logged event #{event.id} (Total logged: {len(self.logged_events)})")


class FilteringSubscriber(Subscriber[MyMessage]):
    """Subscriber that only processes events containing specific keywords."""
    
    payload_type = MyMessage
    
    def __init__(self, keywords: list[str], name: str = "FilteringSubscriber"):
        self.keywords = keywords
        self.name = name
        self.processed_count = 0
    
    async def on_event(self, event: QueueEvent[MyMessage]) -> None:
        """Process only events containing specified keywords."""
        if any(keyword.lower() in event.payload.msg.lower() for keyword in self.keywords):
            self.processed_count += 1
            print(f"🔍 [{self.name}] Filtered event #{event.id} - Contains keywords: {self.keywords}")
            print(f"   Message: {event.payload.msg}")
            print(f"   Processed count: {self.processed_count}")
            print()


class EventQueueManager:
    """Manager class to handle the event queue and subscribers."""
    
    def __init__(self):
        self.queue_manager = MemoryQueueManager()
        self.queue = None
        self.subscribers = {}
        self.running = False
        self.message_count = 0
    
    async def initialize(self):
        """Initialize the queue manager and register the message type."""
        await self.queue_manager.register(MyMessage)
        self.queue = await self.queue_manager.get_event_queue(MyMessage)
    
    async def add_subscriber(self, subscriber: Subscriber[MyMessage], name: str) -> str:
        """Add a subscriber to the queue."""
        subscriber_id = await self.queue.subscribe(subscriber)
        self.subscribers[name] = {
            'id': subscriber_id,
            'subscriber': subscriber
        }
        print(f"✅ Added subscriber '{name}' with ID: {subscriber_id}")
        return subscriber_id
    
    async def remove_subscriber(self, name: str) -> bool:
        """Remove a subscriber from the queue."""
        if name in self.subscribers:
            subscriber_id = self.subscribers[name]['id']
            success = await self.queue.unsubscribe(subscriber_id)
            if success:
                del self.subscribers[name]
                print(f"✅ Removed subscriber '{name}'")
            return success
        return False
    
    async def publish_message(self, message: str) -> QueueEvent[MyMessage]:
        """Publish a message to the queue."""
        self.message_count += 1
        msg = MyMessage(msg=message)
        event = await self.queue.publish(msg)
        return event
    
    async def show_statistics(self):
        """Display queue statistics."""
        total_events = await self.queue.count_events()
        processing_events = await self.queue.count_events(status__eq=EventStatus.PROCESSING)
        error_events = await self.queue.count_events(status__eq=EventStatus.ERROR)
        
        print("\n📊 Queue Statistics:")
        print(f"   Total events: {total_events}")
        print(f"   Processing events: {processing_events}")
        print(f"   Error events: {error_events}")
        print(f"   Active subscribers: {len(self.subscribers)}")
        
        # Show subscriber-specific stats
        for name, sub_info in self.subscribers.items():
            subscriber = sub_info['subscriber']
            if hasattr(subscriber, 'logged_events'):
                print(f"   {name}: {len(subscriber.logged_events)} events logged")
            elif hasattr(subscriber, 'processed_count'):
                print(f"   {name}: {subscriber.processed_count} events processed")
    
    async def start_publishing(self, interval: float = 1.0):
        """Start publishing messages at regular intervals."""
        self.running = True
        message_types = [
            "Hello from eventy!",
            "System status update",
            "Important notification",
            "Debug message",
            "Error report",
            "Performance metrics"
        ]
        
        try:
            while self.running:
                # Rotate through different message types
                base_message = message_types[self.message_count % len(message_types)]
                current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                message = f"{base_message} Message #{self.message_count + 1} at {current_time}"
                
                event = await self.publish_message(message)
                print(f"📤 Published message #{self.message_count} (Event ID: {event.id})")
                
                await asyncio.sleep(interval)
                
        except asyncio.CancelledError:
            print("\n🛑 Publishing stopped")
            self.running = False
    
    async def cleanup(self):
        """Clean up all subscribers."""
        print("\n🧹 Cleaning up subscribers...")
        for name in list(self.subscribers.keys()):
            await self.remove_subscriber(name)
        await self.show_statistics()
        
        # Deregister the message type from the queue manager
        await self.queue_manager.deregister(MyMessage)
        print("✅ Deregistered MyMessage from the queue manager")


async def main():
    """Main function demonstrating advanced eventy queue usage."""
    print("🚀 Advanced Eventy Queue Example with MyMessage")
    print("=" * 60)
    print()
    
    # Create and initialize the queue manager
    manager = EventQueueManager()
    await manager.initialize()
    print(f"✅ Created event queue with worker ID: {manager.queue.worker_id}")
    print()
    
    # Add different types of subscribers
    print_sub = PrintSubscriber("Console Printer")
    await manager.add_subscriber(print_sub, "printer")
    
    logger_sub = LoggingSubscriber("Event Logger")
    await manager.add_subscriber(logger_sub, "logger")
    
    filter_sub = FilteringSubscriber(["important", "error"], "Important Filter")
    await manager.add_subscriber(filter_sub, "filter")
    
    print()
    
    # Set up graceful shutdown
    shutdown_event = asyncio.Event()
    
    def signal_handler():
        print("\n🛑 Received shutdown signal...")
        shutdown_event.set()
    
    # Register signal handlers
    for sig in [signal.SIGINT, signal.SIGTERM]:
        signal.signal(sig, lambda s, f: signal_handler())
    
    try:
        # Start publishing in the background
        publish_task = asyncio.create_task(manager.start_publishing(1.0))
        
        # Wait for shutdown signal
        await shutdown_event.wait()
        
        # Cancel publishing
        publish_task.cancel()
        try:
            await publish_task
        except asyncio.CancelledError:
            pass
        
    finally:
        # Clean up
        await manager.cleanup()
        print("\n👋 Advanced example completed!")


if __name__ == "__main__":
    asyncio.run(main())