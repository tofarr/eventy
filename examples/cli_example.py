#!/usr/bin/env python3
"""
Example script demonstrating eventy queue usage with a custom dataclass.

This script:
1. Creates a MyMessage dataclass with a string msg attribute
2. Sets up an event queue for MyMessage payloads using the default queue manager
3. Subscribes to the queue to print received events
4. Publishes a new message with the current time every second
"""
import asyncio
from dataclasses import dataclass
from datetime import datetime

from eventy.event_queue import EventQueue
from eventy.queue_manager import get_default_queue_manager
from eventy.subscribers.subscriber import Subscriber
from eventy.queue_event import QueueEvent


@dataclass
class MyMessage:
    """Custom message dataclass with a string msg attribute."""
    msg: str


@dataclass
class PrintSubscriber(Subscriber[MyMessage]):
    """Simple subscriber that prints received events to the console."""
    
    @property
    def payload_type(self):
        return MyMessage
    
    async def on_event(self, event: QueueEvent[MyMessage], event_queue: EventQueue[MyMessage]) -> None:
        """Print the received event details."""
        print(f"📨 Received event #{event.id} at {event.created_at.strftime('%H:%M:%S')}")
        print(f"   Message: {event.payload.msg}")
        print()


async def main():
    """Main function demonstrating the eventy queue usage."""
    print("🚀 Eventy Queue Example with MyMessage")
    print("=" * 50)
    print()
    
    # Create a queue manager and register our message type
    queue_manager = get_default_queue_manager()
    async with queue_manager:
        await queue_manager.register(MyMessage)
        print(f"✅ Registered MyMessage with the {queue_manager.__class__.__name__}")
        
        # Get the event queue for MyMessage payloads
        queue = await queue_manager.get_event_queue(MyMessage)
        async with queue:

            print(f"✅ Retrieved event queue with worker ID: {queue.get_worker_id()}")
            
            # Create and subscribe a print subscriber
            subscriber = PrintSubscriber()
            subscription = await queue.subscribe(subscriber, check_subscriber_unique=True)
            print(f"✅ Subscribed PrintSubscriber with ID: {subscription.id}")
            print()
            
            # Counter for message numbering
            message_count = 0
            
            try:
                # Publish messages every second
                while True:
                    message_count += 1
                    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Create and publish a new message
                    message = MyMessage(msg=f"Hello from eventy! Message #{message_count} at {current_time}")
                    event = await queue.publish(message)
                    
                    print(f"📤 Published message #{message_count} (Event ID: {event.id})")
                    
                    # Wait for 1 second before publishing the next message
                    await asyncio.sleep(1)
                    
            except asyncio.CancelledError:
                print("\n🛑 Stopping the example...")
                
                # Unsubscribe the subscriber
                success = await queue.unsubscribe(subscription.id)
                print(f"✅ Unsubscribed PrintSubscriber: {success}")
                
                # Show final statistics
                event_count = await queue.count_events()
                print(f"📊 Total events published: {event_count}")
                
                # Deregister the message type from the queue manager
                await queue_manager.deregister(MyMessage)
                print("✅ Deregistered MyMessage from the queue manager")
                
                print("\n👋 Example completed!")


if __name__ == "__main__":
    asyncio.run(main())