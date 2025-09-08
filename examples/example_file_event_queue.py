#!/usr/bin/env python3
"""
Example usage of the file-based event queue implementations.

This example demonstrates how to use both the polling and watchdog-based
file event queues to publish and subscribe to events.
"""

import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path
import tempfile
import shutil
from uuid import UUID

# Add parent directory to path so we can import eventy
sys.path.insert(0, str(Path(__file__).parent.parent))

from eventy.fs.polling_file_event_queue import PollingFileEventQueue
from eventy.fs.watchdog_file_event_queue import WatchdogFileEventQueue
from eventy.subscriber.subscriber import Subscriber
from eventy.queue_event import QueueEvent


@dataclass
class OrderEvent:
    """Example event payload for an e-commerce order"""
    order_id: str
    customer_id: str
    total_amount: float
    items: list


class EmailNotificationSubscriber(Subscriber[OrderEvent]):
    """Subscriber that sends email notifications for orders"""
    
    payload_type = OrderEvent
    
    async def on_event(self, event: QueueEvent[OrderEvent], current_worker_id: UUID) -> None:
        """Process order event by sending email notification"""
        order = event.payload
        print(f"📧 Sending email notification for order {order.order_id}")
        print(f"   Customer: {order.customer_id}")
        print(f"   Total: ${order.total_amount:.2f}")
        print(f"   Items: {len(order.items)} items")


class InventoryUpdateSubscriber(Subscriber[OrderEvent]):
    """Subscriber that updates inventory when orders are placed"""
    
    payload_type = OrderEvent
    
    async def on_event(self, event: QueueEvent[OrderEvent], current_worker_id: UUID) -> None:
        """Process order event by updating inventory"""
        order = event.payload
        print(f"📦 Updating inventory for order {order.order_id}")
        for item in order.items:
            print(f"   - Reducing stock for: {item}")


async def demonstrate_polling_queue():
    """Demonstrate the polling-based file event queue"""
    print("\n" + "="*60)
    print("POLLING FILE EVENT QUEUE DEMONSTRATION")
    print("="*60)
    
    # Create temporary directory
    temp_dir = Path(tempfile.mkdtemp())
    print(f"Using directory: {temp_dir}")
    
    try:
        # Create polling-based queue
        queue = PollingFileEventQueue(
            root_dir=temp_dir,
            payload_type=OrderEvent,
            polling_interval=0.5  # Poll every 500ms for faster demo
        )
        
        async with queue:
            print("✅ Polling queue started")
            
            # Create subscribers
            email_subscriber = EmailNotificationSubscriber()
            inventory_subscriber = InventoryUpdateSubscriber()
            
            # Subscribe to events
            email_subscription = await queue.subscribe(email_subscriber)
            inventory_subscription = await queue.subscribe(inventory_subscriber)
            
            print(f"✅ Subscribed email notifications: {email_subscription.id}")
            print(f"✅ Subscribed inventory updates: {inventory_subscription.id}")
            
            # Publish some orders
            orders = [
                OrderEvent("ORD-001", "customer@example.com", 99.99, ["Widget A", "Widget B"]),
                OrderEvent("ORD-002", "buyer@test.com", 149.50, ["Gadget X", "Gadget Y", "Gadget Z"]),
                OrderEvent("ORD-003", "user@demo.org", 29.99, ["Tool Alpha"])
            ]
            
            print("\n📤 Publishing orders...")
            for order in orders:
                event = await queue.publish(order)
                print(f"Published order {order.order_id} as event {event.id}")
            
            # Wait for processing
            print("\n⏳ Waiting for event processing...")
            await asyncio.sleep(3.0)
            
            print("✅ Polling queue demonstration completed")
            
    finally:
        shutil.rmtree(temp_dir)
        print(f"🧹 Cleaned up: {temp_dir}")


async def demonstrate_watchdog_queue():
    """Demonstrate the watchdog-based file event queue"""
    print("\n" + "="*60)
    print("WATCHDOG FILE EVENT QUEUE DEMONSTRATION")
    print("="*60)
    
    # Create temporary directory
    temp_dir = Path(tempfile.mkdtemp())
    print(f"Using directory: {temp_dir}")
    
    try:
        # Create watchdog-based queue (falls back to polling if watchdog not available)
        queue = WatchdogFileEventQueue(
            root_dir=temp_dir,
            payload_type=OrderEvent,
            polling_interval=0.5  # Fallback polling interval
        )
        
        async with queue:
            print("✅ Watchdog queue started")
            
            # Create subscribers
            email_subscriber = EmailNotificationSubscriber()
            inventory_subscriber = InventoryUpdateSubscriber()
            
            # Subscribe to events
            email_subscription = await queue.subscribe(email_subscriber)
            inventory_subscription = await queue.subscribe(inventory_subscriber)
            
            print(f"✅ Subscribed email notifications: {email_subscription.id}")
            print(f"✅ Subscribed inventory updates: {inventory_subscription.id}")
            
            # Publish some orders
            orders = [
                OrderEvent("ORD-101", "premium@example.com", 299.99, ["Premium Widget"]),
                OrderEvent("ORD-102", "bulk@business.com", 999.00, ["Bulk Item A", "Bulk Item B"])
            ]
            
            print("\n📤 Publishing orders...")
            for order in orders:
                event = await queue.publish(order)
                print(f"Published order {order.order_id} as event {event.id}")
            
            # Wait for processing
            print("\n⏳ Waiting for event processing...")
            await asyncio.sleep(3.0)
            
            print("✅ Watchdog queue demonstration completed")
            
    finally:
        shutil.rmtree(temp_dir)
        print(f"🧹 Cleaned up: {temp_dir}")


async def main():
    """Run both demonstrations"""
    print("FILE-BASED EVENT QUEUE EXAMPLES")
    print("This example shows how to use file-based event queues for")
    print("distributed event processing with persistent storage.")
    
    await demonstrate_polling_queue()
    await demonstrate_watchdog_queue()
    
    print("\n" + "="*60)
    print("🎉 All demonstrations completed successfully!")
    print("="*60)


if __name__ == "__main__":
    asyncio.run(main())