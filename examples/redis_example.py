"""Example usage of Redis-based eventy implementations.

This example demonstrates how to use RedisEventQueue and RedisQueueManager
for distributed event processing with Redis pub/sub and expiring keys.

Requirements:
    - Redis server running on localhost:6379
    - redis>=5.0.0 (install with: pip install eventy[redis])
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime, UTC
from typing import Optional

try:
    from eventy.redis import RedisEventQueue, RedisQueueManager
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    print("Redis not available. Install with: pip install eventy[redis]")

from eventy.subscriber.subscriber import Subscriber


@dataclass
class OrderEvent:
    """Example order event payload"""
    order_id: str
    customer_id: str
    amount: float
    timestamp: datetime


@dataclass
class NotificationEvent:
    """Example notification event payload"""
    user_id: str
    message: str
    priority: str = "normal"


class OrderProcessor(Subscriber[OrderEvent]):
    """Example subscriber that processes order events"""
    
    def __init__(self, processor_name: str):
        self.processor_name = processor_name
        self.payload_type = OrderEvent
        self.processed_orders = []
    
    async def on_worker_event(self, event, source_worker_id, target_worker_id):
        """Process an order event"""
        print(f"[{self.processor_name}] Processing order {event.payload.order_id} "
              f"for ${event.payload.amount}")
        
        # Simulate processing time
        await asyncio.sleep(0.1)
        
        self.processed_orders.append(event.payload)
        print(f"[{self.processor_name}] Order {event.payload.order_id} processed successfully")


class NotificationSender(Subscriber[NotificationEvent]):
    """Example subscriber that sends notifications"""
    
    def __init__(self):
        self.payload_type = NotificationEvent
        self.sent_notifications = []
    
    async def on_worker_event(self, event, source_worker_id, target_worker_id):
        """Send a notification"""
        notification = event.payload
        print(f"[NotificationSender] Sending {notification.priority} notification "
              f"to user {notification.user_id}: {notification.message}")
        
        self.sent_notifications.append(notification)


async def demonstrate_redis_event_queue():
    """Demonstrate basic Redis event queue usage"""
    print("\n=== Redis Event Queue Demo ===")
    
    # Create Redis client
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=False)
    
    try:
        await redis_client.ping()
        print("✓ Connected to Redis")
    except Exception as e:
        print(f"✗ Failed to connect to Redis: {e}")
        return
    
    # Create event queue for orders
    order_queue = RedisEventQueue[OrderEvent](
        event_type=OrderEvent,
        redis_client=redis_client,
        key_prefix="demo",
        event_ttl=3600  # 1 hour TTL
    )
    
    async with order_queue:
        # Add subscribers
        processor1 = OrderProcessor("Processor-1")
        processor2 = OrderProcessor("Processor-2")
        
        sub1_id = await order_queue.subscribe(processor1)
        sub2_id = await order_queue.subscribe(processor2)
        
        print(f"✓ Added subscribers: {sub1_id}, {sub2_id}")
        
        # Publish some orders
        orders = [
            OrderEvent("ORD-001", "CUST-123", 99.99, datetime.now(UTC)),
            OrderEvent("ORD-002", "CUST-456", 149.50, datetime.now(UTC)),
            OrderEvent("ORD-003", "CUST-789", 75.25, datetime.now(UTC)),
        ]
        
        published_events = []
        for order in orders:
            event = await order_queue.publish(order)
            published_events.append(event)
            print(f"✓ Published order {order.order_id} as event {event.id}")
        
        # Wait a bit for processing
        await asyncio.sleep(0.5)
        
        # Check results
        print(f"✓ Processor-1 handled {len(processor1.processed_orders)} orders")
        print(f"✓ Processor-2 handled {len(processor2.processed_orders)} orders")
        
        # Search events
        page = await order_queue.search_events(limit=10)
        print(f"✓ Found {len(page.items)} events in queue")
        
        # Count events
        total_count = await order_queue.count_events()
        print(f"✓ Total events in queue: {total_count}")
        
        # Unsubscribe
        await order_queue.unsubscribe(sub1_id)
        await order_queue.unsubscribe(sub2_id)
        print("✓ Unsubscribed all processors")
    
    await redis_client.close()


async def demonstrate_redis_queue_manager():
    """Demonstrate Redis queue manager usage"""
    print("\n=== Redis Queue Manager Demo ===")
    
    # Create Redis client
    redis_client = redis.Redis(host='localhost', port=6379, decode_responses=False)
    
    # Create queue manager
    manager = RedisQueueManager(
        redis_client=redis_client,
        key_prefix="demo_manager",
        event_ttl=3600
    )
    
    async with manager:
        print("✓ Queue manager initialized")
        
        # Register multiple queue types
        await manager.register(OrderEvent)
        await manager.register(NotificationEvent)
        
        print("✓ Registered OrderEvent and NotificationEvent queues")
        
        # Get queues
        order_queue = await manager.get_event_queue(OrderEvent)
        notification_queue = await manager.get_event_queue(NotificationEvent)
        
        # Add subscribers
        order_processor = OrderProcessor("Manager-Processor")
        notification_sender = NotificationSender()
        
        await order_queue.subscribe(order_processor)
        await notification_queue.subscribe(notification_sender)
        
        print("✓ Added subscribers to both queues")
        
        # Publish events to both queues
        order = OrderEvent("ORD-100", "CUST-999", 299.99, datetime.now(UTC))
        notification = NotificationEvent("CUST-999", "Your order has been received!", "high")
        
        order_event = await order_queue.publish(order)
        notification_event = await notification_queue.publish(notification)
        
        print(f"✓ Published order event {order_event.id}")
        print(f"✓ Published notification event {notification_event.id}")
        
        # Wait for processing
        await asyncio.sleep(0.2)
        
        # Check queue types
        queue_types = await manager.get_queue_types()
        print(f"✓ Active queue types: {[t.__name__ for t in queue_types]}")
        
        # Cleanup
        await manager.deregister(OrderEvent)
        await manager.deregister(NotificationEvent)
        print("✓ Deregistered all queues")


async def demonstrate_distributed_processing():
    """Demonstrate distributed processing capabilities"""
    print("\n=== Distributed Processing Demo ===")
    
    # Simulate multiple workers by creating separate queue instances
    # that share the same Redis backend
    
    redis_client1 = redis.Redis(host='localhost', port=6379, decode_responses=False)
    redis_client2 = redis.Redis(host='localhost', port=6379, decode_responses=False)
    
    # Worker 1 - Publisher
    queue1 = RedisEventQueue[NotificationEvent](
        event_type=NotificationEvent,
        redis_client=redis_client1,
        key_prefix="distributed_demo"
    )
    
    # Worker 2 - Subscriber
    queue2 = RedisEventQueue[NotificationEvent](
        event_type=NotificationEvent,
        redis_client=redis_client2,
        key_prefix="distributed_demo"
    )
    
    async with queue1, queue2:
        # Add subscriber to worker 2
        sender = NotificationSender()
        await queue2.subscribe(sender)
        
        print("✓ Worker 2 subscribed to notifications")
        
        # Publish from worker 1
        notifications = [
            NotificationEvent("USER-001", "Welcome to our service!", "normal"),
            NotificationEvent("USER-002", "Your payment was processed", "high"),
            NotificationEvent("USER-003", "New features available", "low"),
        ]
        
        for notification in notifications:
            event = await queue1.publish(notification)
            print(f"✓ Worker 1 published notification {event.id}")
        
        # Wait for processing
        await asyncio.sleep(0.3)
        
        print(f"✓ Worker 2 processed {len(sender.sent_notifications)} notifications")
        
        # Both workers can read the same events
        events1 = await queue1.search_events(limit=10)
        events2 = await queue2.search_events(limit=10)
        
        print(f"✓ Worker 1 sees {len(events1.items)} events")
        print(f"✓ Worker 2 sees {len(events2.items)} events")
        
        assert len(events1.items) == len(events2.items), "Both workers should see the same events"
    
    await redis_client1.close()
    await redis_client2.close()


async def main():
    """Run all demonstrations"""
    if not REDIS_AVAILABLE:
        print("Redis not available. Please install with: pip install eventy[redis]")
        return
    
    try:
        await demonstrate_redis_event_queue()
        await demonstrate_redis_queue_manager()
        await demonstrate_distributed_processing()
        
        print("\n🎉 All Redis demonstrations completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Demo failed: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())