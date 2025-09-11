#!/usr/bin/env python3
"""
Example demonstrating the RedisQueueManager usage.

This example shows how to:
1. Create a RedisQueueManager instance
2. Register payload types
3. Create and process events using Redis-enhanced queues
4. Handle Redis connection failures gracefully
"""

import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

from eventy.redis.redis_queue_manager import RedisQueueManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class TaskPayload:
    """Example payload for task processing"""
    task_id: str
    description: str
    priority: int = 1


@dataclass
class NotificationPayload:
    """Example payload for notifications"""
    user_id: str
    message: str
    notification_type: str = "info"


async def main():
    """Main example function"""
    
    # Create RedisQueueManager with custom configuration
    manager = RedisQueueManager(
        root_dir=Path("./redis_queue_data"),
        redis_url="redis://localhost:6379",
        redis_db=1,
        redis_prefix="example_app",
        worker_id=uuid4(),
        claim_expiration_seconds=300,  # 5 minutes
        enable_pubsub_monitor=True,
        resync=False,
    )
    
    logger.info("Starting RedisQueueManager example...")
    
    async with manager:
        # Register payload types
        await manager.register(TaskPayload)
        await manager.register(NotificationPayload)
        
        logger.info(f"Registered queue types: {await manager.get_queue_types()}")
        
        # Get Redis connection status
        status = await manager.get_redis_connection_status()
        logger.info(f"Redis connection status: {status}")
        
        # Get event queues
        task_queue = await manager.get_event_queue(TaskPayload)
        notification_queue = await manager.get_event_queue(NotificationPayload)
        
        # Create some events
        logger.info("Creating events...")
        
        # Create task events
        task1 = TaskPayload(task_id="task-001", description="Process user data", priority=1)
        task2 = TaskPayload(task_id="task-002", description="Generate report", priority=2)
        
        event1 = await task_queue.publish(task1)
        event2 = await task_queue.publish(task2)
        
        logger.info(f"Created task events: {event1.id}, {event2.id}")
        
        # Create notification events
        notification1 = NotificationPayload(
            user_id="user-123", 
            message="Your task has been completed",
            notification_type="success"
        )
        notification2 = NotificationPayload(
            user_id="user-456", 
            message="New message received",
            notification_type="info"
        )
        
        notif_event1 = await notification_queue.publish(notification1)
        notif_event2 = await notification_queue.publish(notification2)
        
        logger.info(f"Created notification events: {notif_event1.id}, {notif_event2.id}")
        
        # Process events by creating claims
        logger.info("Processing events...")
        
        # Process task events
        async for claim in task_queue.iter_claims(limit=2):
            logger.info(f"Processing task: {claim.payload.description} (Priority: {claim.payload.priority})")
            
            # Simulate processing
            await asyncio.sleep(0.1)
            
            # Create result
            result = f"Task {claim.payload.task_id} completed successfully"
            await task_queue.add_result(claim.event_id, result)
            logger.info(f"Task {claim.payload.task_id} completed")
        
        # Process notification events
        async for claim in notification_queue.iter_claims(limit=2):
            logger.info(f"Sending notification to {claim.payload.user_id}: {claim.payload.message}")
            
            # Simulate sending notification
            await asyncio.sleep(0.1)
            
            # Create result
            result = f"Notification sent to {claim.payload.user_id}"
            await notification_queue.add_result(claim.event_id, result)
            logger.info(f"Notification sent to {claim.payload.user_id}")
        
        # Show queue statistics
        task_event_count = await task_queue.count_events()
        task_result_count = await task_queue.count_results()
        notif_event_count = await notification_queue.count_events()
        notif_result_count = await notification_queue.count_results()
        
        logger.info(f"Task queue - Events: {task_event_count}, Results: {task_result_count}")
        logger.info(f"Notification queue - Events: {notif_event_count}, Results: {notif_result_count}")
        
        # Demonstrate resync functionality
        logger.info("Enabling resync for all queues...")
        await manager.set_resync_for_all_queues(True)
        
        # Reset one queue to demonstrate cleanup
        logger.info("Resetting task queue...")
        await manager.reset(TaskPayload)
        
        # Verify reset
        task_event_count_after = await task_queue.count_events()
        task_result_count_after = await task_queue.count_results()
        logger.info(f"Task queue after reset - Events: {task_event_count_after}, Results: {task_result_count_after}")
        
        logger.info("RedisQueueManager example completed successfully!")


async def demonstrate_fallback():
    """Demonstrate graceful fallback when Redis is unavailable"""
    logger.info("\n--- Demonstrating Redis fallback behavior ---")
    
    # Create manager with invalid Redis URL to simulate connection failure
    manager = RedisQueueManager(
        root_dir=Path("./redis_queue_fallback_data"),
        redis_url="redis://invalid-host:6379",  # This will fail
        redis_db=1,
        redis_prefix="fallback_example",
        worker_id=uuid4(),
    )
    
    async with manager:
        # Register a payload type
        await manager.register(TaskPayload)
        
        # Check Redis connection status (should be False)
        status = await manager.get_redis_connection_status()
        logger.info(f"Redis connection status (should be False): {status}")
        
        # Queue should still work in filesystem-only mode
        task_queue = await manager.get_event_queue(TaskPayload)
        
        # Create and process an event
        task = TaskPayload(task_id="fallback-001", description="Test fallback mode")
        event = await task_queue.publish(task)
        logger.info(f"Created event in fallback mode: {event.id}")
        
        # Process the event
        async for claim in task_queue.iter_claims(limit=1):
            logger.info(f"Processing in fallback mode: {claim.payload.description}")
            result = f"Task {claim.payload.task_id} completed in fallback mode"
            await task_queue.add_result(claim.event_id, result)
            logger.info("Event processed successfully in fallback mode")
        
        logger.info("Fallback demonstration completed!")


if __name__ == "__main__":
    try:
        # Run main example
        asyncio.run(main())
        
        # Demonstrate fallback behavior
        asyncio.run(demonstrate_fallback())
        
    except KeyboardInterrupt:
        logger.info("Example interrupted by user")
    except Exception as e:
        logger.error(f"Example failed: {e}", exc_info=True)