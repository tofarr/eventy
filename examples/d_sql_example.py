"""Example demonstrating SQL-based event queue usage."""

import asyncio
import logging
import os
from dataclasses import dataclass
from typing import Optional

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

try:
    from eventy.sql import SqlEventQueue, SqlQueueManager, upgrade_database
    from eventy.subscribers.nonce_subscriber import NonceSubscriber
    SQL_AVAILABLE = True
except ImportError:
    logger.error("SQL dependencies not available. Install with: pip install eventy[sql]")
    SQL_AVAILABLE = False


@dataclass
class TaskPayload:
    """Example task payload."""
    task_name: str
    priority: int
    description: Optional[str] = None


@dataclass
class NotificationPayload:
    """Example notification payload."""
    recipient: str
    message: str
    urgent: bool = False


async def demonstrate_sql_event_queue():
    """Demonstrate basic SqlEventQueue usage."""
    if not SQL_AVAILABLE:
        return
        
    logger.info("=== SqlEventQueue Demo ===")
    
    # Use SQLite for this example
    database_url = "sqlite:///./example_eventy.db"
    
    # Ensure database schema is up to date
    try:
        upgrade_database(database_url)
        logger.info("Database schema updated")
    except Exception as e:
        logger.info(f"Database migration info: {e}")
    
    # Create event queue
    queue = SqlEventQueue(
        database_url=database_url,
        payload_type=TaskPayload
    )
    
    async with queue:
        logger.info(f"Started queue for {TaskPayload.__name__}")
        logger.info(f"Worker ID: {queue.get_worker_id()}")
        
        # Publish some events
        tasks = [
            TaskPayload("process_data", 1, "Process customer data"),
            TaskPayload("send_email", 2, "Send welcome email"),
            TaskPayload("backup_db", 3, "Daily database backup"),
        ]
        
        published_events = []
        for task in tasks:
            event = await queue.publish(task)
            published_events.append(event)
            logger.info(f"Published event {event.id}: {task.task_name}")
        
        # Search and display events
        logger.info("\n--- Searching Events ---")
        page = await queue.search_events(limit=10)
        for event in page.items:
            logger.info(f"Event {event.id}: {event.payload.task_name} (priority {event.payload.priority})")
        
        # Add a subscriber
        logger.info("\n--- Adding Subscriber ---")
        subscriber = NonceSubscriber(nonce="task-processor-1")
        subscription = await queue.subscribe(subscriber)
        logger.info(f"Added subscriber {subscription.id}")
        
        # Create some claims
        logger.info("\n--- Creating Claims ---")
        claims = ["claim-1", "claim-2", "claim-3"]
        for claim_id in claims:
            success = await queue.create_claim(claim_id, f"Data for {claim_id}")
            logger.info(f"Claim {claim_id}: {'created' if success else 'already exists'}")
        
        # Try to create duplicate claim
        duplicate = await queue.create_claim("claim-1", "Duplicate data")
        logger.info(f"Duplicate claim-1: {'created' if duplicate else 'already exists'}")
        
        # Search claims
        logger.info("\n--- Searching Claims ---")
        claims_page = await queue.search_claims()
        for claim in claims_page.items:
            logger.info(f"Claim {claim.id}: {claim.data}")
        
        # Store some results
        logger.info("\n--- Storing Results ---")
        from eventy.event_result import EventResult
        
        for i, event in enumerate(published_events[:2]):
            result = EventResult(
                worker_id=queue.get_worker_id(),
                event_id=event.id,
                success=i % 2 == 0,  # Alternate success/failure
                details=f"Processed task {event.payload.task_name}"
            )
            await queue._store_result(result)
            logger.info(f"Stored result for event {event.id}: {'success' if result.success else 'failure'}")
        
        # Search results
        logger.info("\n--- Searching Results ---")
        results_page = await queue.search_results()
        for result in results_page.items:
            status = "SUCCESS" if result.success else "FAILURE"
            logger.info(f"Result {result.id}: Event {result.event_id} - {status}")
        
        # Count various items
        event_count = await queue.count_events()
        result_count = await queue.count_results()
        subscription_count = await queue.count_subscriptions()
        claim_count = await queue.count_claims()
        
        logger.info(f"\n--- Summary ---")
        logger.info(f"Events: {event_count}")
        logger.info(f"Results: {result_count}")
        logger.info(f"Subscriptions: {subscription_count}")
        logger.info(f"Claims: {claim_count}")


async def demonstrate_sql_queue_manager():
    """Demonstrate SqlQueueManager with multiple payload types."""
    if not SQL_AVAILABLE:
        return
        
    logger.info("\n=== SqlQueueManager Demo ===")
    
    database_url = "sqlite:///./example_eventy.db"
    
    manager = SqlQueueManager(database_url=database_url)
    
    async with manager:
        logger.info("Started SqlQueueManager")
        
        # Register multiple payload types
        await manager.register(TaskPayload)
        await manager.register(NotificationPayload)
        
        logger.info("Registered payload types:")
        for payload_type in await manager.get_queue_types():
            logger.info(f"  - {payload_type.__name__}")
        
        # Get queues for different payload types
        task_queue = await manager.get_event_queue(TaskPayload)
        notification_queue = await manager.get_event_queue(NotificationPayload)
        
        # Publish to different queues
        logger.info("\n--- Publishing to Multiple Queues ---")
        
        task_event = await task_queue.publish(
            TaskPayload("analyze_logs", 1, "Analyze application logs")
        )
        logger.info(f"Published task event {task_event.id}")
        
        notification_event = await notification_queue.publish(
            NotificationPayload("admin@example.com", "System maintenance scheduled", urgent=True)
        )
        logger.info(f"Published notification event {notification_event.id}")
        
        # Verify isolation between queues
        logger.info("\n--- Verifying Queue Isolation ---")
        
        task_events = await task_queue.search_events()
        notification_events = await notification_queue.search_events()
        
        logger.info(f"Task queue has {len(task_events.items)} events")
        logger.info(f"Notification queue has {len(notification_events.items)} events")
        
        # Show events from each queue
        for event in task_events.items:
            logger.info(f"  Task: {event.payload.task_name}")
        
        for event in notification_events.items:
            logger.info(f"  Notification: {event.payload.message}")
        
        # Demonstrate reset functionality
        logger.info("\n--- Demonstrating Reset ---")
        logger.info(f"Before reset - Task events: {len(task_events.items)}")
        
        await manager.reset(TaskPayload)
        logger.info("Reset TaskPayload queue")
        
        task_events_after = await task_queue.search_events()
        notification_events_after = await notification_queue.search_events()
        
        logger.info(f"After reset - Task events: {len(task_events_after.items)}")
        logger.info(f"After reset - Notification events: {len(notification_events_after.items)}")


async def demonstrate_environment_integration():
    """Demonstrate integration with environment variables."""
    if not SQL_AVAILABLE:
        return
        
    logger.info("\n=== Environment Integration Demo ===")
    
    # Set environment variables
    os.environ["EVENTY_DATABASE_URL"] = "sqlite:///./example_eventy.db"
    os.environ["EVENTY_QUEUE_MANAGER"] = "eventy.sql.sql_queue_manager.SqlQueueManager"
    
    # Import after setting environment variables
    from eventy.queue_manager import get_default_queue_manager
    
    # Get default queue manager (should be SqlQueueManager)
    manager = await get_default_queue_manager()
    logger.info(f"Default queue manager type: {type(manager).__name__}")
    
    async with manager:
        # Register and use a queue
        await manager.register(TaskPayload)
        queue = await manager.get_event_queue(TaskPayload)
        
        event = await queue.publish(TaskPayload("env_test", 1, "Testing environment integration"))
        logger.info(f"Published event {event.id} using default manager")


async def main():
    """Run all demonstrations."""
    logger.info("Starting SQL Event Queue Examples")
    logger.info("=" * 50)
    
    if not SQL_AVAILABLE:
        logger.error("SQL dependencies not available. Install with: pip install eventy[sql]")
        return
    
    try:
        await demonstrate_sql_event_queue()
        await demonstrate_sql_queue_manager()
        await demonstrate_environment_integration()
        
        logger.info("\n" + "=" * 50)
        logger.info("All demonstrations completed successfully!")
        
    except Exception as e:
        logger.error(f"Error during demonstration: {e}", exc_info=True)
    
    finally:
        # Clean up example database
        try:
            os.unlink("example_eventy.db")
            logger.info("Cleaned up example database")
        except OSError:
            pass


if __name__ == "__main__":
    asyncio.run(main())