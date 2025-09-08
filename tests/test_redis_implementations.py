"""Tests for Redis-based implementations."""

import pytest
import asyncio
from dataclasses import dataclass
from datetime import datetime, UTC
from uuid import uuid4

try:
    import redis.asyncio as redis
    from eventy.redis.redis_event_queue import RedisEventQueue
    from eventy.redis.redis_queue_manager import RedisQueueManager
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

from eventy.event_status import EventStatus
from eventy.subscriber.subscriber import Subscriber


@dataclass
class RedisTestPayload:
    """Test payload for Redis queue tests"""
    message: str
    timestamp: datetime


class TestSubscriber(Subscriber[RedisTestPayload]):
    """Test subscriber for Redis queue tests"""
    
    def __init__(self):
        self.received_events = []
        self.payload_type = RedisTestPayload
    
    async def on_worker_event(self, event, source_worker_id, target_worker_id):
        """Handle received events"""
        self.received_events.append(event)


@pytest.mark.skipif(not REDIS_AVAILABLE, reason="Redis not available")
class TestRedisImplementations:
    """Test Redis-based implementations"""
    
    @pytest.fixture
    async def redis_client(self):
        """Create a Redis client for testing"""
        try:
            client = redis.Redis(host='localhost', port=6379, decode_responses=False)
            await client.ping()
            yield client
            await client.close()
        except Exception:
            pytest.skip("Redis server not available")
    
    @pytest.fixture
    async def redis_queue(self, redis_client):
        """Create a Redis event queue for testing"""
        queue = RedisEventQueue[RedisTestPayload](
            event_type=RedisTestPayload,
            redis_client=redis_client,
            key_prefix="test_eventy"
        )
        async with queue:
            yield queue
    
    @pytest.fixture
    async def redis_manager(self, redis_client):
        """Create a Redis queue manager for testing"""
        manager = RedisQueueManager(
            redis_client=redis_client,
            key_prefix="test_eventy"
        )
        async with manager:
            yield manager
    
    @pytest.mark.asyncio
    async def test_redis_event_queue_basic_operations(self, redis_queue):
        """Test basic Redis event queue operations"""
        # Test publishing an event
        payload = RedisTestPayload(
            message="Hello Redis!",
            timestamp=datetime.now(UTC)
        )
        
        event = await redis_queue.publish(payload)
        assert event.id == 1
        assert event.payload.message == "Hello Redis!"
        assert event.status == EventStatus.PROCESSING
        
        # Test retrieving the event
        retrieved_event = await redis_queue.get_event(1)
        assert retrieved_event.id == 1
        assert retrieved_event.payload.message == "Hello Redis!"
        assert retrieved_event.status == EventStatus.PROCESSING
    
    @pytest.mark.asyncio
    async def test_redis_event_queue_subscribers(self, redis_queue):
        """Test Redis event queue subscriber functionality"""
        subscriber = TestSubscriber()
        
        # Subscribe
        subscriber_id = await redis_queue.subscribe(subscriber)
        assert subscriber_id is not None
        
        # Publish an event
        payload = RedisTestPayload(
            message="Test subscriber",
            timestamp=datetime.now(UTC)
        )
        
        event = await redis_queue.publish(payload)
        
        # Check that subscriber received the event
        assert len(subscriber.received_events) == 1
        assert subscriber.received_events[0].payload.message == "Test subscriber"
        
        # Unsubscribe
        success = await redis_queue.unsubscribe(subscriber_id)
        assert success is True
    
    @pytest.mark.asyncio
    async def test_redis_event_queue_search_events(self, redis_queue):
        """Test Redis event queue search functionality"""
        # Publish multiple events
        payloads = [
            RedisTestPayload(message=f"Event {i}", timestamp=datetime.now(UTC))
            for i in range(5)
        ]
        
        events = []
        for payload in payloads:
            event = await redis_queue.publish(payload)
            events.append(event)
        
        # Search all events
        page = await redis_queue.search_events(limit=10)
        assert len(page.items) == 5
        
        # Search with limit
        page = await redis_queue.search_events(limit=2)
        assert len(page.items) == 2
        assert page.next_page_id is not None
        
        # Count events
        count = await redis_queue.count_events()
        assert count == 5
    
    @pytest.mark.asyncio
    async def test_redis_queue_manager_basic_operations(self, redis_manager):
        """Test basic Redis queue manager operations"""
        # Register a queue type
        await redis_manager.register(RedisTestPayload)
        
        # Get the queue
        queue = await redis_manager.get_event_queue(RedisTestPayload)
        assert isinstance(queue, RedisEventQueue)
        assert queue.event_type == RedisTestPayload
        
        # Check queue types
        queue_types = await redis_manager.get_queue_types()
        assert RedisTestPayload in queue_types
        
        # Deregister
        await redis_manager.deregister(RedisTestPayload)
        
        # Should raise KeyError now
        with pytest.raises(KeyError):
            await redis_manager.get_event_queue(RedisTestPayload)
    
    @pytest.mark.asyncio
    async def test_redis_queue_manager_with_events(self, redis_manager):
        """Test Redis queue manager with actual events"""
        # Register and get queue
        await redis_manager.register(RedisTestPayload)
        queue = await redis_manager.get_event_queue(RedisTestPayload)
        
        # Publish an event through the manager's queue
        payload = RedisTestPayload(
            message="Manager test",
            timestamp=datetime.now(UTC)
        )
        
        event = await queue.publish(payload)
        assert event.payload.message == "Manager test"
        
        # Retrieve the event
        retrieved_event = await queue.get_event(event.id)
        assert retrieved_event.payload.message == "Manager test"


if __name__ == "__main__":
    # Simple test runner for manual testing
    async def run_basic_test():
        """Run a basic test manually"""
        try:
            client = redis.Redis(host='localhost', port=6379, decode_responses=False)
            await client.ping()
            print("✓ Redis connection successful")
            
            # Test queue creation
            queue = RedisEventQueue[RedisTestPayload](
                event_type=RedisTestPayload,
                redis_client=client,
                key_prefix="manual_test"
            )
            
            async with queue:
                # Test publishing
                payload = RedisTestPayload(
                    message="Manual test",
                    timestamp=datetime.now(UTC)
                )
                
                event = await queue.publish(payload)
                print(f"✓ Published event {event.id}: {event.payload.message}")
                
                # Test retrieval
                retrieved = await queue.get_event(event.id)
                print(f"✓ Retrieved event {retrieved.id}: {retrieved.payload.message}")
                
                print("✓ All manual tests passed!")
            
            await client.close()
            
        except Exception as e:
            print(f"✗ Test failed: {e}")
            import traceback
            traceback.print_exc()
    
    if REDIS_AVAILABLE:
        asyncio.run(run_basic_test())
    else:
        print("Redis not available for manual testing")