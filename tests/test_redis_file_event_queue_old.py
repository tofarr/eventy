import asyncio
import tempfile
import pytest
from pathlib import Path
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

from eventy.redis import RedisFileEventQueue


@dataclass
class TestPayload:
    """Simple test payload for testing"""
    message: str
    value: int


class TestSubscriber:
    """Simple test subscriber that can be pickled"""
    async def on_event(self, event, event_queue) -> None:
        pass


class TestRedisFileEventQueueClass:
    """Test cases for RedisFileEventQueue"""

    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for testing"""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield Path(temp_dir)

    @pytest.fixture
    def queue_dir(self, temp_dir):
        """Create a queue directory"""
        return temp_dir / "test_queue"

    @pytest.fixture
    def mock_redis(self):
        """Create a mock Redis client"""
        mock_redis = AsyncMock()
        mock_redis.ping = AsyncMock()
        mock_redis.get = AsyncMock(return_value=None)
        mock_redis.set = AsyncMock(return_value=True)
        mock_redis.delete = AsyncMock(return_value=1)
        mock_redis.publish = AsyncMock(return_value=1)
        mock_redis.close = AsyncMock()
        
        # Mock pubsub
        mock_pubsub = AsyncMock()
        mock_pubsub.subscribe = AsyncMock()
        mock_pubsub.unsubscribe = AsyncMock()
        mock_pubsub.close = AsyncMock()
        mock_pubsub.get_message = AsyncMock(return_value=None)
        mock_redis.pubsub = MagicMock(return_value=mock_pubsub)
        
        return mock_redis

    @pytest.fixture
    def redis_queue(self, queue_dir, mock_redis):
        """Create a RedisFileEventQueue with mocked Redis"""
        queue = RedisFileEventQueue(
            root_dir=queue_dir,
            payload_type=TestPayload,
            redis_url="redis://localhost:6379",
            redis_db=1,
        )
        return queue

    @pytest.mark.asyncio
    async def test_initialization(self, redis_queue, mock_redis):
        """Test queue initialization"""
        with patch('redis.asyncio.from_url', return_value=mock_redis):
            async with redis_queue:
                assert redis_queue.running
                assert redis_queue._redis is not None
                mock_redis.ping.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_event(self, redis_queue, mock_redis):
        """Test publishing an event"""
        with patch('redis.asyncio.from_url', return_value=mock_redis):
            async with redis_queue:
                payload = TestPayload(message="test", value=42)
                event = await redis_queue.publish(payload)
                
                assert event.id == 1
                assert event.payload == payload
                
                # Verify Redis operations were called
                mock_redis.set.assert_called()
                mock_redis.publish.assert_called()

    @pytest.mark.asyncio
    async def test_create_claim_success(self, redis_queue, mock_redis):
        """Test successful claim creation"""
        mock_redis.set.return_value = True  # Redis lock acquired
        
        with patch('redis.asyncio.from_url', return_value=mock_redis):
            async with redis_queue:
                success = await redis_queue.create_claim("test_claim", "test_data")
                
                assert success is True
                
                # Verify Redis SET with NX and EX was called
                mock_redis.set.assert_called()
                call_args = mock_redis.set.call_args
                assert call_args[1]['nx'] is True
                assert call_args[1]['ex'] == redis_queue.claim_expiration_seconds

    @pytest.mark.asyncio
    async def test_create_claim_failure(self, redis_queue, mock_redis):
        """Test failed claim creation (already exists)"""
        mock_redis.set.return_value = False  # Redis lock not acquired
        
        with patch('redis.asyncio.from_url', return_value=mock_redis):
            async with redis_queue:
                success = await redis_queue.create_claim("test_claim", "test_data")
                
                assert success is False

    @pytest.mark.asyncio
    async def test_get_claim_from_redis(self, redis_queue, mock_redis):
        """Test getting a claim from Redis when not in local filesystem"""
        import json
        from datetime import datetime
        
        # Mock Redis to return claim data for the claim key, but None for next_event_id
        def mock_get_side_effect(key):
            if "next_event_id" in key:
                return None  # No event ID stored yet
            elif "claim:test_claim" in key:
                claim_data = {
                    "worker_id": str(redis_queue.worker_id),
                    "created_at": datetime.now().isoformat(),
                    "data": "test_data"
                }
                return json.dumps(claim_data)
            return None
        
        mock_redis.get.side_effect = mock_get_side_effect
        
        with patch('redis.asyncio.from_url', return_value=mock_redis):
            async with redis_queue:
                claim = await redis_queue.get_claim("test_claim")
                
                assert claim.id == "test_claim"
                assert claim.worker_id == redis_queue.worker_id
                assert claim.data == "test_data"

    @pytest.mark.asyncio
    async def test_subscribe_publishes_to_redis(self, redis_queue, mock_redis):
        """Test that subscribing publishes to Redis pubsub"""
        subscriber = TestSubscriber()
        
        with patch('redis.asyncio.from_url', return_value=mock_redis):
            async with redis_queue:
                subscription = await redis_queue.subscribe(subscriber)
                
                assert subscription.subscriber == subscriber
                
                # Verify Redis publish was called for create_subscriber
                mock_redis.publish.assert_called()

    @pytest.mark.asyncio
    async def test_unsubscribe_publishes_to_redis(self, redis_queue, mock_redis):
        """Test that unsubscribing publishes to Redis pubsub"""
        subscriber = TestSubscriber()
        
        with patch('redis.asyncio.from_url', return_value=mock_redis):
            async with redis_queue:
                # First subscribe
                subscription = await redis_queue.subscribe(subscriber)
                
                # Reset mock to clear subscribe calls
                mock_redis.publish.reset_mock()
                
                # Then unsubscribe
                success = await redis_queue.unsubscribe(subscription.id)
                
                assert success is True
                
                # Verify Redis publish was called for remove_subscriber
                mock_redis.publish.assert_called()

    @pytest.mark.asyncio
    async def test_redis_key_generation(self, redis_queue):
        """Test Redis key generation"""
        key = redis_queue._get_redis_key("test_type", "test_id")
        expected = f"{redis_queue.redis_prefix}:{redis_queue.root_dir.name}:test_type:test_id"
        assert key == expected
        
        key_no_id = redis_queue._get_redis_key("test_type")
        expected_no_id = f"{redis_queue.redis_prefix}:{redis_queue.root_dir.name}:test_type"
        assert key_no_id == expected_no_id

    @pytest.mark.asyncio
    async def test_pubsub_channel_name(self, redis_queue):
        """Test pubsub channel name generation"""
        channel = redis_queue._get_pubsub_channel()
        expected = f"{redis_queue.redis_prefix}:{redis_queue.root_dir.name}:events"
        assert channel == expected

    @pytest.mark.asyncio
    async def test_connection_error_handling(self, queue_dir):
        """Test handling of Redis connection errors"""
        queue = RedisFileEventQueue(
            root_dir=queue_dir,
            payload_type=TestPayload,
            redis_url="redis://invalid-host:6379",
        )
        
        # Should raise an exception when trying to connect
        with pytest.raises(Exception):
            async with queue:
                pass

    def test_import_error_handling(self):
        """Test that appropriate error is raised when redis is not installed"""
        # This test would need to mock the import to simulate redis not being available
        # For now, we just verify the import works
        from eventy.redis import RedisFileEventQueue
        assert RedisFileEventQueue is not None