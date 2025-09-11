import asyncio
import tempfile
import shutil
import unittest
import pytest
from pathlib import Path
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

from eventy.event_queue import EventQueue
from eventy.redis import RedisFileEventQueue
from tests.abstract_event_queue_base import AbstractEventQueueTestBase


@dataclass
class TestPayload:
    """Simple test payload for testing"""

    message: str
    value: int


class TestSubscriber:
    """Simple test subscriber that can be pickled"""

    async def on_event(self, event, event_queue) -> None:
        pass


class TestRedisFileEventQueue(AbstractEventQueueTestBase):
    """Test cases for RedisFileEventQueue implementation extending AbstractEventQueueTestBase"""

    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        # Create a temporary directory for file storage
        self.temp_dir = tempfile.mkdtemp()
        self.root_path = Path(self.temp_dir)

        # Set up Redis mock for all tests
        self.mock_redis = AsyncMock()
        self.mock_redis.incr.return_value = 1
        self.mock_redis.set.return_value = True
        self.mock_redis.get.return_value = None
        self.mock_redis.publish.return_value = 1
        self.mock_redis.close.return_value = None
        self.mock_redis.wait_closed.return_value = None
        self.mock_redis.ping.return_value = True

        # Mock pubsub - use MagicMock for synchronous methods
        self.mock_pubsub = MagicMock()
        self.mock_pubsub.subscribe = AsyncMock(return_value=None)
        self.mock_pubsub.unsubscribe = AsyncMock(return_value=None)
        self.mock_pubsub.close = AsyncMock(return_value=None)
        self.mock_pubsub.get_message = AsyncMock(return_value=None)
        self.mock_pubsub.__aenter__ = AsyncMock(return_value=self.mock_pubsub)
        self.mock_pubsub.__aexit__ = AsyncMock(return_value=None)

        # Make pubsub() return the mock directly (not a coroutine)
        self.mock_redis.pubsub = MagicMock(return_value=self.mock_pubsub)

        # Start the Redis mock patch
        self.redis_patch = patch("redis.asyncio.from_url", return_value=self.mock_redis)
        self.redis_patch.start()

    def tearDown(self):
        """Clean up test fixtures"""
        super().tearDown()
        # Stop the Redis mock patch
        if hasattr(self, "redis_patch"):
            self.redis_patch.stop()
        # Clean up temporary directory
        if hasattr(self, "temp_dir"):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def create_queue(self) -> EventQueue[str]:
        """Create a RedisFileEventQueue instance for testing"""
        return RedisFileEventQueue(
            root_dir=self.root_path,
            payload_type=str,
            redis_url="redis://localhost:6379",
            redis_db=0,
            redis_prefix="test_eventy",
            enable_pubsub_monitor=False,  # Disable for testing
        )


class TestRedisFileEventQueueSpecific(unittest.IsolatedAsyncioTestCase):
    """Redis-specific test cases that don't inherit from base"""

    def setUp(self):
        """Set up test fixtures"""
        self.temp_dir = tempfile.mkdtemp()
        self.root_path = Path(self.temp_dir)

    def tearDown(self):
        """Clean up test fixtures"""
        if hasattr(self, "temp_dir"):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def create_redis_queue(self) -> RedisFileEventQueue[TestPayload]:
        """Create a RedisFileEventQueue instance for Redis-specific testing"""
        return RedisFileEventQueue(
            root_dir=self.root_path,
            payload_type=TestPayload,
            redis_url="redis://localhost:6379",
            redis_db=1,
            redis_prefix="test_redis",
        )

    async def test_initialization(self):
        """Test RedisFileEventQueue initialization"""
        queue = await self.create_redis_queue()

        assert queue.redis_url == "redis://localhost:6379"
        assert queue.redis_db == 1
        assert queue.redis_prefix == "test_redis"
        assert queue.claim_expiration_seconds == 300
        assert queue.resync_lock_timeout_seconds == 30

    async def test_publish_event(self):
        """Test publishing an event with Redis coordination"""
        mock_redis = AsyncMock()
        mock_redis.set.return_value = True  # For updating event counter
        mock_redis.get.return_value = None  # No existing counter in Redis
        mock_redis.publish.return_value = 1
        mock_redis.ping.return_value = True

        # Set up pubsub mock properly
        mock_pubsub = MagicMock()
        mock_pubsub.subscribe = AsyncMock(return_value=None)
        mock_pubsub.unsubscribe = AsyncMock(return_value=None)
        mock_pubsub.close = AsyncMock(return_value=None)
        mock_redis.pubsub = MagicMock(return_value=mock_pubsub)

        queue = await self.create_redis_queue()

        with patch("redis.asyncio.from_url", return_value=mock_redis):
            async with queue:
                payload = TestPayload(message="test", value=42)
                event = await queue.publish(payload)

                assert event.payload == payload
                assert event.id == 1

                # Verify Redis operations were called
                mock_redis.set.assert_called()
                mock_redis.publish.assert_called()

    async def test_create_claim_success(self):
        """Test successful claim creation with Redis"""
        mock_redis = AsyncMock()
        mock_redis.set.return_value = True  # Successful claim
        mock_redis.publish.return_value = 1
        mock_redis.ping.return_value = True

        # Set up pubsub mock properly
        mock_pubsub = MagicMock()
        mock_pubsub.subscribe = AsyncMock(return_value=None)
        mock_pubsub.unsubscribe = AsyncMock(return_value=None)
        mock_pubsub.close = AsyncMock(return_value=None)
        mock_redis.pubsub = MagicMock(return_value=mock_pubsub)

        queue = await self.create_redis_queue()

        with patch("redis.asyncio.from_url", return_value=mock_redis):
            async with queue:
                success = await queue.create_claim("test_claim", "test_data")

                assert success is True

                # Verify Redis SET with NX and EX was called
                mock_redis.set.assert_called()
                call_args = mock_redis.set.call_args
                assert call_args[1]["nx"] is True  # Not exists
                assert "ex" in call_args[1]  # Expiration

    async def test_create_claim_failure(self):
        """Test failed claim creation (already exists)"""
        mock_redis = AsyncMock()
        mock_redis.set.return_value = False  # Claim already exists
        mock_redis.ping.return_value = True

        # Set up pubsub mock properly
        mock_pubsub = MagicMock()
        mock_pubsub.subscribe = AsyncMock(return_value=None)
        mock_pubsub.unsubscribe = AsyncMock(return_value=None)
        mock_pubsub.close = AsyncMock(return_value=None)
        mock_redis.pubsub = MagicMock(return_value=mock_pubsub)

        queue = await self.create_redis_queue()

        with patch("redis.asyncio.from_url", return_value=mock_redis):
            async with queue:
                success = await queue.create_claim("existing_claim", "test_data")

                assert success is False

    async def test_get_claim_from_redis(self):
        """Test getting a claim from Redis"""
        mock_redis = AsyncMock()

        # Set up different return values for different keys
        def mock_get(key):
            if "next_event_id" in key:
                return None  # No existing counter
            elif "claim:test_claim" in key:
                return '{"worker_id": "12345678-1234-5678-1234-567812345678", "data": "test_data", "created_at": "2023-01-01T00:00:00Z"}'
            return None

        mock_redis.get.side_effect = mock_get
        mock_redis.set.return_value = True
        mock_redis.ping.return_value = True

        # Set up pubsub mock properly
        mock_pubsub = MagicMock()
        mock_pubsub.subscribe = AsyncMock(return_value=None)
        mock_pubsub.unsubscribe = AsyncMock(return_value=None)
        mock_pubsub.close = AsyncMock(return_value=None)
        mock_redis.pubsub = MagicMock(return_value=mock_pubsub)

        queue = await self.create_redis_queue()

        with patch("redis.asyncio.from_url", return_value=mock_redis):
            async with queue:
                claim = await queue.get_claim("test_claim")

                assert claim.id == "test_claim"
                assert str(claim.worker_id) == "12345678-1234-5678-1234-567812345678"
                assert claim.data == "test_data"

    async def test_subscribe_publishes_to_redis(self):
        """Test that subscribing publishes to Redis pubsub"""
        subscriber = TestSubscriber()

        mock_redis = AsyncMock()
        mock_redis.publish.return_value = 1
        mock_redis.ping.return_value = True

        # Set up pubsub mock properly
        mock_pubsub = MagicMock()
        mock_pubsub.subscribe = AsyncMock(return_value=None)
        mock_pubsub.unsubscribe = AsyncMock(return_value=None)
        mock_pubsub.close = AsyncMock(return_value=None)
        mock_redis.pubsub = MagicMock(return_value=mock_pubsub)

        queue = await self.create_redis_queue()

        with patch("redis.asyncio.from_url", return_value=mock_redis):
            async with queue:
                subscription = await queue.subscribe(subscriber)

                assert subscription.subscriber == subscriber

                # Verify Redis publish was called for create_subscriber
                mock_redis.publish.assert_called()

    async def test_unsubscribe_publishes_to_redis(self):
        """Test that unsubscribing publishes to Redis pubsub"""
        subscriber = TestSubscriber()

        mock_redis = AsyncMock()
        mock_redis.publish.return_value = 1
        mock_redis.ping.return_value = True

        # Set up pubsub mock properly
        mock_pubsub = MagicMock()
        mock_pubsub.subscribe = AsyncMock(return_value=None)
        mock_pubsub.unsubscribe = AsyncMock(return_value=None)
        mock_pubsub.close = AsyncMock(return_value=None)
        mock_redis.pubsub = MagicMock(return_value=mock_pubsub)

        queue = await self.create_redis_queue()

        with patch("redis.asyncio.from_url", return_value=mock_redis):
            async with queue:
                # First subscribe
                subscription = await queue.subscribe(subscriber)

                # Reset mock to clear subscribe calls
                mock_redis.publish.reset_mock()

                # Then unsubscribe
                success = await queue.unsubscribe(subscription.id)

                assert success is True

                # Verify Redis publish was called for remove_subscriber
                mock_redis.publish.assert_called()

    async def test_redis_key_generation(self):
        """Test Redis key generation"""
        queue = await self.create_redis_queue()

        # Test various key types
        event_id_key = queue._get_redis_key("next_event_id")
        claim_key = queue._get_redis_key("claim", "test_claim")
        resync_key = queue._get_redis_key("resync_lock")

        expected_prefix = f"test_redis:{queue.root_dir.name}"

        assert event_id_key == f"{expected_prefix}:next_event_id"
        assert claim_key == f"{expected_prefix}:claim:test_claim"
        assert resync_key == f"{expected_prefix}:resync_lock"

    async def test_pubsub_channel_name(self):
        """Test pub/sub channel name generation"""
        queue = await self.create_redis_queue()

        channel = queue._get_pubsub_channel()
        expected_channel = f"test_redis:{queue.root_dir.name}:events"

        assert channel == expected_channel

    async def test_connection_error_handling(self):
        """Test graceful handling of Redis connection errors"""
        queue = await self.create_redis_queue()

        # Mock Redis to raise connection error
        with patch(
            "redis.asyncio.from_url", side_effect=Exception("Connection failed")
        ):
            # Should not raise exception, should handle gracefully
            async with queue:
                # Basic operations should still work (fallback to filesystem)
                payload = TestPayload(message="test", value=42)
                event = await queue.publish(payload)
                assert event.payload == payload

    async def test_import_error_handling(self):
        """Test handling when redis package is not available"""
        # This test simulates the case where redis is not installed
        with patch.dict("sys.modules", {"redis": None}):
            with patch(
                "builtins.__import__",
                side_effect=ImportError("No module named 'redis'"),
            ):
                # Should raise ImportError with helpful message
                with pytest.raises(ImportError, match="redis"):
                    await self.create_redis_queue()


if __name__ == "__main__":
    unittest.main()
