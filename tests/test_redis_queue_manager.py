import asyncio
import pytest
import tempfile
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

from eventy.eventy_error import EventyError
from eventy.redis.redis_queue_manager import RedisQueueManager
from eventy.redis.redis_file_event_queue import RedisFileEventQueue


@dataclass
class TestPayload:
    message: str
    value: int


class TestRedisQueueManager:
    """Test cases for RedisQueueManager"""

    def create_redis_queue_manager(self, **kwargs) -> RedisQueueManager:
        """Create a RedisQueueManager instance for testing"""
        temp_dir = tempfile.mkdtemp()
        defaults = {
            "root_dir": Path(temp_dir),
            "redis_url": "redis://localhost:6379",
            "redis_db": 1,
            "redis_prefix": "test_eventy",
            "worker_id": uuid4(),
        }
        defaults.update(kwargs)
        return RedisQueueManager(**defaults)

    @pytest.mark.asyncio
    @pytest.mark.asyncio
    async def test_initialization(self):
        """Test RedisQueueManager initialization"""
        manager = self.create_redis_queue_manager()
        
        assert manager.redis_url == "redis://localhost:6379"
        assert manager.redis_db == 1
        assert manager.redis_prefix == "test_eventy"
        assert manager.claim_expiration_seconds == 300
        assert manager.resync_lock_timeout_seconds == 30
        assert manager.enable_pubsub_monitor is True
        assert manager.resync is False
        assert len(manager._queues) == 0
        assert manager._entered is False

    @pytest.mark.asyncio
    async def test_context_manager_enter_exit(self):
        """Test context manager functionality"""
        manager = self.create_redis_queue_manager()
        
        assert not manager._entered
        
        async with manager:
            assert manager._entered
            
        assert not manager._entered

    @pytest.mark.asyncio
    async def test_check_entered_raises_error(self):
        """Test that operations require entering the context manager"""
        manager = self.create_redis_queue_manager()
        
        with pytest.raises(EventyError, match="must be entered using async context manager"):
            await manager.get_event_queue(TestPayload)
            
        with pytest.raises(EventyError, match="must be entered using async context manager"):
            await manager.get_queue_types()
            
        with pytest.raises(EventyError, match="must be entered using async context manager"):
            await manager.deregister(TestPayload)

    @pytest.mark.asyncio
    async def test_register_and_get_queue(self):
        """Test registering a payload type and getting the queue"""
        manager = self.create_redis_queue_manager()
        
        # Mock Redis to avoid actual connection
        with patch('redis.asyncio.from_url') as mock_redis_factory:
            mock_redis = AsyncMock()
            mock_redis.ping.return_value = True
            mock_redis.get.return_value = None
            mock_redis.set.return_value = True
            mock_redis.publish.return_value = 1
            
            # Set up pubsub mock
            mock_pubsub = MagicMock()
            mock_pubsub.subscribe = AsyncMock(return_value=None)
            mock_pubsub.unsubscribe = AsyncMock(return_value=None)
            mock_pubsub.close = AsyncMock(return_value=None)
            mock_redis.pubsub = MagicMock(return_value=mock_pubsub)
            mock_redis.close = AsyncMock(return_value=None)
            
            mock_redis_factory.return_value = mock_redis
            
            async with manager:
                # Register a payload type
                await manager.register(TestPayload)
                
                # Check that queue was created
                assert TestPayload in manager._queues
                queue = await manager.get_event_queue(TestPayload)
                assert isinstance(queue, RedisFileEventQueue)
                assert queue.payload_type == TestPayload

    @pytest.mark.asyncio
    async def test_register_duplicate_payload_type(self):
        """Test registering the same payload type twice"""
        manager = self.create_redis_queue_manager()
        
        with patch('redis.asyncio.from_url') as mock_redis_factory:
            mock_redis = AsyncMock()
            mock_redis.ping.return_value = True
            mock_redis.get.return_value = None
            mock_redis.set.return_value = True
            mock_redis.publish.return_value = 1
            
            mock_pubsub = MagicMock()
            mock_pubsub.subscribe = AsyncMock(return_value=None)
            mock_pubsub.unsubscribe = AsyncMock(return_value=None)
            mock_pubsub.close = AsyncMock(return_value=None)
            mock_redis.pubsub = MagicMock(return_value=mock_pubsub)
            mock_redis.close = AsyncMock(return_value=None)
            
            mock_redis_factory.return_value = mock_redis
            
            async with manager:
                # Register once
                await manager.register(TestPayload)
                initial_queue = manager._queues[TestPayload]
                
                # Register again - should not create new queue
                await manager.register(TestPayload)
                assert manager._queues[TestPayload] is initial_queue

    @pytest.mark.asyncio
    async def test_get_queue_types(self):
        """Test getting list of registered queue types"""
        manager = self.create_redis_queue_manager()
        
        with patch('redis.asyncio.from_url') as mock_redis_factory:
            mock_redis = AsyncMock()
            mock_redis.ping.return_value = True
            mock_redis.get.return_value = None
            mock_redis.set.return_value = True
            mock_redis.publish.return_value = 1
            
            mock_pubsub = MagicMock()
            mock_pubsub.subscribe = AsyncMock(return_value=None)
            mock_pubsub.unsubscribe = AsyncMock(return_value=None)
            mock_pubsub.close = AsyncMock(return_value=None)
            mock_redis.pubsub = MagicMock(return_value=mock_pubsub)
            mock_redis.close = AsyncMock(return_value=None)
            
            mock_redis_factory.return_value = mock_redis
            
            async with manager:
                # Initially empty
                types = await manager.get_queue_types()
                assert types == []
                
                # Register a type
                await manager.register(TestPayload)
                types = await manager.get_queue_types()
                assert types == [TestPayload]

    @pytest.mark.asyncio
    async def test_deregister(self):
        """Test deregistering a payload type"""
        manager = self.create_redis_queue_manager()
        
        with patch('redis.asyncio.from_url') as mock_redis_factory:
            mock_redis = AsyncMock()
            mock_redis.ping.return_value = True
            mock_redis.get.return_value = None
            mock_redis.set.return_value = True
            mock_redis.publish.return_value = 1
            
            mock_pubsub = MagicMock()
            mock_pubsub.subscribe = AsyncMock(return_value=None)
            mock_pubsub.unsubscribe = AsyncMock(return_value=None)
            mock_pubsub.close = AsyncMock(return_value=None)
            mock_redis.pubsub = MagicMock(return_value=mock_pubsub)
            mock_redis.close = AsyncMock(return_value=None)
            
            mock_redis_factory.return_value = mock_redis
            
            async with manager:
                # Register and then deregister
                await manager.register(TestPayload)
                assert TestPayload in manager._queues
                
                await manager.deregister(TestPayload)
                assert TestPayload not in manager._queues

    @pytest.mark.asyncio
    async def test_deregister_nonexistent_type(self):
        """Test deregistering a non-existent payload type"""
        manager = self.create_redis_queue_manager()
        
        async with manager:
            # Should not raise error, just log warning
            await manager.deregister(TestPayload)

    @pytest.mark.asyncio
    async def test_get_nonexistent_queue(self):
        """Test getting a queue for unregistered payload type"""
        manager = self.create_redis_queue_manager()
        
        async with manager:
            with pytest.raises(EventyError, match="No queue registered for payload type"):
                await manager.get_event_queue(TestPayload)

    @pytest.mark.asyncio
    async def test_reset_queue(self):
        """Test resetting a queue"""
        manager = self.create_redis_queue_manager()
        
        with patch('redis.asyncio.from_url') as mock_redis_factory:
            mock_redis = AsyncMock()
            mock_redis.ping.return_value = True
            mock_redis.get.return_value = None
            mock_redis.set.return_value = True
            mock_redis.publish.return_value = 1
            mock_redis.keys.return_value = ["key1", "key2"]
            mock_redis.delete.return_value = 2
            
            mock_pubsub = MagicMock()
            mock_pubsub.subscribe = AsyncMock(return_value=None)
            mock_pubsub.unsubscribe = AsyncMock(return_value=None)
            mock_pubsub.close = AsyncMock(return_value=None)
            mock_redis.pubsub = MagicMock(return_value=mock_pubsub)
            mock_redis.close = AsyncMock(return_value=None)
            
            mock_redis_factory.return_value = mock_redis
            
            async with manager:
                await manager.register(TestPayload)
                
                # Reset should not raise error
                await manager.reset(TestPayload)
                
                # Verify Redis keys were cleared
                mock_redis.keys.assert_called()
                mock_redis.delete.assert_called_with("key1", "key2")

    @pytest.mark.asyncio
    async def test_reset_nonexistent_queue(self):
        """Test resetting a non-existent queue"""
        manager = self.create_redis_queue_manager()
        
        async with manager:
            with pytest.raises(EventyError, match="No queue found for payload type"):
                await manager.reset(TestPayload)

    @pytest.mark.asyncio
    async def test_set_resync_for_all_queues(self):
        """Test setting resync for all queues"""
        manager = self.create_redis_queue_manager()
        
        with patch('redis.asyncio.from_url') as mock_redis_factory:
            mock_redis = AsyncMock()
            mock_redis.ping.return_value = True
            mock_redis.get.return_value = None
            mock_redis.set.return_value = True
            mock_redis.publish.return_value = 1
            
            mock_pubsub = MagicMock()
            mock_pubsub.subscribe = AsyncMock(return_value=None)
            mock_pubsub.unsubscribe = AsyncMock(return_value=None)
            mock_pubsub.close = AsyncMock(return_value=None)
            mock_redis.pubsub = MagicMock(return_value=mock_pubsub)
            mock_redis.close = AsyncMock(return_value=None)
            
            mock_redis_factory.return_value = mock_redis
            
            async with manager:
                await manager.register(TestPayload)
                
                # Set resync to True
                await manager.set_resync_for_all_queues(True)
                assert manager.resync is True
                
                queue = manager._queues[TestPayload]
                assert queue.resync is True

    @pytest.mark.asyncio
    async def test_get_redis_connection_status(self):
        """Test getting Redis connection status for all queues"""
        manager = self.create_redis_queue_manager()
        
        with patch('redis.asyncio.from_url') as mock_redis_factory:
            mock_redis = AsyncMock()
            mock_redis.ping.return_value = True
            mock_redis.get.return_value = None
            mock_redis.set.return_value = True
            mock_redis.publish.return_value = 1
            
            mock_pubsub = MagicMock()
            mock_pubsub.subscribe = AsyncMock(return_value=None)
            mock_pubsub.unsubscribe = AsyncMock(return_value=None)
            mock_pubsub.close = AsyncMock(return_value=None)
            mock_redis.pubsub = MagicMock(return_value=mock_pubsub)
            mock_redis.close = AsyncMock(return_value=None)
            
            mock_redis_factory.return_value = mock_redis
            
            async with manager:
                await manager.register(TestPayload)
                
                status = await manager.get_redis_connection_status()
                assert TestPayload in status
                assert status[TestPayload] is True  # Redis is connected

    @pytest.mark.asyncio
    async def test_redis_connection_failure_fallback(self):
        """Test graceful fallback when Redis connection fails"""
        manager = self.create_redis_queue_manager()
        
        # Mock Redis connection failure
        with patch('redis.asyncio.from_url', side_effect=Exception("Connection failed")):
            async with manager:
                await manager.register(TestPayload)
                
                # Queue should still be created and functional
                queue = await manager.get_event_queue(TestPayload)
                assert isinstance(queue, RedisFileEventQueue)
                
                # Redis should be None (fallback mode)
                status = await manager.get_redis_connection_status()
                assert status[TestPayload] is False

    @pytest.mark.asyncio
    async def test_queue_creation_with_custom_config(self):
        """Test creating queues with custom Redis configuration"""
        custom_config = {
            "redis_url": "redis://custom:6380",
            "redis_db": 5,
            "redis_prefix": "custom_prefix",
            "claim_expiration_seconds": 600,
            "resync_lock_timeout_seconds": 60,
            "enable_pubsub_monitor": False,
            "resync": True,
        }
        
        manager = self.create_redis_queue_manager(**custom_config)
        
        with patch('redis.asyncio.from_url') as mock_redis_factory:
            mock_redis = AsyncMock()
            mock_redis.ping.return_value = True
            mock_redis.get.return_value = None
            mock_redis.set.return_value = True
            mock_redis.publish.return_value = 1
            
            mock_pubsub = MagicMock()
            mock_pubsub.subscribe = AsyncMock(return_value=None)
            mock_pubsub.unsubscribe = AsyncMock(return_value=None)
            mock_pubsub.close = AsyncMock(return_value=None)
            mock_redis.pubsub = MagicMock(return_value=mock_pubsub)
            mock_redis.close = AsyncMock(return_value=None)
            
            mock_redis_factory.return_value = mock_redis
            
            async with manager:
                await manager.register(TestPayload)
                queue = await manager.get_event_queue(TestPayload)
                
                # Verify custom configuration was applied
                assert queue.redis_url == "redis://custom:6380"
                assert queue.redis_db == 5
                assert queue.redis_prefix == "custom_prefix"
                assert queue.claim_expiration_seconds == 600
                assert queue.resync_lock_timeout_seconds == 60
                assert queue.enable_pubsub_monitor is False
                assert queue.resync is True