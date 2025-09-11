"""Tests for MemoryQueueManager"""

import pytest
from dataclasses import dataclass
from unittest.mock import AsyncMock, patch

from eventy.eventy_error import EventyError
from eventy.mem.memory_queue_manager import MemoryQueueManager
from eventy.mem.memory_event_queue import MemoryEventQueue
from eventy.serializers.serializer import get_default_serializer


@dataclass
class MockPayload:
    """Test payload for memory queue manager tests."""
    message: str
    value: int = 42


@dataclass
class AnotherPayload:
    """Another test payload for testing multiple payload types."""
    name: str
    count: int = 0


class TestMemoryQueueManager:
    """Test cases for MemoryQueueManager"""

    def create_memory_queue_manager(self, **kwargs) -> MemoryQueueManager:
        """Create a MemoryQueueManager instance for testing"""
        defaults = {
            "serializer": get_default_serializer(),
        }
        defaults.update(kwargs)
        return MemoryQueueManager(**defaults)

    @pytest.mark.asyncio
    async def test_initialization(self):
        """Test MemoryQueueManager initialization"""
        manager = self.create_memory_queue_manager()

        assert manager.serializer is not None
        assert len(manager._queues) == 0
        assert manager._entered is False

    @pytest.mark.asyncio
    async def test_context_manager_enter_exit(self):
        """Test context manager functionality"""
        manager = self.create_memory_queue_manager()

        assert not manager._entered

        async with manager:
            assert manager._entered

        assert not manager._entered

    @pytest.mark.asyncio
    async def test_context_manager_with_existing_queues(self):
        """Test context manager with pre-registered queues"""
        manager = self.create_memory_queue_manager()

        # Register a queue before entering context
        await manager.register(MockPayload)
        
        # Mock the queue's context manager methods
        mock_queue = manager._queues[MockPayload]
        mock_queue.__aenter__ = AsyncMock(return_value=mock_queue)
        mock_queue.__aexit__ = AsyncMock(return_value=None)

        async with manager:
            assert manager._entered
            # Verify queue was entered
            mock_queue.__aenter__.assert_called_once()

        # Verify queue was exited
        mock_queue.__aexit__.assert_called_once()
        assert not manager._entered
        assert len(manager._queues) == 0  # Queues cleared on exit

    @pytest.mark.asyncio
    async def test_check_entered_raises_error(self):
        """Test that operations require entering the context manager"""
        manager = self.create_memory_queue_manager()

        with pytest.raises(
            EventyError, match="must be entered using async context manager"
        ):
            await manager.get_event_queue(MockPayload)

        with pytest.raises(
            EventyError, match="must be entered using async context manager"
        ):
            await manager.get_queue_types()

        with pytest.raises(
            EventyError, match="must be entered using async context manager"
        ):
            await manager.deregister(MockPayload)

    @pytest.mark.asyncio
    async def test_register_and_get_queue(self):
        """Test registering a payload type and getting the queue"""
        manager = self.create_memory_queue_manager()

        async with manager:
            # Register a payload type
            await manager.register(MockPayload)

            # Check that queue was created
            assert MockPayload in manager._queues
            queue = await manager.get_event_queue(MockPayload)
            assert isinstance(queue, MemoryEventQueue)
            assert queue.payload_type == MockPayload

    @pytest.mark.asyncio
    async def test_register_duplicate_payload_type(self):
        """Test registering the same payload type twice"""
        manager = self.create_memory_queue_manager()

        async with manager:
            # Register once
            await manager.register(MockPayload)
            initial_queue = manager._queues[MockPayload]

            # Register again - should not create new queue
            await manager.register(MockPayload)
            assert manager._queues[MockPayload] is initial_queue

    @pytest.mark.asyncio
    async def test_register_multiple_payload_types(self):
        """Test registering multiple different payload types"""
        manager = self.create_memory_queue_manager()

        async with manager:
            await manager.register(MockPayload)
            await manager.register(AnotherPayload)

            assert MockPayload in manager._queues
            assert AnotherPayload in manager._queues

            mock_queue = await manager.get_event_queue(MockPayload)
            another_queue = await manager.get_event_queue(AnotherPayload)

            assert mock_queue.payload_type == MockPayload
            assert another_queue.payload_type == AnotherPayload
            assert mock_queue is not another_queue

    @pytest.mark.asyncio
    async def test_register_before_entering_context(self):
        """Test registering a payload type before entering context manager"""
        manager = self.create_memory_queue_manager()

        # Register before entering context
        await manager.register(MockPayload)
        assert MockPayload in manager._queues

        # Mock the queue's __aenter__ method
        mock_queue = manager._queues[MockPayload]
        mock_queue.__aenter__ = AsyncMock(return_value=mock_queue)
        mock_queue.__aexit__ = AsyncMock(return_value=None)

        # Enter context - should call __aenter__ on existing queue
        async with manager:
            mock_queue.__aenter__.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_queue_types(self):
        """Test getting list of registered queue types"""
        manager = self.create_memory_queue_manager()

        async with manager:
            # Initially empty
            types = await manager.get_queue_types()
            assert types == []

            # Register types
            await manager.register(MockPayload)
            await manager.register(AnotherPayload)

            types = await manager.get_queue_types()
            assert set(types) == {MockPayload, AnotherPayload}

    @pytest.mark.asyncio
    async def test_deregister(self):
        """Test deregistering a payload type"""
        manager = self.create_memory_queue_manager()

        async with manager:
            # Register and then deregister
            await manager.register(MockPayload)
            assert MockPayload in manager._queues

            # Mock the queue's __aexit__ method
            mock_queue = manager._queues[MockPayload]
            mock_queue.__aexit__ = AsyncMock(return_value=None)

            await manager.deregister(MockPayload)
            assert MockPayload not in manager._queues
            mock_queue.__aexit__.assert_called_once_with(None, None, None)

    @pytest.mark.asyncio
    async def test_deregister_nonexistent_type(self):
        """Test deregistering a non-existent payload type"""
        manager = self.create_memory_queue_manager()

        async with manager:
            # Should not raise error, just log warning
            await manager.deregister(MockPayload)

    @pytest.mark.asyncio
    async def test_deregister_with_exception(self):
        """Test deregistering when queue.__aexit__ raises exception"""
        manager = self.create_memory_queue_manager()

        async with manager:
            await manager.register(MockPayload)
            
            # Mock the queue's __aexit__ to raise an exception
            mock_queue = manager._queues[MockPayload]
            mock_queue.__aexit__ = AsyncMock(side_effect=Exception("Test exception"))

            # Should not raise exception, just log warning
            await manager.deregister(MockPayload)
            assert MockPayload not in manager._queues

    @pytest.mark.asyncio
    async def test_get_nonexistent_queue(self):
        """Test getting a queue for unregistered payload type"""
        manager = self.create_memory_queue_manager()

        async with manager:
            with pytest.raises(
                EventyError, match="No queue registered for payload type"
            ):
                await manager.get_event_queue(MockPayload)

    @pytest.mark.asyncio
    async def test_reset_queue(self):
        """Test resetting a queue"""
        manager = self.create_memory_queue_manager()

        async with manager:
            await manager.register(MockPayload)
            original_queue = manager._queues[MockPayload]

            # Reset should create a new queue instance
            await manager.reset(MockPayload)
            new_queue = manager._queues[MockPayload]

            assert new_queue is not original_queue
            assert isinstance(new_queue, MemoryEventQueue)
            assert new_queue.payload_type == MockPayload

    @pytest.mark.asyncio
    async def test_reset_queue_not_entered(self):
        """Test resetting a queue when manager is not entered"""
        manager = self.create_memory_queue_manager()

        # Reset should work even when not entered
        await manager.reset(MockPayload)
        assert MockPayload in manager._queues
        assert isinstance(manager._queues[MockPayload], MemoryEventQueue)

    @pytest.mark.asyncio
    async def test_custom_serializer(self):
        """Test creating manager with custom serializer"""
        custom_serializer = get_default_serializer()
        manager = self.create_memory_queue_manager(serializer=custom_serializer)

        assert manager.serializer is custom_serializer

        async with manager:
            await manager.register(MockPayload)
            queue = await manager.get_event_queue(MockPayload)
            assert queue.serializer is custom_serializer

    @pytest.mark.asyncio
    async def test_queue_isolation(self):
        """Test that different payload types have isolated queues"""
        manager = self.create_memory_queue_manager()

        async with manager:
            await manager.register(MockPayload)
            await manager.register(AnotherPayload)

            mock_queue = await manager.get_event_queue(MockPayload)
            another_queue = await manager.get_event_queue(AnotherPayload)

            # Queues should be completely separate instances
            assert mock_queue is not another_queue
            assert mock_queue.payload_type != another_queue.payload_type

    @pytest.mark.asyncio
    async def test_concurrent_operations(self):
        """Test concurrent registration and deregistration"""
        import asyncio
        
        manager = self.create_memory_queue_manager()

        async def register_payload(payload_type):
            await manager.register(payload_type)

        async def deregister_payload(payload_type):
            await manager.deregister(payload_type)

        async with manager:
            # Concurrent registration
            await asyncio.gather(
                register_payload(MockPayload),
                register_payload(AnotherPayload)
            )

            assert MockPayload in manager._queues
            assert AnotherPayload in manager._queues

            # Mock the queues' __aexit__ methods
            for queue in manager._queues.values():
                queue.__aexit__ = AsyncMock(return_value=None)

            # Concurrent deregistration
            await asyncio.gather(
                deregister_payload(MockPayload),
                deregister_payload(AnotherPayload)
            )

            assert MockPayload not in manager._queues
            assert AnotherPayload not in manager._queues

    @pytest.mark.asyncio
    async def test_manager_state_consistency(self):
        """Test that manager state remains consistent across operations"""
        manager = self.create_memory_queue_manager()

        # Test state before entering
        assert not manager._entered
        assert len(manager._queues) == 0

        async with manager:
            # Test state after entering
            assert manager._entered

            # Register some queues
            await manager.register(MockPayload)
            await manager.register(AnotherPayload)
            
            assert len(manager._queues) == 2
            types = await manager.get_queue_types()
            assert len(types) == 2

            # Deregister one
            mock_queue = manager._queues[MockPayload]
            mock_queue.__aexit__ = AsyncMock(return_value=None)
            
            await manager.deregister(MockPayload)
            assert len(manager._queues) == 1
            
            types = await manager.get_queue_types()
            assert len(types) == 1
            assert AnotherPayload in types

        # Test state after exiting
        assert not manager._entered
        assert len(manager._queues) == 0  # Cleared on exit