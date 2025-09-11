"""Tests for FileQueueManager"""

import pytest
import tempfile
import shutil
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import AsyncMock, patch, MagicMock

from eventy.eventy_error import EventyError
from eventy.fs.file_queue_manager import FileQueueManager
from eventy.fs.polling_file_event_queue import PollingFileEventQueue
from eventy.serializers.serializer import get_default_serializer


@dataclass
class MockPayload:
    """Test payload for file queue manager tests."""
    message: str
    value: int = 42


@dataclass
class AnotherPayload:
    """Another test payload for testing multiple payload types."""
    name: str
    count: int = 0


class TestFileQueueManager:
    """Test cases for FileQueueManager"""

    def create_file_queue_manager(self, **kwargs) -> FileQueueManager:
        """Create a FileQueueManager instance for testing"""
        temp_dir = tempfile.mkdtemp()
        defaults = {
            "root_dir": Path(temp_dir),
            "serializer": get_default_serializer(),
            "polling_interval": 0.1,  # Faster for tests
        }
        defaults.update(kwargs)
        return FileQueueManager(**defaults)

    def teardown_method(self):
        """Clean up temporary directories after each test"""
        # This will be called after each test method
        pass

    @pytest.mark.asyncio
    async def test_initialization(self):
        """Test FileQueueManager initialization"""
        temp_dir = tempfile.mkdtemp()
        try:
            manager = FileQueueManager(
                root_dir=temp_dir,
                serializer=get_default_serializer(),
                polling_interval=1.5
            )

            assert manager.root_dir == Path(temp_dir)
            assert manager.serializer is not None
            assert manager.polling_interval == 1.5
            assert len(manager._queues) == 0
            assert manager._entered is False
            assert manager.root_dir.exists()  # Directory should be created
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_initialization_with_string_path(self):
        """Test FileQueueManager initialization with string path"""
        temp_dir = tempfile.mkdtemp()
        try:
            manager = FileQueueManager(root_dir=temp_dir)  # String path
            
            assert manager.root_dir == Path(temp_dir)
            assert manager.root_dir.exists()
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_watchdog_detection(self):
        """Test watchdog availability detection"""
        temp_dir = tempfile.mkdtemp()
        try:
            # Create manager and check that watchdog detection works
            manager = FileQueueManager(root_dir=temp_dir)
            
            # The _use_watchdog should be set based on actual watchdog availability
            # Since watchdog is available in this environment, it should be True
            assert manager._use_watchdog is not None
            assert isinstance(manager._use_watchdog, bool)
            
            # Test manual override
            manager._use_watchdog = False
            assert manager._use_watchdog is False
            
            manager._use_watchdog = True
            assert manager._use_watchdog is True
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_context_manager_enter_exit(self):
        """Test context manager functionality"""
        manager = self.create_file_queue_manager()
        temp_dir = manager.root_dir
        
        try:
            assert not manager._entered

            async with manager:
                assert manager._entered
                assert manager.root_dir.exists()

            assert not manager._entered
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_context_manager_with_existing_queues(self):
        """Test context manager with pre-registered queues"""
        manager = self.create_file_queue_manager()
        temp_dir = manager.root_dir
        
        try:
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
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_check_entered_raises_error(self):
        """Test that operations require entering the context manager"""
        manager = self.create_file_queue_manager()
        temp_dir = manager.root_dir
        
        try:
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
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_create_polling_queue(self):
        """Test creating a polling queue when watchdog is not available"""
        manager = self.create_file_queue_manager()
        temp_dir = manager.root_dir
        
        try:
            # Force polling queue usage
            manager._use_watchdog = False
            
            queue = manager._create_queue(MockPayload)
            
            assert isinstance(queue, PollingFileEventQueue)
            assert queue.payload_type == MockPayload
            assert queue.polling_interval == manager.polling_interval
            assert queue.root_dir == manager.root_dir / MockPayload.__name__
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_create_watchdog_queue_fallback(self):
        """Test fallback to polling queue when watchdog import fails"""
        manager = self.create_file_queue_manager()
        temp_dir = manager.root_dir
        
        try:
            # Force watchdog usage but make import fail
            manager._use_watchdog = True
            
            # Mock the dynamic import to fail
            with patch('builtins.__import__') as mock_import:
                def side_effect(name, *args, **kwargs):
                    if name == 'eventy.fs.watchdog_file_event_queue':
                        raise ImportError("Test import error")
                    return __import__(name, *args, **kwargs)
                mock_import.side_effect = side_effect
                
                queue = manager._create_queue(MockPayload)
                
                assert isinstance(queue, PollingFileEventQueue)
                assert manager._use_watchdog is False  # Should be set to False after failure
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_create_watchdog_queue_success(self):
        """Test creating a watchdog queue when available"""
        manager = self.create_file_queue_manager()
        temp_dir = manager.root_dir
        
        try:
            # Force watchdog usage
            manager._use_watchdog = True
            
            # Mock WatchdogFileEventQueue class
            mock_watchdog_class = MagicMock()
            mock_watchdog_queue = MagicMock()
            mock_watchdog_queue.payload_type = MockPayload
            mock_watchdog_class.return_value = mock_watchdog_queue
            
            # Mock the dynamic import to return our mock class
            with patch('builtins.__import__') as mock_import:
                def side_effect(name, *args, **kwargs):
                    if name == 'eventy.fs.watchdog_file_event_queue':
                        mock_module = MagicMock()
                        mock_module.WatchdogFileEventQueue = mock_watchdog_class
                        return mock_module
                    return __import__(name, *args, **kwargs)
                mock_import.side_effect = side_effect
                
                queue = manager._create_queue(MockPayload)
                
                assert queue is mock_watchdog_queue
                mock_watchdog_class.assert_called_once_with(
                    root_dir=manager.root_dir / MockPayload.__name__,
                    payload_type=MockPayload,
                    event_serializer=manager.serializer,
                    result_serializer=manager.serializer,
                    subscriber_serializer=manager.serializer,
                    claim_serializer=manager.serializer,
                )
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_register_and_get_queue(self):
        """Test registering a payload type and getting the queue"""
        manager = self.create_file_queue_manager()
        temp_dir = manager.root_dir
        
        try:
            # Force polling queue usage to make test predictable
            manager._use_watchdog = False
            
            async with manager:
                # Register a payload type
                await manager.register(MockPayload)

                # Check that queue was created
                assert MockPayload in manager._queues
                queue = await manager.get_event_queue(MockPayload)
                assert isinstance(queue, PollingFileEventQueue)
                assert queue.payload_type == MockPayload
                
                # Check that queue directory was created
                queue_dir = manager.root_dir / MockPayload.__name__
                assert queue_dir.exists()
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_register_duplicate_payload_type(self):
        """Test registering the same payload type twice"""
        manager = self.create_file_queue_manager()
        temp_dir = manager.root_dir
        
        try:
            async with manager:
                # Register once
                await manager.register(MockPayload)
                initial_queue = manager._queues[MockPayload]

                # Register again - should not create new queue
                await manager.register(MockPayload)
                assert manager._queues[MockPayload] is initial_queue
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_register_multiple_payload_types(self):
        """Test registering multiple different payload types"""
        manager = self.create_file_queue_manager()
        temp_dir = manager.root_dir
        
        try:
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
                
                # Check that separate directories were created
                assert (manager.root_dir / MockPayload.__name__).exists()
                assert (manager.root_dir / AnotherPayload.__name__).exists()
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_register_before_entering_context(self):
        """Test registering a payload type before entering context manager"""
        manager = self.create_file_queue_manager()
        temp_dir = manager.root_dir
        
        try:
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
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_get_queue_types(self):
        """Test getting list of registered queue types"""
        manager = self.create_file_queue_manager()
        temp_dir = manager.root_dir
        
        try:
            async with manager:
                # Initially empty
                types = await manager.get_queue_types()
                assert types == []

                # Register types
                await manager.register(MockPayload)
                await manager.register(AnotherPayload)

                types = await manager.get_queue_types()
                assert set(types) == {MockPayload, AnotherPayload}
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_deregister(self):
        """Test deregistering a payload type"""
        manager = self.create_file_queue_manager()
        temp_dir = manager.root_dir
        
        try:
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
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_deregister_nonexistent_type(self):
        """Test deregistering a non-existent payload type"""
        manager = self.create_file_queue_manager()
        temp_dir = manager.root_dir
        
        try:
            async with manager:
                # Should not raise error, just log warning
                await manager.deregister(MockPayload)
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_deregister_with_exception(self):
        """Test deregistering when queue.__aexit__ raises exception"""
        manager = self.create_file_queue_manager()
        temp_dir = manager.root_dir
        
        try:
            async with manager:
                await manager.register(MockPayload)
                
                # Mock the queue's __aexit__ to raise an exception
                mock_queue = manager._queues[MockPayload]
                mock_queue.__aexit__ = AsyncMock(side_effect=Exception("Test exception"))

                # Should not raise exception, just log warning
                await manager.deregister(MockPayload)
                assert MockPayload not in manager._queues
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_get_nonexistent_queue(self):
        """Test getting a queue for unregistered payload type"""
        manager = self.create_file_queue_manager()
        temp_dir = manager.root_dir
        
        try:
            async with manager:
                with pytest.raises(
                    EventyError, match="No queue registered for payload type"
                ):
                    await manager.get_event_queue(MockPayload)
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_reset_queue(self):
        """Test resetting a queue"""
        manager = self.create_file_queue_manager()
        temp_dir = manager.root_dir
        
        try:
            async with manager:
                await manager.register(MockPayload)
                queue = manager._queues[MockPayload]
                
                # Create some mock directories and files
                queue.events_dir = manager.root_dir / MockPayload.__name__ / "events"
                queue.results_dir = manager.root_dir / MockPayload.__name__ / "results"
                queue.claims_dir = manager.root_dir / MockPayload.__name__ / "claims"
                
                queue.events_dir.mkdir(parents=True, exist_ok=True)
                queue.results_dir.mkdir(parents=True, exist_ok=True)
                queue.claims_dir.mkdir(parents=True, exist_ok=True)
                
                # Create some test files
                (queue.events_dir / "test_event.json").write_text("test")
                (queue.results_dir / "test_result.json").write_text("test")
                (queue.claims_dir / "test_claim.json").write_text("test")
                
                # Set some state
                queue.processed_event_id = 5
                queue.next_event_id = 10

                # Reset should remove directories and reset state
                await manager.reset(MockPayload)
                
                assert not queue.events_dir.exists()
                assert not queue.results_dir.exists()
                assert not queue.claims_dir.exists()
                assert queue.processed_event_id == 0
                assert queue.next_event_id == 1
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_custom_serializer(self):
        """Test creating manager with custom serializer"""
        temp_dir = tempfile.mkdtemp()
        try:
            custom_serializer = get_default_serializer()
            manager = FileQueueManager(
                root_dir=temp_dir,
                serializer=custom_serializer,
                polling_interval=0.5
            )

            assert manager.serializer is custom_serializer
            assert manager.polling_interval == 0.5

            async with manager:
                await manager.register(MockPayload)
                queue = await manager.get_event_queue(MockPayload)
                assert queue.event_serializer is custom_serializer
                assert queue.result_serializer is custom_serializer
                assert queue.subscriber_serializer is custom_serializer
                assert queue.claim_serializer is custom_serializer
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_queue_isolation(self):
        """Test that different payload types have isolated queues and directories"""
        manager = self.create_file_queue_manager()
        temp_dir = manager.root_dir
        
        try:
            async with manager:
                await manager.register(MockPayload)
                await manager.register(AnotherPayload)

                mock_queue = await manager.get_event_queue(MockPayload)
                another_queue = await manager.get_event_queue(AnotherPayload)

                # Queues should be completely separate instances
                assert mock_queue is not another_queue
                assert mock_queue.payload_type != another_queue.payload_type
                
                # Should have separate directories
                assert mock_queue.root_dir != another_queue.root_dir
                assert mock_queue.root_dir == manager.root_dir / MockPayload.__name__
                assert another_queue.root_dir == manager.root_dir / AnotherPayload.__name__
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_concurrent_operations(self):
        """Test concurrent registration and deregistration"""
        import asyncio
        
        manager = self.create_file_queue_manager()
        temp_dir = manager.root_dir
        
        try:
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
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_directory_creation_and_cleanup(self):
        """Test that directories are properly created and managed"""
        temp_dir = tempfile.mkdtemp()
        try:
            # Test with non-existent directory
            non_existent_dir = Path(temp_dir) / "non_existent"
            manager = FileQueueManager(root_dir=non_existent_dir)
            
            # Directory should be created during initialization
            assert non_existent_dir.exists()
            
            async with manager:
                await manager.register(MockPayload)
                
                # Queue-specific directory should be created
                queue_dir = non_existent_dir / MockPayload.__name__
                queue = await manager.get_event_queue(MockPayload)
                assert queue.root_dir == queue_dir
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_manager_state_consistency(self):
        """Test that manager state remains consistent across operations"""
        manager = self.create_file_queue_manager()
        temp_dir = manager.root_dir
        
        try:
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
        finally:
            shutil.rmtree(temp_dir)

    @pytest.mark.asyncio
    async def test_polling_interval_configuration(self):
        """Test that polling interval is properly configured"""
        temp_dir = tempfile.mkdtemp()
        try:
            custom_interval = 2.5
            manager = FileQueueManager(
                root_dir=temp_dir,
                polling_interval=custom_interval
            )
            
            # Force polling queue usage
            manager._use_watchdog = False
            
            async with manager:
                await manager.register(MockPayload)
                queue = await manager.get_event_queue(MockPayload)
                
                assert isinstance(queue, PollingFileEventQueue)
                assert queue.polling_interval == custom_interval
        finally:
            shutil.rmtree(temp_dir)