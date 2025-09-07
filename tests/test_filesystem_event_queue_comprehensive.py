"""
Comprehensive test suite for FilesystemEventQueue using the abstract test case.

This module provides concrete tests for FilesystemEventQueue by extending the
AbstractEventQueueCase. It includes all the comprehensive tests defined in
the abstract case plus any FilesystemEventQueue-specific tests.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from eventy.fs.filesystem_event_queue import FilesystemEventQueue
from eventy.event_queue import EventQueue
from tests.abstract_event_queue_case import AbstractEventQueueCase, MockPayload


class TestFilesystemEventQueueComprehensive(AbstractEventQueueCase):
    """
    Comprehensive test suite for FilesystemEventQueue.

    This class extends AbstractEventQueueCase to run all the standard
    EventQueue tests against the FilesystemEventQueue implementation.
    """

    def get_event_queue(self) -> EventQueue[MockPayload]:
        """Return a FilesystemEventQueue instance for testing"""
        # Create a temporary directory for each test
        temp_dir = Path(tempfile.mkdtemp())
        return FilesystemEventQueue(
            event_type=MockPayload, 
            root_dir=temp_dir,
            subscriber_task_sleep=0.1  # Faster processing for tests
        )

    # FilesystemEventQueue-specific tests can be added here

    @pytest.mark.asyncio
    async def test_filesystem_queue_initialization(self):
        """Test FilesystemEventQueue-specific initialization"""
        temp_dir = Path(tempfile.mkdtemp())
        try:
            async with FilesystemEventQueue(event_type=MockPayload, root_dir=temp_dir) as queue:
                assert isinstance(queue, FilesystemEventQueue)
                assert queue.event_type == MockPayload
                assert queue.root_dir == temp_dir
                assert queue.root_dir.exists()
                
                # Check that required directories are created
                assert (queue.root_dir / "payload").exists()
                assert (queue.root_dir / "page").exists()
                assert (queue.root_dir / "meta").exists()
                assert (queue.root_dir / "worker").exists()
                assert (queue.root_dir / "process").exists()
                assert (queue.root_dir / "worker_event").exists()
                assert (queue.root_dir / "subscriber").exists()
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_filesystem_queue_subscriber_persistence(self):
        """Test that subscribers are persisted to disk and reloaded"""
        temp_dir = Path(tempfile.mkdtemp())
        try:
            from tests.abstract_event_queue_case import MockSubscriber
            
            subscriber = MockSubscriber("persistent_sub")
            subscriber_id = None
            
            # Create queue and add subscriber
            async with FilesystemEventQueue(event_type=MockPayload, root_dir=temp_dir) as queue:
                subscriber_id = await queue.subscribe(subscriber)
                
                # Check that subscriber file was created
                subscriber_file = queue.subscriber_dir / subscriber_id.hex
                assert subscriber_file.exists()
            
            # Create a new queue instance with the same directory
            async with FilesystemEventQueue(event_type=MockPayload, root_dir=temp_dir) as new_queue:
                # Subscriber should be loaded from disk
                assert subscriber_id in new_queue.subscribers
                retrieved_subscriber = await new_queue.get_subscriber(subscriber_id)
                assert retrieved_subscriber.subscriber_id == subscriber.subscriber_id
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_filesystem_queue_subscriber_cleanup_on_unsubscribe(self):
        """Test that subscriber files are cleaned up when unsubscribing"""
        temp_dir = Path(tempfile.mkdtemp())
        try:
            from tests.abstract_event_queue_case import MockSubscriber
            
            async with FilesystemEventQueue(event_type=MockPayload, root_dir=temp_dir) as queue:
                subscriber = MockSubscriber("cleanup_test")
                subscriber_id = await queue.subscribe(subscriber)
                
                # Check that subscriber file exists
                subscriber_file = queue.subscriber_dir / subscriber_id.hex
                assert subscriber_file.exists()
                
                # Unsubscribe
                result = await queue.unsubscribe(subscriber_id)
                assert result is True
                
                # Check that subscriber file is removed
                assert not subscriber_file.exists()
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_filesystem_queue_event_storage_and_retrieval(self):
        """Test that events are properly stored and can be retrieved"""
        temp_dir = Path(tempfile.mkdtemp())
        try:
            async with FilesystemEventQueue(event_type=MockPayload, root_dir=temp_dir) as queue:
                # Publish an event
                payload = MockPayload("filesystem test", 789)
                published_event = await queue.publish(payload)
                
                # Retrieve the event
                retrieved_event = await queue.get_event(published_event.id)
                
                # Verify the event was properly stored and retrieved
                assert retrieved_event.id == published_event.id
                assert retrieved_event.payload.message == payload.message
                assert retrieved_event.payload.value == payload.value
                assert retrieved_event.created_at == published_event.created_at
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_filesystem_queue_background_task_management(self):
        """Test that background tasks are properly managed"""
        temp_dir = Path(tempfile.mkdtemp())
        try:
            queue = FilesystemEventQueue(event_type=MockPayload, root_dir=temp_dir)
            
            # Before entering context, tasks should not exist
            assert queue._bg_task is None
            assert queue._subscriber_task is None
            
            async with queue:
                # After entering context, tasks should be created
                assert queue._bg_task is not None
                assert queue._subscriber_task is not None
                assert not queue._bg_task.done()
                assert not queue._subscriber_task.done()
            
            # After exiting context, tasks should be cancelled
            assert queue._bg_task.cancelled()
            assert queue._subscriber_task.cancelled()
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_filesystem_queue_directory_structure(self):
        """Test that the correct directory structure is created"""
        temp_dir = Path(tempfile.mkdtemp())
        try:
            async with FilesystemEventQueue(event_type=MockPayload, root_dir=temp_dir) as queue:
                # Check all expected directories exist
                expected_dirs = [
                    "payload", "page", "meta", "worker", "process", 
                    "worker_event", "subscriber"
                ]
                
                for dir_name in expected_dirs:
                    dir_path = queue.root_dir / dir_name
                    assert dir_path.exists(), f"Directory {dir_name} should exist"
                    assert dir_path.is_dir(), f"{dir_name} should be a directory"
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_filesystem_queue_multiple_instances_same_directory(self):
        """Test behavior when multiple queue instances use the same directory"""
        temp_dir = Path(tempfile.mkdtemp())
        try:
            from tests.abstract_event_queue_case import MockSubscriber
            
            # Create first queue instance and add a subscriber
            async with FilesystemEventQueue(event_type=MockPayload, root_dir=temp_dir) as queue1:
                subscriber1 = MockSubscriber("shared_dir_test1")
                subscriber_id1 = await queue1.subscribe(subscriber1)
                
                # Create second queue instance with same directory
                async with FilesystemEventQueue(event_type=MockPayload, root_dir=temp_dir) as queue2:
                    # Second queue should load the subscriber from disk
                    assert subscriber_id1 in queue2.subscribers
                    
                    # Add another subscriber to second queue
                    subscriber2 = MockSubscriber("shared_dir_test2")
                    subscriber_id2 = await queue2.subscribe(subscriber2)
                    
                    # Both subscribers should be present
                    assert len(queue2.subscribers) == 2
                    assert subscriber_id1 in queue2.subscribers
                    assert subscriber_id2 in queue2.subscribers
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    @pytest.mark.asyncio
    async def test_filesystem_queue_graceful_shutdown(self):
        """Test that the queue shuts down gracefully"""
        temp_dir = Path(tempfile.mkdtemp())
        try:
            queue = FilesystemEventQueue(event_type=MockPayload, root_dir=temp_dir)
            
            # Start the queue
            await queue.__aenter__()
            
            # Verify background tasks are running
            assert queue._bg_task is not None
            assert queue._subscriber_task is not None
            assert not queue._bg_task.done()
            assert not queue._subscriber_task.done()
            
            # Shutdown the queue
            await queue.__aexit__(None, None, None)
            
            # Verify tasks are cancelled
            assert queue._bg_task.cancelled()
            assert queue._subscriber_task.cancelled()
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)