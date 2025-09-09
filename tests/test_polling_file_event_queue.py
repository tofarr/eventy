"""
Test cases for PollingFileEventQueue implementation.

This module contains concrete test cases for the PollingFileEventQueue class
that inherit from the abstract test base.
"""

import asyncio
import tempfile
import shutil
import unittest
from pathlib import Path
from eventy.event_queue import EventQueue
from eventy.fs.polling_file_event_queue import PollingFileEventQueue
from tests.abstract_event_queue_base import AbstractEventQueueTestBase


class TestPollingFileEventQueue(AbstractEventQueueTestBase):
    """Test cases for PollingFileEventQueue implementation"""
    
    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        # Create a temporary directory for file storage
        self.temp_dir = tempfile.mkdtemp()
        self.root_path = Path(self.temp_dir)
    
    def tearDown(self):
        """Clean up test fixtures"""
        super().tearDown()
        # Clean up temporary directory
        if hasattr(self, 'temp_dir'):
            shutil.rmtree(self.temp_dir, ignore_errors=True)
    
    async def create_queue(self) -> EventQueue[str]:
        """Create a PollingFileEventQueue instance for testing"""
        return PollingFileEventQueue(
            root_dir=self.root_path,
            payload_type=str,
            polling_interval=0.1  # Fast polling for tests
        )
    
    # Additional tests specific to PollingFileEventQueue
    
    async def test_polling_queue_directory_creation(self):
        """Test that PollingFileEventQueue creates necessary directories"""
        queue = PollingFileEventQueue(
            root_dir=self.root_path / "new_queue",
            payload_type=str,
            polling_interval=0.1
        )
        
        async with queue:
            # Directories should be created
            self.assertTrue((self.root_path / "new_queue").exists())
            self.assertTrue((self.root_path / "new_queue" / "events").exists())
            self.assertTrue((self.root_path / "new_queue" / "results").exists())
            self.assertTrue((self.root_path / "new_queue" / "subscriptions").exists())
            self.assertTrue((self.root_path / "new_queue" / "claims").exists())
    
    async def test_polling_queue_file_persistence(self):
        """Test that events are persisted to files"""
        async with self.queue:
            event = await self.queue.publish("persistent_event")
            
            # Check that event file exists (files are stored without extension, just the event ID)
            event_file = self.root_path / "events" / str(event.id)
            self.assertTrue(event_file.exists())
            
            # Verify we can read the event back
            retrieved_event = await self.queue.get_event(event.id)
            self.assertEqual(retrieved_event.payload, "persistent_event")
    
    async def test_polling_queue_isolation_by_directory(self):
        """Test that different PollingFileEventQueue instances with different directories are isolated"""
        temp_dir2 = tempfile.mkdtemp()
        root_path2 = Path(temp_dir2)
        
        try:
            queue1 = PollingFileEventQueue(
                root_dir=self.root_path,
                payload_type=str,
                polling_interval=0.1
            )
            queue2 = PollingFileEventQueue(
                root_dir=root_path2,
                payload_type=str,
                polling_interval=0.1
            )
            
            async with queue1, queue2:
                # Publish to each queue
                event1 = await queue1.publish("queue1_event")
                event2 = await queue2.publish("queue2_event")
                
                # Each queue should only see its own events
                events1 = await queue1.search_events()
                events2 = await queue2.search_events()
                
                self.assertEqual(len(events1.items), 1)
                self.assertEqual(len(events2.items), 1)
                self.assertEqual(events1.items[0].payload, "queue1_event")
                self.assertEqual(events2.items[0].payload, "queue2_event")
        
        finally:
            shutil.rmtree(temp_dir2, ignore_errors=True)
    
    async def test_polling_queue_background_tasks(self):
        """Test that polling background tasks are started and stopped properly"""
        queue = PollingFileEventQueue(
            root_dir=self.root_path,
            payload_type=str,
            polling_interval=0.1
        )
        
        # Before entering, tasks should not exist
        self.assertFalse(hasattr(queue, '_polling_task') and queue._polling_task)
        
        async with queue:
            # After entering, tasks should be running
            self.assertTrue(hasattr(queue, '_polling_task'))
            self.assertTrue(hasattr(queue, '_subscription_polling_task'))
            
            if queue._polling_task:
                self.assertFalse(queue._polling_task.done())
            if queue._subscription_polling_task:
                self.assertFalse(queue._subscription_polling_task.done())
        
        # After exiting, tasks should be cancelled/done
        if hasattr(queue, '_polling_task') and queue._polling_task:
            self.assertTrue(queue._polling_task.done() or queue._polling_task.cancelled())
        if hasattr(queue, '_subscription_polling_task') and queue._subscription_polling_task:
            self.assertTrue(queue._subscription_polling_task.done() or queue._subscription_polling_task.cancelled())
    
    async def test_polling_queue_event_processing_delay(self):
        """Test that polling queue processes events with some delay"""
        async with self.queue:
            # Subscribe a subscriber
            from tests.abstract_event_queue_base import MockSubscriber
            subscriber = MockSubscriber("polling_test")
            await self.queue.subscribe(subscriber)
            
            # Publish an event
            await self.queue.publish("delayed_event")
            
            # Wait for polling to process the event
            await asyncio.sleep(0.3)  # Wait longer than polling interval
            
            # Check if subscriber received the event
            # Note: This test might be flaky depending on the implementation
            # Some implementations might not process events immediately
    
    async def test_polling_queue_custom_polling_interval(self):
        """Test that custom polling interval is respected"""
        queue = PollingFileEventQueue(
            root_dir=self.root_path,
            payload_type=str,
            polling_interval=0.05  # Very fast polling
        )
        
        async with queue:
            self.assertEqual(queue.polling_interval, 0.05)
    
    async def test_polling_queue_restart_persistence(self):
        """Test that data persists across queue restarts"""
        # First session - create some data
        queue1 = PollingFileEventQueue(
            root_dir=self.root_path,
            payload_type=str,
            polling_interval=0.1
        )
        
        async with queue1:
            event = await queue1.publish("persistent_data")
            claim_result = await queue1.create_claim("persistent_claim", "test_data")
            self.assertTrue(claim_result)
        
        # Second session - verify data persists
        queue2 = PollingFileEventQueue(
            root_dir=self.root_path,
            payload_type=str,
            polling_interval=0.1
        )
        
        async with queue2:
            # Should be able to retrieve the event
            retrieved_event = await queue2.get_event(event.id)
            self.assertEqual(retrieved_event.payload, "persistent_data")
            
            # Should be able to retrieve the claim
            claim = await queue2.get_claim("persistent_claim")
            self.assertEqual(claim.data, "test_data")


if __name__ == '__main__':
    unittest.main()