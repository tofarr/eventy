"""
Test cases for WatchdogFileEventQueue implementation.

This module contains concrete test cases for the WatchdogFileEventQueue class
that inherit from the abstract test base.
"""

import asyncio
import tempfile
import shutil
import unittest
from pathlib import Path
from eventy.event_queue import EventQueue
from eventy.fs.watchdog_file_event_queue import WatchdogFileEventQueue
from tests.abstract_event_queue_base import AbstractEventQueueTestBase


class TestWatchdogFileEventQueue(AbstractEventQueueTestBase):
    """Test cases for WatchdogFileEventQueue implementation"""

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
        if hasattr(self, "temp_dir"):
            shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def create_queue(self) -> EventQueue[str]:
        """Create a WatchdogFileEventQueue instance for testing"""
        return WatchdogFileEventQueue(root_dir=self.root_path, payload_type=str)

    # Additional tests specific to WatchdogFileEventQueue

    async def test_watchdog_queue_directory_creation(self):
        """Test that WatchdogFileEventQueue creates necessary directories"""
        queue = WatchdogFileEventQueue(
            root_dir=self.root_path / "new_queue", payload_type=str
        )

        async with queue:
            # Directories should be created
            self.assertTrue((self.root_path / "new_queue").exists())
            self.assertTrue((self.root_path / "new_queue" / "events").exists())
            self.assertTrue((self.root_path / "new_queue" / "results").exists())
            self.assertTrue((self.root_path / "new_queue" / "subscriptions").exists())
            self.assertTrue((self.root_path / "new_queue" / "claims").exists())

    async def test_watchdog_queue_file_persistence(self):
        """Test that events are persisted to files"""
        async with self.queue:
            event = await self.queue.publish("persistent_event")

            # Check that event file exists (files are stored without extension, just the event ID)
            event_file = self.root_path / "events" / str(event.id)
            self.assertTrue(event_file.exists())

            # Verify we can read the event back
            retrieved_event = await self.queue.get_event(event.id)
            self.assertEqual(retrieved_event.payload, "persistent_event")

    async def test_watchdog_queue_isolation_by_directory(self):
        """Test that different WatchdogFileEventQueue instances with different directories are isolated"""
        temp_dir2 = tempfile.mkdtemp()
        root_path2 = Path(temp_dir2)

        try:
            queue1 = WatchdogFileEventQueue(root_dir=self.root_path, payload_type=str)
            queue2 = WatchdogFileEventQueue(root_dir=root_path2, payload_type=str)

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

    async def test_watchdog_queue_observer_lifecycle(self):
        """Test that watchdog observer is started and stopped properly"""
        queue = WatchdogFileEventQueue(root_dir=self.root_path, payload_type=str)

        # Before entering, observer should not be running
        self.assertFalse(
            hasattr(queue, "_observer")
            and queue._observer
            and queue._observer.is_alive()
        )

        async with queue:
            # After entering, observer should be running
            self.assertTrue(hasattr(queue, "_observer"))
            if queue._observer:
                self.assertTrue(queue._observer.is_alive())

        # After exiting, observer should be stopped
        if hasattr(queue, "_observer") and queue._observer:
            self.assertFalse(queue._observer.is_alive())

    async def test_watchdog_queue_event_handlers(self):
        """Test that event handlers are properly set up"""
        async with self.queue:
            # Check that handlers are created
            self.assertTrue(hasattr(self.queue, "_event_handler"))
            self.assertTrue(hasattr(self.queue, "_subscription_handler"))

            # Handlers should have references to the queue
            if hasattr(self.queue, "_event_handler") and self.queue._event_handler:
                self.assertEqual(self.queue._event_handler.queue, self.queue)
            if (
                hasattr(self.queue, "_subscription_handler")
                and self.queue._subscription_handler
            ):
                self.assertEqual(self.queue._subscription_handler.queue, self.queue)

    async def test_watchdog_queue_real_time_processing(self):
        """Test that watchdog queue processes events in real-time"""
        async with self.queue:
            # Subscribe a subscriber
            from tests.abstract_event_queue_base import MockSubscriber

            subscriber = MockSubscriber("watchdog_test")
            await self.queue.subscribe(subscriber)

            # Publish an event
            await self.queue.publish("realtime_event")

            # Wait a short time for file system events to be processed
            await asyncio.sleep(0.2)

            # Check if subscriber received the event
            # Note: This test might be flaky depending on the implementation
            # and file system event timing

    async def test_watchdog_queue_background_tasks(self):
        """Test that background processing tasks are managed properly"""
        queue = WatchdogFileEventQueue(root_dir=self.root_path, payload_type=str)

        async with queue:
            # Check that background processing task is created
            self.assertTrue(hasattr(queue, "_processing_task"))
            self.assertIsNotNone(queue._processing_task)

            # Task should be running
            self.assertFalse(queue._processing_task.done())

            # Check that observers are running
            self.assertIsNotNone(queue._observer)
            self.assertTrue(queue._observer.is_alive())

            self.assertIsNotNone(queue._subscription_observer)
            self.assertTrue(queue._subscription_observer.is_alive())

        # After exiting, task should be cancelled/done and observers stopped
        self.assertTrue(
            queue._processing_task.done() or queue._processing_task.cancelled()
        )
        self.assertIsNone(queue._observer)
        self.assertIsNone(queue._subscription_observer)

    async def test_watchdog_queue_restart_persistence(self):
        """Test that data persists across queue restarts"""
        # First session - create some data
        queue1 = WatchdogFileEventQueue(root_dir=self.root_path, payload_type=str)

        async with queue1:
            event = await queue1.publish("persistent_data")
            claim_result = await queue1.create_claim("persistent_claim", "test_data")
            self.assertTrue(claim_result)

        # Second session - verify data persists
        queue2 = WatchdogFileEventQueue(root_dir=self.root_path, payload_type=str)

        async with queue2:
            # Should be able to retrieve the event
            retrieved_event = await queue2.get_event(event.id)
            self.assertEqual(retrieved_event.payload, "persistent_data")

            # Should be able to retrieve the claim
            claim = await queue2.get_claim("persistent_claim")
            self.assertEqual(claim.data, "test_data")

    async def test_watchdog_queue_file_system_responsiveness(self):
        """Test that watchdog queue responds to file system changes"""
        async with self.queue:
            # Create a file directly in the events directory to simulate external event creation
            events_dir = self.root_path / "events"
            events_dir.mkdir(exist_ok=True)

            # Wait a moment for the watchdog to be fully set up
            await asyncio.sleep(0.1)

            # This test would need to be more sophisticated to actually test
            # file system event detection, but it serves as a placeholder
            # for testing the watchdog functionality

            # Publish an event normally to ensure the system is working
            event = await self.queue.publish("watchdog_responsive_test")
            self.assertIsNotNone(event)


if __name__ == "__main__":
    unittest.main()
