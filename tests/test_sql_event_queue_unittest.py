"""Tests for SQL event queue implementation using unittest."""

import os
import tempfile
import unittest
from dataclasses import dataclass

try:
    from eventy.sql.sql_event_queue import SqlEventQueue
    from eventy.sql.sql_queue_manager import SqlQueueManager

    SQL_AVAILABLE = True
except ImportError:
    SQL_AVAILABLE = False

from eventy.subscribers.subscriber import Subscriber
from tests.abstract_event_queue_base import AbstractEventQueueTestBase


@dataclass
class MockPayload:
    """Test payload for SQL event queue tests."""

    message: str
    value: int = 42


@dataclass
class AnotherPayload:
    """Another test payload for testing multiple payload types."""

    name: str
    count: int = 0


class MockSubscriber(Subscriber):
    """Simple test subscriber for testing purposes."""

    async def on_event(self, event, event_queue):
        pass


@unittest.skipIf(not SQL_AVAILABLE, "SQL dependencies not available")
class TestSqlEventQueue(AbstractEventQueueTestBase):
    """Test SQL event queue implementation."""

    def setUp(self):
        """Set up test fixtures"""
        super().setUp()
        # Create a temporary SQLite database
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.temp_db.close()
        self.database_url = f"sqlite+aiosqlite:///{self.temp_db.name}"

    def tearDown(self):
        """Clean up test fixtures"""
        super().tearDown()
        # Clean up the temporary database
        if hasattr(self, "temp_db"):
            try:
                os.unlink(self.temp_db.name)
            except FileNotFoundError:
                pass

    async def create_queue(self):
        """Create a test SQL event queue."""
        queue = SqlEventQueue(
            database_url=self.database_url,
            payload_type=str,  # The base class expects str type
        )
        return queue


@unittest.skipIf(not SQL_AVAILABLE, "SQL dependencies not available")
class TestSqlQueueManager(unittest.IsolatedAsyncioTestCase):
    """Test SQL queue manager implementation."""

    def setUp(self):
        """Set up test fixtures"""
        # Create a temporary SQLite database
        self.temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.temp_db.close()
        self.database_url = f"sqlite+aiosqlite:///{self.temp_db.name}"

    def tearDown(self):
        """Clean up test fixtures"""
        # Clean up the temporary database
        if hasattr(self, "temp_db"):
            try:
                os.unlink(self.temp_db.name)
            except FileNotFoundError:
                pass

    async def test_register_and_get_queue(self):
        """Test registering and getting queues."""
        manager = SqlQueueManager(database_url=self.database_url)

        async with manager:
            # Register a queue
            await manager.register(MockPayload)
            queue = await manager.get_event_queue(MockPayload)
            self.assertIsInstance(queue, SqlEventQueue)
            self.assertEqual(queue.payload_type, MockPayload)

            # Get the same queue again
            queue2 = await manager.get_event_queue(MockPayload)
            self.assertIs(queue, queue2)

    async def test_deregister_queue(self):
        """Test deregistering queues."""
        manager = SqlQueueManager(database_url=self.database_url)

        async with manager:
            # Register a queue
            await manager.register(MockPayload)
            queue = await manager.get_event_queue(MockPayload)
            self.assertIsInstance(queue, SqlEventQueue)

            # Deregister the queue
            await manager.deregister(MockPayload)

            # Getting the queue again should fail
            with self.assertRaises(Exception):  # Should raise EventyError
                await manager.get_event_queue(MockPayload)

    async def test_reset_queue(self):
        """Test resetting queues."""
        manager = SqlQueueManager(database_url=self.database_url)

        async with manager:
            # Register a queue and add some data
            await manager.register(MockPayload)
            queue = await manager.get_event_queue(MockPayload)

            # Add a subscriber
            subscriber = MockSubscriber()
            await queue.subscribe(subscriber)

            # Add an event
            payload = MockPayload(message="test", value=123)
            await queue.publish(payload)

            # Reset the queue
            await manager.reset(MockPayload)

            # Queue should be empty
            events_page = await queue.search_events()
            self.assertEqual(len(events_page.items), 0)

    async def test_multiple_payload_types(self):
        """Test managing multiple payload types."""
        manager = SqlQueueManager(database_url=self.database_url)

        async with manager:
            # Register queues for different payload types
            await manager.register(MockPayload)
            await manager.register(AnotherPayload)
            queue1 = await manager.get_event_queue(MockPayload)
            queue2 = await manager.get_event_queue(AnotherPayload)

            self.assertIsNot(queue1, queue2)
            self.assertEqual(queue1.payload_type, MockPayload)
            self.assertEqual(queue2.payload_type, AnotherPayload)

            # Add events to both queues
            await queue1.publish(MockPayload(message="test1"))
            await queue2.publish(AnotherPayload(name="test2"))

            # Check events are in correct queues
            events1_page = await queue1.search_events()
            events2_page = await queue2.search_events()

            self.assertEqual(len(events1_page.items), 1)
            self.assertEqual(len(events2_page.items), 1)
            self.assertEqual(events1_page.items[0].payload.message, "test1")
            self.assertEqual(events2_page.items[0].payload.name, "test2")


if __name__ == "__main__":
    unittest.main()
