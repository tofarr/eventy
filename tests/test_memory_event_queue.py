"""
Test cases for MemoryEventQueue implementation.

This module contains concrete test cases for the MemoryEventQueue class
that inherit from the abstract test base.
"""

import unittest
from eventy.event_queue import EventQueue
from eventy.mem.memory_event_queue import MemoryEventQueue
from tests.abstract_event_queue_base import AbstractEventQueueTestBase


class TestMemoryEventQueue(AbstractEventQueueTestBase):
    """Test cases for MemoryEventQueue implementation"""
    
    async def create_queue(self) -> EventQueue[str]:
        """Create a MemoryEventQueue instance for testing"""
        return MemoryEventQueue(payload_type=str)
    
    # Additional tests specific to MemoryEventQueue
    
    async def test_memory_queue_isolation(self):
        """Test that different MemoryEventQueue instances are isolated"""
        queue1 = MemoryEventQueue(payload_type=str)
        queue2 = MemoryEventQueue(payload_type=str)
        
        async with queue1, queue2:
            # Publish to queue1
            event1 = await queue1.publish("queue1_event")
            
            # Publish to queue2
            event2 = await queue2.publish("queue2_event")
            
            # Each queue should only see its own events
            events1 = await queue1.search_events()
            events2 = await queue2.search_events()
            
            self.assertEqual(len(events1.items), 1)
            self.assertEqual(len(events2.items), 1)
            self.assertEqual(events1.items[0].payload, "queue1_event")
            self.assertEqual(events2.items[0].payload, "queue2_event")
    
    async def test_memory_queue_worker_id_uniqueness(self):
        """Test that different MemoryEventQueue instances have different worker IDs"""
        queue1 = MemoryEventQueue(payload_type=str)
        queue2 = MemoryEventQueue(payload_type=str)
        
        async with queue1, queue2:
            worker_id1 = queue1.get_worker_id()
            worker_id2 = queue2.get_worker_id()
            
            self.assertNotEqual(worker_id1, worker_id2)
    
    async def test_memory_queue_context_manager_requirement(self):
        """Test that MemoryEventQueue requires context manager usage"""
        queue = MemoryEventQueue(payload_type=str)
        
        # Should raise error when used without context manager
        with self.assertRaises(Exception):
            await queue.publish("test")
    
    async def test_memory_queue_multiple_enter_exit(self):
        """Test that MemoryEventQueue can be entered and exited multiple times"""
        queue = MemoryEventQueue(payload_type=str)
        
        # First usage
        async with queue:
            await queue.publish("event1")
            events = await queue.search_events()
            self.assertEqual(len(events.items), 1)
        
        # Second usage - should maintain state
        async with queue:
            events = await queue.search_events()
            self.assertEqual(len(events.items), 1)  # Should still have the event
            
            await queue.publish("event2")
            events = await queue.search_events()
            self.assertEqual(len(events.items), 2)


if __name__ == '__main__':
    unittest.main()