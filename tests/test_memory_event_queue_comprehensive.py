"""
Comprehensive test suite for MemoryEventQueue using the abstract test case.

This module provides concrete tests for MemoryEventQueue by extending the
AbstractEventQueueCase. It includes all the comprehensive tests defined in
the abstract case plus any MemoryEventQueue-specific tests.
"""

import pytest
from eventy.mem.memory_event_queue import MemoryEventQueue
from eventy.event_queue import EventQueue
from tests.abstract_event_queue_case import AbstractEventQueueCase, TestPayload


class TestMemoryEventQueueComprehensive(AbstractEventQueueCase):
    """
    Comprehensive test suite for MemoryEventQueue.
    
    This class extends AbstractEventQueueCase to run all the standard
    EventQueue tests against the MemoryEventQueue implementation.
    """
    
    def get_event_queue(self) -> EventQueue[TestPayload]:
        """Return a MemoryEventQueue instance for testing"""
        return MemoryEventQueue(event_type=TestPayload)
    
    # MemoryEventQueue-specific tests can be added here
    
    @pytest.mark.asyncio
    async def test_memory_queue_initialization(self):
        """Test MemoryEventQueue-specific initialization"""
        queue = self.get_event_queue()
        
        assert isinstance(queue, MemoryEventQueue)
        assert queue.event_type == TestPayload
        assert len(queue.events) == 0
        assert len(queue.subscribers) == 0
        assert queue.worker_id is not None
    
    @pytest.mark.asyncio
    async def test_memory_queue_direct_access_to_internal_state(self):
        """Test direct access to MemoryEventQueue internal state"""
        queue = self.get_event_queue()
        
        # Initially empty
        assert len(queue.events) == 0
        assert len(queue.subscribers) == 0
        
        # Add a subscriber
        from tests.abstract_event_queue_case import MockSubscriber
        subscriber = MockSubscriber()
        subscriber_id = await queue.subscribe(subscriber)
        
        # Check internal state
        assert len(queue.subscribers) == 1
        assert subscriber_id in queue.subscribers
        assert queue.subscribers[subscriber_id] is subscriber
        
        # Publish an event
        payload = TestPayload("test", 42)
        await queue.publish(payload)
        
        # Check internal state
        assert len(queue.events) == 1
        stored_event = queue.events[0]
        assert stored_event.serialized_payload is not None
        assert stored_event.created_at is not None
    
    @pytest.mark.asyncio
    async def test_memory_queue_serialization_deserialization(self):
        """Test that MemoryEventQueue properly serializes and deserializes payloads"""
        queue = self.get_event_queue()
        
        # Create a payload with specific values
        original_payload = TestPayload("test message", 123)
        
        # Publish the event
        published_event = await queue.publish(original_payload)
        
        # Retrieve the event
        retrieved_event = await queue.get_event(published_event.id)
        
        # Verify the payload was properly serialized and deserialized
        assert retrieved_event.payload.message == original_payload.message
        assert retrieved_event.payload.value == original_payload.value
        
        # Verify they are different objects (deserialized)
        assert retrieved_event.payload is not original_payload
    
    @pytest.mark.asyncio
    async def test_memory_queue_event_reconstruction(self):
        """Test MemoryEventQueue's _reconstruct_event method behavior"""
        queue = self.get_event_queue()
        
        # Publish multiple events
        payloads = [TestPayload(f"msg{i}", i) for i in range(3)]
        published_events = []
        
        for payload in payloads:
            event = await queue.publish(payload)
            published_events.append(event)
        
        # Verify each event can be reconstructed correctly
        for i, original_event in enumerate(published_events):
            reconstructed_event = await queue.get_event(original_event.id)
            
            assert reconstructed_event.id == original_event.id
            assert reconstructed_event.payload.message == payloads[i].message
            assert reconstructed_event.payload.value == payloads[i].value
            assert reconstructed_event.status == original_event.status
            assert reconstructed_event.created_at == original_event.created_at
    
    @pytest.mark.asyncio
    async def test_memory_queue_thread_safety_simulation(self):
        """Test MemoryEventQueue's lock mechanism with simulated concurrent access"""
        import asyncio
        
        queue = self.get_event_queue()
        
        # Create multiple subscribers
        from tests.abstract_event_queue_case import MockSubscriber
        subscribers = [MockSubscriber(f"sub{i}") for i in range(5)]
        
        async def subscribe_all():
            for subscriber in subscribers:
                await queue.subscribe(subscriber)
        
        async def publish_events():
            for i in range(10):
                payload = TestPayload(f"concurrent_msg{i}", i)
                await queue.publish(payload)
        
        async def search_operations():
            for _ in range(5):
                await queue.search_events(limit=5)
                await queue.search_subscribers(page_id=None, limit=5)
                await asyncio.sleep(0.001)  # Small delay
        
        # Run operations concurrently
        await asyncio.gather(
            subscribe_all(),
            publish_events(),
            search_operations()
        )
        
        # Verify final state
        assert len(queue.subscribers) == 5
        assert len(queue.events) == 10
        
        # All subscribers should have received all events
        for subscriber in subscribers:
            assert len(subscriber.received_events) == 10
    
    @pytest.mark.asyncio
    async def test_memory_queue_list_subscribers_backward_compatibility(self):
        """Test the backward compatibility list_subscribers method"""
        queue = self.get_event_queue()
        
        from tests.abstract_event_queue_case import MockSubscriber
        subscriber1 = MockSubscriber("sub1")
        subscriber2 = MockSubscriber("sub2")
        
        id1 = await queue.subscribe(subscriber1)
        id2 = await queue.subscribe(subscriber2)
        
        # Test the backward compatibility method
        subscribers_dict = await queue.list_subscribers()
        
        assert isinstance(subscribers_dict, dict)
        assert len(subscribers_dict) == 2
        assert id1 in subscribers_dict
        assert id2 in subscribers_dict
        assert subscribers_dict[id1] is subscriber1
        assert subscribers_dict[id2] is subscriber2
        
        # Verify it returns a copy
        subscribers_dict.clear()
        original_dict = await queue.list_subscribers()
        assert len(original_dict) == 2
    
    @pytest.mark.asyncio
    async def test_memory_queue_error_handling_in_serialization(self):
        """Test error handling when serialization fails"""
        from eventy.serializers.serializer import Serializer
        from typing import TypeVar
        
        T = TypeVar('T')
        
        class FailingSerializer(Serializer[TestPayload]):
            """Serializer that always fails"""
            
            def serialize(self, obj: TestPayload) -> bytes:
                raise ValueError("Serialization failed")
            
            def deserialize(self, data: bytes) -> TestPayload:
                raise ValueError("Deserialization failed")
        
        # Create queue with failing serializer
        queue = MemoryEventQueue(
            event_type=TestPayload,
            serializer=FailingSerializer()
        )
        
        payload = TestPayload("test", 42)
        
        # Publishing should fail due to serialization error
        with pytest.raises(ValueError, match="Serialization failed"):
            await queue.publish(payload)
    
    @pytest.mark.asyncio
    async def test_memory_queue_custom_serializer(self):
        """Test MemoryEventQueue with custom serializer"""
        from eventy.serializers.json_serializer import JsonSerializer
        
        # Create queue with JSON serializer
        queue = MemoryEventQueue(
            event_type=TestPayload,
            serializer=JsonSerializer()
        )
        
        payload = TestPayload("json test", 456)
        
        # Publish and retrieve event
        published_event = await queue.publish(payload)
        retrieved_event = await queue.get_event(published_event.id)
        
        # Note: JsonSerializer deserializes to dict, not the original object type
        # This is expected behavior for the basic JsonSerializer
        assert isinstance(retrieved_event.payload, dict)
        assert retrieved_event.payload["message"] == payload.message
        assert retrieved_event.payload["value"] == payload.value
        
        # Verify JSON serialization was used
        stored_event = queue.events[0]
        # JSON serialized data should be readable as text
        serialized_str = stored_event.serialized_payload.decode('utf-8')
        assert '"message"' in serialized_str
        assert '"json test"' in serialized_str