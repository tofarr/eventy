import asyncio
import pytest
from datetime import datetime, UTC, timedelta
from dataclasses import dataclass
from uuid import UUID
from unittest.mock import AsyncMock, Mock

from eventy.mem.memory_event_queue import MemoryEventQueue, StoredEvent
from eventy.event_status import EventStatus
from eventy.queue_event import QueueEvent
from eventy.subscriber import Subscriber
from eventy.serializers.pickle_serializer import PickleSerializer
from eventy.serializers.json_serializer import JsonSerializer


@dataclass
class PayloadForTesting:
    """Test payload class for testing"""
    message: str
    value: int


class SubscriberForTesting(Subscriber[PayloadForTesting]):
    """Test subscriber implementation"""
    
    def __init__(self):
        self.payload_type = PayloadForTesting
        self.received_events = []
        self.should_raise = False
        
    async def on_event(self, event: QueueEvent[PayloadForTesting]) -> None:
        if self.should_raise:
            raise ValueError("Test error")
        self.received_events.append(event)


class TestMemoryEventQueue:
    """Test suite for MemoryEventQueue"""

    @pytest.fixture
    def queue(self):
        """Create a fresh MemoryEventQueue for testing"""
        return MemoryEventQueue(event_type=PayloadForTesting)

    @pytest.fixture
    def payload(self):
        """Create a test payload"""
        return PayloadForTesting(message="test message", value=42)

    @pytest.fixture
    def subscriber(self):
        """Create a test subscriber"""
        return SubscriberForTesting()

    def test_initialization_default_serializer(self):
        """Test MemoryEventQueue initialization with default serializer"""
        queue = MemoryEventQueue(event_type=PayloadForTesting)
        
        assert queue.event_type == PayloadForTesting
        assert isinstance(queue.serializer, PickleSerializer)
        assert queue.events == []
        assert queue.subscribers == {}
        assert isinstance(queue.lock, asyncio.Lock)

    def test_initialization_custom_serializer(self):
        """Test MemoryEventQueue initialization with custom serializer"""
        custom_serializer = JsonSerializer()
        queue = MemoryEventQueue(event_type=PayloadForTesting, serializer=custom_serializer)
        
        assert queue.event_type == PayloadForTesting
        assert queue.serializer is custom_serializer
        assert queue.events == []
        assert queue.subscribers == {}

    def test_stored_event_creation(self):
        """Test StoredEvent creation with default values"""
        payload_bytes = b"test payload"
        stored_event = StoredEvent(serialized_payload=payload_bytes)
        
        assert stored_event.serialized_payload == payload_bytes
        assert stored_event.status == EventStatus.PROCESSING
        assert isinstance(stored_event.created_at, datetime)
        assert stored_event.created_at.tzinfo == UTC

    def test_stored_event_creation_with_custom_values(self):
        """Test StoredEvent creation with custom values"""
        payload_bytes = b"test payload"
        custom_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
        stored_event = StoredEvent(
            serialized_payload=payload_bytes,
            created_at=custom_time,
            status=EventStatus.ERROR
        )
        
        assert stored_event.serialized_payload == payload_bytes
        assert stored_event.created_at == custom_time
        assert stored_event.status == EventStatus.ERROR

    @pytest.mark.asyncio
    async def test_subscribe_single_subscriber(self, queue, subscriber):
        """Test subscribing a single subscriber"""
        subscriber_id = await queue.subscribe(subscriber)
        
        assert isinstance(subscriber_id, UUID)
        assert subscriber_id in queue.subscribers
        assert queue.subscribers[subscriber_id] is subscriber

    @pytest.mark.asyncio
    async def test_subscribe_multiple_subscribers(self, queue):
        """Test subscribing multiple subscribers"""
        subscriber1 = SubscriberForTesting()
        subscriber2 = SubscriberForTesting()
        
        id1 = await queue.subscribe(subscriber1)
        id2 = await queue.subscribe(subscriber2)
        
        assert id1 != id2
        assert len(queue.subscribers) == 2
        assert queue.subscribers[id1] is subscriber1
        assert queue.subscribers[id2] is subscriber2

    @pytest.mark.asyncio
    async def test_unsubscribe_existing_subscriber(self, queue, subscriber):
        """Test unsubscribing an existing subscriber"""
        subscriber_id = await queue.subscribe(subscriber)
        
        result = await queue.unsubscribe(subscriber_id)
        
        assert result is True
        assert subscriber_id not in queue.subscribers
        assert len(queue.subscribers) == 0

    @pytest.mark.asyncio
    async def test_unsubscribe_nonexistent_subscriber(self, queue):
        """Test unsubscribing a non-existent subscriber"""
        from uuid import uuid4
        fake_id = uuid4()
        
        result = await queue.unsubscribe(fake_id)
        
        assert result is False
        assert len(queue.subscribers) == 0

    @pytest.mark.asyncio
    async def test_list_subscribers_empty(self, queue):
        """Test listing subscribers when there are none"""
        subscribers = await queue.list_subscribers()
        
        assert isinstance(subscribers, dict)
        assert len(subscribers) == 0

    @pytest.mark.asyncio
    async def test_list_subscribers_single(self, queue, subscriber):
        """Test listing subscribers with a single subscriber"""
        subscriber_id = await queue.subscribe(subscriber)
        
        subscribers = await queue.list_subscribers()
        
        assert len(subscribers) == 1
        assert subscriber_id in subscribers
        assert subscribers[subscriber_id] is subscriber

    @pytest.mark.asyncio
    async def test_list_subscribers_multiple(self, queue):
        """Test listing subscribers with multiple subscribers"""
        subscriber1 = SubscriberForTesting()
        subscriber2 = SubscriberForTesting()
        subscriber3 = SubscriberForTesting()
        
        id1 = await queue.subscribe(subscriber1)
        id2 = await queue.subscribe(subscriber2)
        id3 = await queue.subscribe(subscriber3)
        
        subscribers = await queue.list_subscribers()
        
        assert len(subscribers) == 3
        assert id1 in subscribers
        assert id2 in subscribers
        assert id3 in subscribers
        assert subscribers[id1] is subscriber1
        assert subscribers[id2] is subscriber2
        assert subscribers[id3] is subscriber3

    @pytest.mark.asyncio
    async def test_list_subscribers_after_unsubscribe(self, queue):
        """Test listing subscribers after unsubscribing some"""
        subscriber1 = SubscriberForTesting()
        subscriber2 = SubscriberForTesting()
        subscriber3 = SubscriberForTesting()
        
        id1 = await queue.subscribe(subscriber1)
        id2 = await queue.subscribe(subscriber2)
        id3 = await queue.subscribe(subscriber3)
        
        # Unsubscribe the middle one
        await queue.unsubscribe(id2)
        
        subscribers = await queue.list_subscribers()
        
        assert len(subscribers) == 2
        assert id1 in subscribers
        assert id2 not in subscribers
        assert id3 in subscribers
        assert subscribers[id1] is subscriber1
        assert subscribers[id3] is subscriber3

    @pytest.mark.asyncio
    async def test_list_subscribers_returns_copy(self, queue, subscriber):
        """Test that list_subscribers returns a copy, not the original dict"""
        subscriber_id = await queue.subscribe(subscriber)
        
        subscribers = await queue.list_subscribers()
        
        # Modify the returned dict
        subscribers.clear()
        
        # Original should be unchanged
        original_subscribers = await queue.list_subscribers()
        assert len(original_subscribers) == 1
        assert subscriber_id in original_subscribers

    @pytest.mark.asyncio
    async def test_publish_single_event_no_subscribers(self, queue, payload):
        """Test publishing an event with no subscribers"""
        await queue.publish(payload)
        
        assert len(queue.events) == 1
        stored_event = queue.events[0]
        assert stored_event.status == EventStatus.PROCESSING
        assert isinstance(stored_event.created_at, datetime)
        
        # Verify payload can be deserialized correctly
        deserialized = queue.serializer.deserialize(stored_event.serialized_payload)
        assert deserialized.message == payload.message
        assert deserialized.value == payload.value

    @pytest.mark.asyncio
    async def test_publish_single_event_with_subscriber(self, queue, payload, subscriber):
        """Test publishing an event with a subscriber"""
        await queue.subscribe(subscriber)
        
        await queue.publish(payload)
        
        assert len(queue.events) == 1
        assert len(subscriber.received_events) == 1
        
        received_event = subscriber.received_events[0]
        assert received_event.id == 1
        assert received_event.payload.message == payload.message
        assert received_event.payload.value == payload.value
        assert received_event.status == EventStatus.PROCESSING

    @pytest.mark.asyncio
    async def test_publish_multiple_events(self, queue, subscriber):
        """Test publishing multiple events"""
        await queue.subscribe(subscriber)
        
        payload1 = PayloadForTesting("message1", 1)
        payload2 = PayloadForTesting("message2", 2)
        
        await queue.publish(payload1)
        await queue.publish(payload2)
        
        assert len(queue.events) == 2
        assert len(subscriber.received_events) == 2
        
        # Check first event
        event1 = subscriber.received_events[0]
        assert event1.id == 1
        assert event1.payload.message == "message1"
        assert event1.payload.value == 1
        
        # Check second event
        event2 = subscriber.received_events[1]
        assert event2.id == 2
        assert event2.payload.message == "message2"
        assert event2.payload.value == 2

    @pytest.mark.asyncio
    async def test_publish_with_subscriber_error(self, queue, payload):
        """Test publishing when subscriber raises an error"""
        subscriber = SubscriberForTesting()
        subscriber.should_raise = True
        await queue.subscribe(subscriber)
        
        await queue.publish(payload)
        
        assert len(queue.events) == 1
        stored_event = queue.events[0]
        assert stored_event.status == EventStatus.ERROR

    @pytest.mark.asyncio
    async def test_publish_with_multiple_subscribers_one_error(self, queue, payload):
        """Test publishing with multiple subscribers where one raises an error"""
        good_subscriber = SubscriberForTesting()
        bad_subscriber = SubscriberForTesting()
        bad_subscriber.should_raise = True
        
        await queue.subscribe(good_subscriber)
        await queue.subscribe(bad_subscriber)
        
        await queue.publish(payload)
        
        assert len(queue.events) == 1
        stored_event = queue.events[0]
        assert stored_event.status == EventStatus.ERROR
        
        # Good subscriber should still receive the event
        assert len(good_subscriber.received_events) == 1
        assert len(bad_subscriber.received_events) == 0

    @pytest.mark.asyncio
    async def test_get_event_by_id(self, queue, payload):
        """Test retrieving a specific event by ID"""
        await queue.publish(payload)
        
        event = await queue.get_event(1)
        
        assert event.id == 1
        assert event.payload.message == payload.message
        assert event.payload.value == payload.value
        assert event.status == EventStatus.PROCESSING

    @pytest.mark.asyncio
    async def test_get_event_invalid_id(self, queue):
        """Test retrieving an event with invalid ID"""
        with pytest.raises(IndexError):
            await queue.get_event(1)

    @pytest.mark.asyncio
    async def test_get_events_empty_queue(self, queue):
        """Test getting events from empty queue"""
        page = await queue.get_events()
        
        assert page.items == []
        assert page.next_page_id is None

    @pytest.mark.asyncio
    async def test_get_events_single_event(self, queue, payload):
        """Test getting events with single event"""
        await queue.publish(payload)
        
        page = await queue.get_events()
        
        assert len(page.items) == 1
        assert page.next_page_id is None
        
        event = page.items[0]
        assert event.id == 1
        assert event.payload.message == payload.message

    @pytest.mark.asyncio
    async def test_get_events_multiple_events(self, queue):
        """Test getting multiple events"""
        payloads = [PayloadForTesting(f"message{i}", i) for i in range(5)]
        
        for payload in payloads:
            await queue.publish(payload)
        
        page = await queue.get_events()
        
        assert len(page.items) == 5
        assert page.next_page_id is None
        
        # Events should be in reverse order (newest first)
        for i, event in enumerate(page.items):
            expected_id = 5 - i
            assert event.id == expected_id
            assert event.payload.message == f"message{expected_id - 1}"

    @pytest.mark.asyncio
    async def test_get_events_with_limit(self, queue):
        """Test getting events with limit"""
        payloads = [PayloadForTesting(f"message{i}", i) for i in range(5)]
        
        for payload in payloads:
            await queue.publish(payload)
        
        page = await queue.get_events(limit=3)
        
        assert len(page.items) == 3
        assert page.next_page_id == "3"
        
        # Should get the 3 newest events
        assert page.items[0].id == 5
        assert page.items[1].id == 4
        assert page.items[2].id == 3

    @pytest.mark.asyncio
    async def test_get_events_pagination(self, queue):
        """Test event pagination"""
        payloads = [PayloadForTesting(f"message{i}", i) for i in range(5)]
        
        for payload in payloads:
            await queue.publish(payload)
        
        # Get first page
        page1 = await queue.get_events(limit=2)
        assert len(page1.items) == 2
        assert page1.next_page_id == "2"
        assert page1.items[0].id == 5
        assert page1.items[1].id == 4
        
        # Get second page
        page2 = await queue.get_events(page_id=page1.next_page_id, limit=2)
        assert len(page2.items) == 2
        assert page2.next_page_id == "4"
        assert page2.items[0].id == 3
        assert page2.items[1].id == 2
        
        # Get final page
        page3 = await queue.get_events(page_id=page2.next_page_id, limit=2)
        assert len(page3.items) == 1
        assert page3.next_page_id is None
        assert page3.items[0].id == 1

    @pytest.mark.asyncio
    async def test_get_events_filter_by_status(self, queue):
        """Test filtering events by status"""
        # Create events with different statuses
        good_subscriber = SubscriberForTesting()
        bad_subscriber = SubscriberForTesting()
        bad_subscriber.should_raise = True
        
        await queue.subscribe(good_subscriber)
        
        # This will have PROCESSING status
        await queue.publish(PayloadForTesting("good", 1))
        
        await queue.subscribe(bad_subscriber)
        
        # This will have ERROR status
        await queue.publish(PayloadForTesting("bad", 2))
        
        # Filter for PROCESSING events
        processing_page = await queue.get_events(status__eq=EventStatus.PROCESSING)
        assert len(processing_page.items) == 1
        assert processing_page.items[0].payload.message == "good"
        
        # Filter for ERROR events
        error_page = await queue.get_events(status__eq=EventStatus.ERROR)
        assert len(error_page.items) == 1
        assert error_page.items[0].payload.message == "bad"

    @pytest.mark.asyncio
    async def test_get_events_filter_by_created_at(self, queue):
        """Test filtering events by creation time"""
        base_time = datetime.now(UTC)
        
        # Manually create events with specific timestamps
        payload1 = PayloadForTesting("old", 1)
        payload2 = PayloadForTesting("new", 2)
        
        await queue.publish(payload1)
        await queue.publish(payload2)
        
        # Modify timestamps manually for testing
        queue.events[0].created_at = base_time - timedelta(hours=2)
        queue.events[1].created_at = base_time - timedelta(hours=1)
        
        # Filter events created after 1.5 hours ago
        min_time = base_time - timedelta(hours=1, minutes=30)
        page = await queue.get_events(created_at__min=min_time)
        
        assert len(page.items) == 1
        assert page.items[0].payload.message == "new"
        
        # Filter events created before 1.5 hours ago
        max_time = base_time - timedelta(hours=1, minutes=30)
        page = await queue.get_events(created_at__max=max_time)
        
        assert len(page.items) == 1
        assert page.items[0].payload.message == "old"

    @pytest.mark.asyncio
    async def test_count_events_empty_queue(self, queue):
        """Test counting events in empty queue"""
        count = await queue.count_events()
        assert count == 0

    @pytest.mark.asyncio
    async def test_count_events_with_events(self, queue):
        """Test counting events with multiple events"""
        payloads = [PayloadForTesting(f"message{i}", i) for i in range(3)]
        
        for payload in payloads:
            await queue.publish(payload)
        
        count = await queue.count_events()
        assert count == 3

    @pytest.mark.asyncio
    async def test_count_events_with_status_filter(self, queue):
        """Test counting events with status filter"""
        good_subscriber = SubscriberForTesting()
        bad_subscriber = SubscriberForTesting()
        bad_subscriber.should_raise = True
        
        await queue.subscribe(good_subscriber)
        await queue.publish(PayloadForTesting("good", 1))
        
        await queue.subscribe(bad_subscriber)
        await queue.publish(PayloadForTesting("bad", 2))
        
        processing_count = await queue.count_events(status__eq=EventStatus.PROCESSING)
        assert processing_count == 1
        
        error_count = await queue.count_events(status__eq=EventStatus.ERROR)
        assert error_count == 1

    @pytest.mark.asyncio
    async def test_count_events_with_time_filter(self, queue):
        """Test counting events with time filter"""
        base_time = datetime.now(UTC)
        
        await queue.publish(PayloadForTesting("old", 1))
        await queue.publish(PayloadForTesting("new", 2))
        
        # Modify timestamps
        queue.events[0].created_at = base_time - timedelta(hours=2)
        queue.events[1].created_at = base_time - timedelta(hours=1)
        
        # Count events created after 1.5 hours ago
        min_time = base_time - timedelta(hours=1, minutes=30)
        count = await queue.count_events(created_at__min=min_time)
        assert count == 1
        
        # Count events created before 1.5 hours ago
        max_time = base_time - timedelta(hours=1, minutes=30)
        count = await queue.count_events(created_at__max=max_time)
        assert count == 1

    @pytest.mark.asyncio
    async def test_reconstruct_event(self, queue, payload):
        """Test internal _reconstruct_event method"""
        serialized = queue.serializer.serialize(payload)
        stored_event = StoredEvent(
            serialized_payload=serialized,
            status=EventStatus.PROCESSED
        )
        
        reconstructed = queue._reconstruct_event(42, stored_event)
        
        assert reconstructed.id == 42
        assert reconstructed.status == EventStatus.PROCESSED
        assert reconstructed.payload.message == payload.message
        assert reconstructed.payload.value == payload.value
        assert reconstructed.created_at == stored_event.created_at

    @pytest.mark.asyncio
    async def test_concurrent_publish(self, queue):
        """Test concurrent publishing of events"""
        subscriber = SubscriberForTesting()
        await queue.subscribe(subscriber)
        
        # Create multiple concurrent publish tasks
        payloads = [PayloadForTesting(f"concurrent{i}", i) for i in range(10)]
        tasks = [queue.publish(payload) for payload in payloads]
        
        await asyncio.gather(*tasks)
        
        assert len(queue.events) == 10
        assert len(subscriber.received_events) == 10
        
        # Verify all events were stored correctly
        received_messages = {event.payload.message for event in subscriber.received_events}
        expected_messages = {f"concurrent{i}" for i in range(10)}
        assert received_messages == expected_messages

    @pytest.mark.asyncio
    async def test_concurrent_subscribe_unsubscribe(self, queue):
        """Test concurrent subscribe/unsubscribe operations"""
        subscribers = [SubscriberForTesting() for _ in range(5)]
        
        # Subscribe all concurrently
        subscribe_tasks = [queue.subscribe(sub) for sub in subscribers]
        subscriber_ids = await asyncio.gather(*subscribe_tasks)
        
        assert len(queue.subscribers) == 5
        assert len(set(subscriber_ids)) == 5  # All IDs should be unique
        
        # Unsubscribe all concurrently
        unsubscribe_tasks = [queue.unsubscribe(sid) for sid in subscriber_ids]
        results = await asyncio.gather(*unsubscribe_tasks)
        
        assert all(results)  # All should return True
        assert len(queue.subscribers) == 0

    @pytest.mark.asyncio
    async def test_get_events_with_corrupted_data(self, queue):
        """Test get_events handles corrupted serialized data gracefully"""
        # Add a valid event first
        await queue.publish(PayloadForTesting("valid", 1))
        
        # Manually add corrupted data
        corrupted_event = StoredEvent(serialized_payload=b"corrupted data")
        queue.events.append(corrupted_event)
        
        # Add another valid event
        await queue.publish(PayloadForTesting("valid2", 2))
        
        page = await queue.get_events()
        
        # Should only return the valid events, skipping corrupted one
        assert len(page.items) == 2
        assert page.items[0].payload.message == "valid2"
        assert page.items[1].payload.message == "valid"

    @pytest.mark.asyncio
    async def test_invalid_page_id(self, queue):
        """Test get_events with invalid page_id"""
        await queue.publish(PayloadForTesting("test", 1))
        
        # Test with invalid page_id - should default to 0
        page = await queue.get_events(page_id="invalid")
        
        assert len(page.items) == 1
        assert page.items[0].payload.message == "test"

    @pytest.mark.asyncio
    async def test_serialization_with_json_serializer(self):
        """Test MemoryEventQueue with JsonSerializer"""
        json_serializer = JsonSerializer()
        queue = MemoryEventQueue(event_type=dict, serializer=json_serializer)
        
        payload = {"message": "test", "value": 42}
        await queue.publish(payload)
        
        assert len(queue.events) == 1
        
        # Retrieve and verify
        event = await queue.get_event(1)
        assert event.payload == payload

    @pytest.mark.asyncio
    async def test_edge_case_zero_limit(self, queue):
        """Test get_events with zero limit (defaults to 100)"""
        await queue.publish(PayloadForTesting("test", 1))
        
        page = await queue.get_events(limit=0)
        
        # Zero limit defaults to 100, so should return the event
        assert len(page.items) == 1
        assert page.next_page_id is None

    @pytest.mark.asyncio
    async def test_edge_case_negative_limit(self, queue):
        """Test get_events with negative limit"""
        payloads = [PayloadForTesting(f"message{i}", i) for i in range(3)]
        for payload in payloads:
            await queue.publish(payload)
        
        page = await queue.get_events(limit=-1)
        
        # Negative limit is used as-is, so start_index + (-1) = start_index - 1
        # This results in slicing from 0 to -1, which gives all but the last element
        assert len(page.items) == 2

    @pytest.mark.asyncio
    async def test_large_page_id(self, queue):
        """Test get_events with page_id larger than available events"""
        await queue.publish(PayloadForTesting("test", 1))
        
        page = await queue.get_events(page_id="1000")
        
        assert len(page.items) == 0
        assert page.next_page_id is None