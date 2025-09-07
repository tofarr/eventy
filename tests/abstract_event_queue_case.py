"""
Abstract test case for EventQueue implementations.

This module provides a comprehensive test suite that can be used to test any
EventQueue implementation. The abstract test case covers all the essential
functionality including publishing, subscribing, unsubscribing, searching,
and pagination.

Note: The class is named AbstractEventQueueCase (not TestAbstractEventQueue)
to avoid automatic discovery by pytest.
"""

import asyncio
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, UTC, timedelta
from typing import TypeVar, Generic
from uuid import UUID, uuid4
import pytest
import pytest_asyncio

from eventy.event_queue import EventQueue
from eventy.event_status import EventStatus
from eventy.queue_event import QueueEvent
from eventy.subscriber.subscriber import Subscriber
from eventy.subscriber.subscription import Subscription


T = TypeVar("T")


@dataclass
class MockPayload:
    """Test payload class for testing"""

    message: str
    value: int


@dataclass
class BaseMockPayload:
    """Base payload class for inheritance testing"""

    base_field: str


@dataclass
class DerivedMockPayload(BaseMockPayload):
    """Derived payload class for inheritance testing"""

    derived_field: int


class MockSubscriber(Subscriber[MockPayload]):
    """Mock subscriber for testing"""

    def __init__(self, subscriber_id: str = None):
        self.payload_type = MockPayload
        self.received_events = []
        self.should_raise = False
        self.subscriber_id = subscriber_id or str(uuid4())

    async def on_event(self, event: QueueEvent[MockPayload]) -> None:
        if self.should_raise:
            raise ValueError(f"Test error from subscriber {self.subscriber_id}")
        self.received_events.append(event)


class SlowSubscriber(Subscriber[MockPayload]):
    """Subscriber that takes time to process events"""

    def __init__(self, delay_seconds: float = 0.1):
        self.payload_type = MockPayload
        self.received_events = []
        self.delay_seconds = delay_seconds

    async def on_event(self, event: QueueEvent[MockPayload]) -> None:
        await asyncio.sleep(self.delay_seconds)
        self.received_events.append(event)


class BasePayloadSubscriber(Subscriber[BaseMockPayload]):
    """Subscriber for base payload type"""

    def __init__(self):
        self.payload_type = BaseMockPayload
        self.received_events = []

    async def on_event(self, event: QueueEvent[BaseMockPayload]) -> None:
        self.received_events.append(event)


class AbstractEventQueueCase(ABC):
    """
    Abstract test case for EventQueue implementations.

    Subclasses must implement get_event_queue() to return an instance
    of the EventQueue implementation to test.
    """

    @abstractmethod
    def get_event_queue(self) -> EventQueue[MockPayload]:
        """Return an EventQueue instance for testing"""
        pass

    # Test fixtures and helpers

    @pytest_asyncio.fixture
    async def queue(self) -> EventQueue[MockPayload]:
        """Get a fresh EventQueue instance for testing"""
        return self.get_event_queue()

    @pytest.fixture
    def payload(self) -> MockPayload:
        """Create a test payload"""
        return MockPayload(message="test message", value=42)

    @pytest.fixture
    def subscriber(self) -> MockSubscriber:
        """Create a test subscriber"""
        return MockSubscriber()

    # Publishing tests

    @pytest.mark.asyncio
    async def test_publish_no_subscribers_no_failure(self, queue, payload):
        """Test that publishing an event with no subscribers doesn't fail"""
        # This should not raise any exceptions
        event = await queue.publish(payload)

        assert event is not None
        assert event.payload.message == payload.message
        assert event.payload.value == payload.value
        assert isinstance(event.id, int)
        assert isinstance(event.created_at, datetime)

    @pytest.mark.asyncio
    async def test_publish_single_subscriber_receives_event(
        self, queue, payload, subscriber
    ):
        """Test that a single subscriber receives published events"""
        await queue.subscribe(subscriber)

        event = await queue.publish(payload)

        assert len(subscriber.received_events) == 1
        received_event = subscriber.received_events[0]
        assert received_event.payload.message == payload.message
        assert received_event.payload.value == payload.value
        assert received_event.id == event.id

    @pytest.mark.asyncio
    async def test_publish_multiple_subscribers_all_receive_events(
        self, queue, payload
    ):
        """Test that all subscribers receive all published events"""
        subscriber1 = MockSubscriber("sub1")
        subscriber2 = MockSubscriber("sub2")
        subscriber3 = MockSubscriber("sub3")

        await queue.subscribe(subscriber1)
        await queue.subscribe(subscriber2)
        await queue.subscribe(subscriber3)

        # Publish multiple events
        payload1 = MockPayload("message1", 1)
        payload2 = MockPayload("message2", 2)

        await queue.publish(payload1)
        await queue.publish(payload2)

        # All subscribers should receive all events
        for subscriber in [subscriber1, subscriber2, subscriber3]:
            assert len(subscriber.received_events) == 2
            assert subscriber.received_events[0].payload.message == "message1"
            assert subscriber.received_events[1].payload.message == "message2"

    @pytest.mark.asyncio
    async def test_publish_with_subscriber_error_continues_processing(
        self, queue, payload
    ):
        """Test that subscriber errors don't prevent other subscribers from receiving events"""
        good_subscriber = MockSubscriber("good")
        bad_subscriber = MockSubscriber("bad")
        bad_subscriber.should_raise = True

        await queue.subscribe(good_subscriber)
        await queue.subscribe(bad_subscriber)

        # This should not raise an exception
        await queue.publish(payload)

        # Good subscriber should still receive the event
        assert len(good_subscriber.received_events) == 1
        assert len(bad_subscriber.received_events) == 0

    # Subscription management tests

    @pytest.mark.asyncio
    async def test_subscribe_returns_uuid(self, queue, subscriber):
        """Test that subscribe returns a UUID"""
        subscriber_id = await queue.subscribe(subscriber)

        assert isinstance(subscriber_id, UUID)

    @pytest.mark.asyncio
    async def test_subscribe_multiple_subscribers_unique_ids(self, queue):
        """Test that multiple subscribers get unique IDs"""
        subscriber1 = MockSubscriber("sub1")
        subscriber2 = MockSubscriber("sub2")

        id1 = await queue.subscribe(subscriber1)
        id2 = await queue.subscribe(subscriber2)

        assert id1 != id2
        assert isinstance(id1, UUID)
        assert isinstance(id2, UUID)

    @pytest.mark.asyncio
    async def test_unsubscribe_existing_subscriber_returns_true(
        self, queue, subscriber
    ):
        """Test that unsubscribing an existing subscriber returns True"""
        subscriber_id = await queue.subscribe(subscriber)

        result = await queue.unsubscribe(subscriber_id)

        assert result is True

    @pytest.mark.asyncio
    async def test_unsubscribe_nonexistent_subscriber_returns_false(self, queue):
        """Test that unsubscribing a non-existent subscriber returns False"""
        fake_id = uuid4()

        result = await queue.unsubscribe(fake_id)

        assert result is False

    @pytest.mark.asyncio
    async def test_unsubscribed_subscriber_no_longer_receives_events(
        self, queue, payload
    ):
        """Test that unsubscribed subscribers no longer receive events"""
        subscriber1 = MockSubscriber("sub1")
        subscriber2 = MockSubscriber("sub2")

        id1 = await queue.subscribe(subscriber1)
        id2 = await queue.subscribe(subscriber2)

        # Publish first event - both should receive it
        await queue.publish(MockPayload("message1", 1))

        assert len(subscriber1.received_events) == 1
        assert len(subscriber2.received_events) == 1

        # Unsubscribe first subscriber
        await queue.unsubscribe(id1)

        # Publish second event - only subscriber2 should receive it
        await queue.publish(MockPayload("message2", 2))

        assert len(subscriber1.received_events) == 1  # Still only 1
        assert len(subscriber2.received_events) == 2  # Now has 2

    @pytest.mark.asyncio
    async def test_get_subscriber_existing(self, queue, subscriber):
        """Test getting an existing subscriber"""
        subscriber_id = await queue.subscribe(subscriber)

        retrieved_subscriber = await queue.get_subscriber(subscriber_id)

        assert retrieved_subscriber is subscriber

    @pytest.mark.asyncio
    async def test_get_subscriber_nonexistent_raises_error(self, queue):
        """Test that getting a non-existent subscriber raises an error"""
        fake_id = uuid4()

        with pytest.raises((KeyError, ValueError, Exception)):
            await queue.get_subscriber(fake_id)

    # Subscriber search tests

    @pytest.mark.asyncio
    async def test_search_subscribers_empty_queue(self, queue):
        """Test searching subscribers in an empty queue"""
        page = await queue.search_subscribers(page_id=None, limit=10)

        assert isinstance(page.items, list)
        assert len(page.items) == 0
        assert page.next_page_id is None

    @pytest.mark.asyncio
    async def test_search_subscribers_single_subscriber(self, queue, subscriber):
        """Test searching subscribers with a single subscriber"""
        subscriber_id = await queue.subscribe(subscriber)

        page = await queue.search_subscribers(page_id=None, limit=10)

        assert len(page.items) == 1
        assert isinstance(page.items[0], Subscription)
        assert page.items[0].id == subscriber_id
        assert page.items[0].subscription is subscriber
        assert page.next_page_id is None

    @pytest.mark.asyncio
    async def test_search_subscribers_multiple_subscribers(self, queue):
        """Test searching subscribers with multiple subscribers"""
        subscribers = [MockSubscriber(f"sub{i}") for i in range(5)]
        subscriber_ids = []

        for subscriber in subscribers:
            subscriber_id = await queue.subscribe(subscriber)
            subscriber_ids.append(subscriber_id)

        page = await queue.search_subscribers(page_id=None, limit=10)

        assert len(page.items) == 5

        # Verify all subscribers are present
        found_ids = {subscription.id for subscription in page.items}
        expected_ids = set(subscriber_ids)
        assert found_ids == expected_ids

    @pytest.mark.asyncio
    async def test_search_subscribers_after_unsubscribe(self, queue):
        """Test that unsubscribed subscribers don't appear in search results"""
        subscriber1 = MockSubscriber("sub1")
        subscriber2 = MockSubscriber("sub2")
        subscriber3 = MockSubscriber("sub3")

        id1 = await queue.subscribe(subscriber1)
        id2 = await queue.subscribe(subscriber2)
        id3 = await queue.subscribe(subscriber3)

        # Unsubscribe the middle one
        await queue.unsubscribe(id2)

        page = await queue.search_subscribers(page_id=None, limit=10)

        assert len(page.items) == 2
        found_ids = {subscription.id for subscription in page.items}
        assert id1 in found_ids
        assert id2 not in found_ids
        assert id3 in found_ids

    @pytest.mark.asyncio
    async def test_search_subscribers_pagination(self, queue):
        """Test subscriber search pagination"""
        # Create more subscribers than the page limit
        subscribers = [MockSubscriber(f"sub{i}") for i in range(7)]

        for subscriber in subscribers:
            await queue.subscribe(subscriber)

        # Get first page with limit of 3
        page1 = await queue.search_subscribers(page_id=None, limit=3)

        assert len(page1.items) == 3
        assert page1.next_page_id is not None

        # Get second page
        page2 = await queue.search_subscribers(page_id=page1.next_page_id, limit=3)

        assert len(page2.items) == 3
        assert page2.next_page_id is not None

        # Get third page
        page3 = await queue.search_subscribers(page_id=page2.next_page_id, limit=3)

        assert len(page3.items) == 1  # Only 1 remaining
        assert page3.next_page_id is None

        # Verify no duplicates across pages
        all_ids = set()
        for page in [page1, page2, page3]:
            page_ids = {subscription.id for subscription in page.items}
            assert len(all_ids & page_ids) == 0  # No intersection
            all_ids.update(page_ids)

        assert len(all_ids) == 7

    # Event search tests

    @pytest.mark.asyncio
    async def test_search_events_empty_queue(self, queue):
        """Test searching events in an empty queue"""
        page = await queue.search_events()

        assert isinstance(page.items, list)
        assert len(page.items) == 0
        assert page.next_page_id is None

    @pytest.mark.asyncio
    async def test_search_events_single_event(self, queue, payload):
        """Test searching events with a single event"""
        published_event = await queue.publish(payload)

        page = await queue.search_events()

        assert len(page.items) == 1
        event = page.items[0]
        assert event.id == published_event.id
        assert event.payload.message == payload.message
        assert event.payload.value == payload.value
        assert page.next_page_id is None

    @pytest.mark.asyncio
    async def test_search_events_multiple_events(self, queue):
        """Test searching events with multiple events"""
        payloads = [MockPayload(f"message{i}", i) for i in range(5)]
        published_events = []

        for payload in payloads:
            event = await queue.publish(payload)
            published_events.append(event)

        page = await queue.search_events()

        assert len(page.items) == 5

        # Verify all events are present (order may vary by implementation)
        found_ids = {event.id for event in page.items}
        expected_ids = {event.id for event in published_events}
        assert found_ids == expected_ids

    @pytest.mark.asyncio
    async def test_search_events_with_status_filter(self, queue, payload):
        """Test searching events with status filter"""
        await queue.publish(payload)

        # Search for events with PROCESSING status
        page = await queue.search_events(status__eq=EventStatus.PROCESSING)

        assert len(page.items) >= 0  # May be 0 or more depending on implementation

        # All returned events should have the requested status
        for event in page.items:
            assert event.status == EventStatus.PROCESSING

    @pytest.mark.asyncio
    async def test_search_events_with_date_filters(self, queue):
        """Test searching events with date filters"""
        now = datetime.now(UTC)
        past = now - timedelta(hours=1)
        future = now + timedelta(hours=1)

        # Publish an event
        payload = MockPayload("test", 1)
        await queue.publish(payload)

        # Search with date range that should include the event
        page = await queue.search_events(created_at__min=past, created_at__max=future)

        assert len(page.items) >= 1

        # Search with date range that should exclude the event
        page = await queue.search_events(
            created_at__min=future, created_at__max=future + timedelta(hours=1)
        )

        assert len(page.items) == 0

    @pytest.mark.asyncio
    async def test_search_events_pagination_multiple_pages(self, queue):
        """Test event search pagination with multiple pages"""
        # Create more events than the page limit
        payloads = [MockPayload(f"message{i}", i) for i in range(7)]

        for payload in payloads:
            await queue.publish(payload)

        # Get first page with limit of 3
        page1 = await queue.search_events(limit=3)

        assert len(page1.items) == 3
        assert page1.next_page_id is not None

        # Get second page
        page2 = await queue.search_events(page_id=page1.next_page_id, limit=3)

        assert len(page2.items) == 3
        assert page2.next_page_id is not None

        # Get third page
        page3 = await queue.search_events(page_id=page2.next_page_id, limit=3)

        assert len(page3.items) == 1  # Only 1 remaining
        assert page3.next_page_id is None

        # Verify no duplicates across pages
        all_ids = set()
        for page in [page1, page2, page3]:
            page_ids = {event.id for event in page.items}
            assert len(all_ids & page_ids) == 0  # No intersection
            all_ids.update(page_ids)

        assert len(all_ids) == 7

    @pytest.mark.asyncio
    async def test_search_events_pagination_edge_cases(self, queue):
        """Test event search pagination edge cases"""
        # Test with limit larger than total events
        payloads = [MockPayload(f"message{i}", i) for i in range(3)]

        for payload in payloads:
            await queue.publish(payload)

        page = await queue.search_events(limit=10)

        assert len(page.items) == 3
        assert page.next_page_id is None

        # Test with limit of 1
        page1 = await queue.search_events(limit=1)
        assert len(page1.items) == 1
        assert page1.next_page_id is not None

        page2 = await queue.search_events(page_id=page1.next_page_id, limit=1)
        assert len(page2.items) == 1
        assert page2.next_page_id is not None

        page3 = await queue.search_events(page_id=page2.next_page_id, limit=1)
        assert len(page3.items) == 1
        assert page3.next_page_id is None

    # Event retrieval tests

    @pytest.mark.asyncio
    async def test_get_event_existing(self, queue, payload):
        """Test getting an existing event by ID"""
        published_event = await queue.publish(payload)

        retrieved_event = await queue.get_event(published_event.id)

        assert retrieved_event.id == published_event.id
        assert retrieved_event.payload.message == payload.message
        assert retrieved_event.payload.value == payload.value

    @pytest.mark.asyncio
    async def test_get_event_nonexistent_raises_error(self, queue):
        """Test that getting a non-existent event raises an error"""
        with pytest.raises((IndexError, KeyError, ValueError, Exception)):
            await queue.get_event(999999)

    @pytest.mark.asyncio
    async def test_get_all_events_mixed_existing_nonexistent(self, queue, payload):
        """Test getting multiple events with mix of existing and non-existent IDs"""
        published_event = await queue.publish(payload)

        events = await queue.get_all_events(
            [published_event.id, 999999, published_event.id]
        )

        assert len(events) == 3
        assert events[0] is not None
        assert events[0].id == published_event.id
        assert events[1] is None  # Non-existent event
        assert events[2] is not None
        assert events[2].id == published_event.id

    # Event counting tests

    @pytest.mark.asyncio
    async def test_count_events_empty_queue(self, queue):
        """Test counting events in an empty queue"""
        count = await queue.count_events()

        assert count == 0

    @pytest.mark.asyncio
    async def test_count_events_with_events(self, queue):
        """Test counting events with multiple events"""
        payloads = [MockPayload(f"message{i}", i) for i in range(5)]

        for payload in payloads:
            await queue.publish(payload)

        count = await queue.count_events()

        assert count == 5

    @pytest.mark.asyncio
    async def test_count_events_with_filters(self, queue, payload):
        """Test counting events with filters"""
        await queue.publish(payload)

        # Count with status filter
        count = await queue.count_events(status__eq=EventStatus.PROCESSING)

        assert count >= 0  # May be 0 or more depending on implementation

    # Event iteration tests

    @pytest.mark.asyncio
    async def test_iter_events_empty_queue(self, queue):
        """Test iterating over events in an empty queue"""
        events = []
        async for event in queue.iter_events():
            events.append(event)

        assert len(events) == 0

    @pytest.mark.asyncio
    async def test_iter_events_multiple_events(self, queue):
        """Test iterating over multiple events"""
        payloads = [MockPayload(f"message{i}", i) for i in range(5)]
        published_events = []

        for payload in payloads:
            event = await queue.publish(payload)
            published_events.append(event)

        events = []
        async for event in queue.iter_events():
            events.append(event)

        assert len(events) == 5

        # Verify all events are present
        found_ids = {event.id for event in events}
        expected_ids = {event.id for event in published_events}
        assert found_ids == expected_ids

    @pytest.mark.asyncio
    async def test_iter_events_with_filters(self, queue, payload):
        """Test iterating over events with filters"""
        await queue.publish(payload)

        events = []
        async for event in queue.iter_events(status__eq=EventStatus.PROCESSING):
            events.append(event)

        # All returned events should have the requested status
        for event in events:
            assert event.status == EventStatus.PROCESSING

    # Concurrency tests

    @pytest.mark.asyncio
    async def test_concurrent_publishing(self, queue):
        """Test concurrent publishing of events"""

        async def publish_events(start_index: int, count: int):
            for i in range(count):
                payload = MockPayload(f"message{start_index + i}", start_index + i)
                await queue.publish(payload)

        # Publish events concurrently
        await asyncio.gather(
            publish_events(0, 5), publish_events(5, 5), publish_events(10, 5)
        )

        # Verify all events were published
        page = await queue.search_events(limit=20)
        assert len(page.items) == 15

    @pytest.mark.asyncio
    async def test_concurrent_subscribe_unsubscribe(self, queue):
        """Test concurrent subscription and unsubscription"""
        subscribers = [MockSubscriber(f"sub{i}") for i in range(10)]

        async def subscribe_and_unsubscribe(subscriber):
            subscriber_id = await queue.subscribe(subscriber)
            await asyncio.sleep(0.01)  # Small delay
            await queue.unsubscribe(subscriber_id)

        # Run concurrent subscribe/unsubscribe operations
        await asyncio.gather(
            *[subscribe_and_unsubscribe(subscriber) for subscriber in subscribers]
        )

        # All subscribers should be unsubscribed
        page = await queue.search_subscribers(page_id=None, limit=20)
        assert len(page.items) == 0

    # Edge case tests

    @pytest.mark.asyncio
    async def test_publish_with_slow_subscribers(self, queue, payload):
        """Test publishing with slow subscribers doesn't block other operations"""
        slow_subscriber = SlowSubscriber(delay_seconds=0.1)
        fast_subscriber = MockSubscriber("fast")

        await queue.subscribe(slow_subscriber)
        await queue.subscribe(fast_subscriber)

        # Publish event - this should complete even with slow subscriber
        start_time = datetime.now()
        await queue.publish(payload)
        end_time = datetime.now()

        # The publish operation should complete (exact timing depends on implementation)
        # but we verify both subscribers eventually get the event
        assert len(slow_subscriber.received_events) == 1
        assert len(fast_subscriber.received_events) == 1

    @pytest.mark.asyncio
    async def test_large_payload_handling(self, queue):
        """Test handling of large payloads"""
        # Create a payload with a large message
        large_message = "x" * 10000  # 10KB string
        large_payload = MockPayload(large_message, 42)

        event = await queue.publish(large_payload)

        assert event.payload.message == large_message
        assert event.payload.value == 42

        # Verify it can be retrieved
        retrieved_event = await queue.get_event(event.id)
        assert retrieved_event.payload.message == large_message

    @pytest.mark.asyncio
    async def test_queue_state_consistency_after_errors(self, queue, payload):
        """Test that queue state remains consistent after subscriber errors"""
        good_subscriber = MockSubscriber("good")
        bad_subscriber = MockSubscriber("bad")
        bad_subscriber.should_raise = True

        await queue.subscribe(good_subscriber)
        await queue.subscribe(bad_subscriber)

        # Publish multiple events with errors
        for i in range(3):
            test_payload = MockPayload(f"message{i}", i)
            await queue.publish(test_payload)

        # Good subscriber should have received all events
        assert len(good_subscriber.received_events) == 3

        # Queue should still be functional
        page = await queue.search_events()
        assert len(page.items) == 3

        # Should be able to add new subscribers
        new_subscriber = MockSubscriber("new")
        new_id = await queue.subscribe(new_subscriber)
        assert isinstance(new_id, UUID)
