"""
Abstract base test case for EventQueue implementations.

This module provides a comprehensive test suite for EventQueue implementations.
The class name is intentionally chosen to avoid automatic discovery by pytest/pylint.
"""

import asyncio
from abc import ABC, abstractmethod
from datetime import datetime, UTC, timedelta
from typing import Generic, TypeVar
from uuid import UUID, uuid4
import unittest

from eventy.claim import Claim
from eventy.event_queue import EventQueue
from eventy.event_result import EventResult
from eventy.eventy_error import EventyError
from eventy.page import Page
from eventy.queue_event import QueueEvent
from eventy.subscribers.subscriber import Subscriber
from eventy.subscription import Subscription

T = TypeVar("T")


class MockSubscriber(Subscriber[str]):
    """Mock subscriber for testing purposes"""

    payload_type = str

    def __init__(self, name: str = "test_subscriber"):
        self.name = name
        self.received_events = []

    async def on_event(
        self, event: QueueEvent[str], event_queue: EventQueue[str]
    ) -> None:
        """Store received events for testing"""
        self.received_events.append(event)

    def __eq__(self, other):
        return isinstance(other, MockSubscriber) and self.name == other.name

    def __hash__(self):
        return hash(self.name)


class AbstractEventQueueTestBase(Generic[T], unittest.IsolatedAsyncioTestCase, ABC):
    """
    Abstract base test case for EventQueue implementations.

    This class provides comprehensive tests for all EventQueue methods.
    Concrete test classes should inherit from this and implement create_queue().
    """

    @abstractmethod
    async def create_queue(self) -> EventQueue[str]:
        """Create an instance of the EventQueue implementation to test"""
        pass

    async def asyncSetUp(self):
        """Set up test fixtures"""
        self.queue = await self.create_queue()
        await self.queue.__aenter__()

    async def asyncTearDown(self):
        """Clean up test fixtures"""
        if hasattr(self, "queue"):
            await self.queue.__aexit__(None, None, None)

    # Context Manager Tests

    async def test_context_manager_enter_exit(self):
        """Test that the queue can be used as an async context manager"""
        queue = await self.create_queue()
        async with queue:
            worker_id = queue.get_worker_id()
            self.assertIsInstance(worker_id, UUID)

    # Worker ID Tests

    async def test_get_worker_id(self):
        """Test that get_worker_id returns a valid UUID"""
        worker_id = self.queue.get_worker_id()
        self.assertIsInstance(worker_id, UUID)

    async def test_worker_id_consistency(self):
        """Test that worker_id is consistent across calls"""
        worker_id1 = self.queue.get_worker_id()
        worker_id2 = self.queue.get_worker_id()
        self.assertEqual(worker_id1, worker_id2)

    # Payload Type Tests

    async def test_get_payload_type(self):
        """Test that get_payload_type returns the correct type"""
        payload_type = self.queue.get_payload_type()
        self.assertEqual(payload_type, str)

    # Subscription Tests

    async def test_subscribe_basic(self):
        """Test basic subscription functionality"""
        subscriber = MockSubscriber("test1")
        subscription = await self.queue.subscribe(subscriber)

        self.assertIsInstance(subscription, Subscription)
        self.assertIsInstance(subscription.id, UUID)
        self.assertEqual(subscription.subscriber, subscriber)

    async def test_subscribe_multiple(self):
        """Test subscribing multiple subscribers"""
        subscriber1 = MockSubscriber("test1")
        subscriber2 = MockSubscriber("test2")

        subscription1 = await self.queue.subscribe(subscriber1)
        subscription2 = await self.queue.subscribe(subscriber2)

        self.assertNotEqual(subscription1.id, subscription2.id)
        self.assertEqual(subscription1.subscriber, subscriber1)
        self.assertEqual(subscription2.subscriber, subscriber2)

    async def test_subscribe_with_from_index(self):
        """Test subscribing with from_index parameter"""
        # Publish some events first
        await self.queue.publish("event1")
        await self.queue.publish("event2")

        subscriber = MockSubscriber("test_from_index")
        subscription = await self.queue.subscribe(subscriber, from_index=1)

        self.assertIsInstance(subscription, Subscription)
        # Allow some time for event processing
        await asyncio.sleep(0.1)

    async def test_subscribe_check_unique(self):
        """Test subscribing with check_subscriber_unique=True"""
        subscriber = MockSubscriber("unique_test")

        subscription1 = await self.queue.subscribe(
            subscriber, check_subscriber_unique=True
        )
        subscription2 = await self.queue.subscribe(
            subscriber, check_subscriber_unique=True
        )

        # Behavior may vary by implementation - some may return same subscription,
        # others may create new ones
        self.assertIsInstance(subscription1, Subscription)
        self.assertIsInstance(subscription2, Subscription)

    async def test_unsubscribe(self):
        """Test unsubscribing a subscriber"""
        subscriber = MockSubscriber("test_unsubscribe")
        subscription = await self.queue.subscribe(subscriber)

        result = await self.queue.unsubscribe(subscription.id)
        self.assertTrue(result)

        # Try to unsubscribe again - should return False
        result = await self.queue.unsubscribe(subscription.id)
        self.assertFalse(result)

    async def test_unsubscribe_nonexistent(self):
        """Test unsubscribing a non-existent subscriber"""
        fake_id = uuid4()
        result = await self.queue.unsubscribe(fake_id)
        self.assertFalse(result)

    async def test_get_subscriber(self):
        """Test getting a subscriber by ID"""
        subscriber = MockSubscriber("test_get")
        subscription = await self.queue.subscribe(subscriber)

        retrieved_subscriber = await self.queue.get_subscriber(subscription.id)
        self.assertEqual(retrieved_subscriber, subscriber)

    async def test_get_subscriber_nonexistent(self):
        """Test getting a non-existent subscriber raises error"""
        fake_id = uuid4()
        with self.assertRaises(Exception):
            await self.queue.get_subscriber(fake_id)

    async def test_batch_get_subscriptions(self):
        """Test batch getting subscriptions"""
        subscriber1 = MockSubscriber("batch1")
        subscriber2 = MockSubscriber("batch2")

        subscription1 = await self.queue.subscribe(subscriber1)
        subscription2 = await self.queue.subscribe(subscriber2)
        fake_id = uuid4()

        subscriptions = await self.queue.batch_get_subscriptions(
            [subscription1.id, subscription2.id, fake_id]
        )

        self.assertEqual(len(subscriptions), 3)
        self.assertIsNotNone(subscriptions[0])
        self.assertIsNotNone(subscriptions[1])
        self.assertIsNone(subscriptions[2])

        self.assertEqual(subscriptions[0].subscriber, subscriber1)
        self.assertEqual(subscriptions[1].subscriber, subscriber2)

    async def test_search_subscriptions(self):
        """Test searching subscriptions"""
        subscriber1 = MockSubscriber("search1")
        subscriber2 = MockSubscriber("search2")

        await self.queue.subscribe(subscriber1)
        await self.queue.subscribe(subscriber2)

        page = await self.queue.search_subscriptions()

        self.assertIsInstance(page, Page)
        self.assertGreaterEqual(len(page.items), 2)
        self.assertIsInstance(page.items[0], Subscription)

    async def test_search_subscriptions_with_pagination(self):
        """Test searching subscriptions with pagination"""
        # Subscribe multiple subscribers
        for i in range(5):
            subscriber = MockSubscriber(f"paginate_{i}")
            await self.queue.subscribe(subscriber)

        page = await self.queue.search_subscriptions(limit=2)

        self.assertIsInstance(page, Page)
        self.assertLessEqual(len(page.items), 2)

        if page.next_page_id:
            next_page = await self.queue.search_subscriptions(
                page_id=page.next_page_id, limit=2
            )
            self.assertIsInstance(next_page, Page)

    async def test_count_subscriptions(self):
        """Test counting subscriptions"""
        initial_count = await self.queue.count_subscriptions()

        subscriber = MockSubscriber("count_test")
        await self.queue.subscribe(subscriber)

        new_count = await self.queue.count_subscriptions()
        self.assertEqual(new_count, initial_count + 1)

    # Event Publishing and Retrieval Tests

    async def test_publish_event(self):
        """Test publishing an event"""
        payload = "test_payload"
        event = await self.queue.publish(payload)

        self.assertIsInstance(event, QueueEvent)
        self.assertEqual(event.payload, payload)
        self.assertIsInstance(event.id, int)
        self.assertIsInstance(event.created_at, datetime)

    async def test_publish_multiple_events(self):
        """Test publishing multiple events"""
        payloads = ["event1", "event2", "event3"]
        events = []

        for payload in payloads:
            event = await self.queue.publish(payload)
            events.append(event)

        # Events should have sequential IDs
        for i in range(1, len(events)):
            self.assertGreater(events[i].id, events[i - 1].id)

    async def test_get_event(self):
        """Test getting an event by ID"""
        payload = "get_test_payload"
        published_event = await self.queue.publish(payload)

        retrieved_event = await self.queue.get_event(published_event.id)

        self.assertEqual(retrieved_event.id, published_event.id)
        self.assertEqual(retrieved_event.payload, payload)

    async def test_get_event_nonexistent(self):
        """Test getting a non-existent event raises error"""
        with self.assertRaises(Exception):
            await self.queue.get_event(99999)

    async def test_batch_get_events(self):
        """Test batch getting events"""
        event1 = await self.queue.publish("batch_event1")
        event2 = await self.queue.publish("batch_event2")

        events = await self.queue.batch_get_events([event1.id, event2.id, 99999])

        self.assertEqual(len(events), 3)
        self.assertIsNotNone(events[0])
        self.assertIsNotNone(events[1])
        self.assertIsNone(events[2])

        self.assertEqual(events[0].payload, "batch_event1")
        self.assertEqual(events[1].payload, "batch_event2")

    async def test_search_events(self):
        """Test searching events"""
        await self.queue.publish("search_event1")
        await self.queue.publish("search_event2")

        page = await self.queue.search_events()

        self.assertIsInstance(page, Page)
        self.assertGreaterEqual(len(page.items), 2)
        self.assertIsInstance(page.items[0], QueueEvent)

    async def test_search_events_with_date_filters(self):
        """Test searching events with date filters"""
        now = datetime.now(UTC)
        past = now - timedelta(hours=1)
        future = now + timedelta(hours=1)

        await self.queue.publish("date_filter_test")

        # Search with date range
        page = await self.queue.search_events(
            created_at__gte=past, created_at__lte=future
        )

        self.assertIsInstance(page, Page)
        self.assertGreaterEqual(len(page.items), 1)

    async def test_search_events_with_pagination(self):
        """Test searching events with pagination"""
        # Publish multiple events
        for i in range(5):
            await self.queue.publish(f"paginate_event_{i}")

        page = await self.queue.search_events(limit=2)

        self.assertIsInstance(page, Page)
        self.assertLessEqual(len(page.items), 2)

        if page.next_page_id:
            next_page = await self.queue.search_events(
                page_id=page.next_page_id, limit=2
            )
            self.assertIsInstance(next_page, Page)

    async def test_count_events(self):
        """Test counting events"""
        initial_count = await self.queue.count_events()

        await self.queue.publish("count_event_test")

        new_count = await self.queue.count_events()
        self.assertEqual(new_count, initial_count + 1)

    async def test_count_events_with_date_filters(self):
        """Test counting events with date filters"""
        now = datetime.now(UTC)
        past = now - timedelta(hours=1)
        future = now + timedelta(hours=1)

        await self.queue.publish("date_count_test")

        count = await self.queue.count_events(
            created_at__gte=past, created_at__lte=future
        )

        self.assertGreaterEqual(count, 1)

    # Event Result Tests

    async def test_create_and_get_result(self):
        """Test creating and retrieving event results"""
        # First publish an event
        event = await self.queue.publish("result_test")

        # Create a result (this might need to be done through the queue's internal mechanisms)
        # For now, we'll test the get_result method assuming results exist
        # This test might need adjustment based on how results are actually created
        pass

    async def test_get_result_nonexistent(self):
        """Test getting a non-existent result raises error"""
        fake_id = uuid4()
        with self.assertRaises(Exception):
            await self.queue.get_result(fake_id)

    async def test_batch_get_results(self):
        """Test batch getting results"""
        fake_id1 = uuid4()
        fake_id2 = uuid4()

        results = await self.queue.batch_get_results([fake_id1, fake_id2])

        self.assertEqual(len(results), 2)
        # Both should be None since they don't exist
        self.assertIsNone(results[0])
        self.assertIsNone(results[1])

    async def test_search_results(self):
        """Test searching results"""
        page = await self.queue.search_results()

        self.assertIsInstance(page, Page)
        # May be empty if no results exist
        for item in page.items:
            self.assertIsInstance(item, EventResult)

    async def test_search_results_with_filters(self):
        """Test searching results with filters"""
        now = datetime.now(UTC)
        past = now - timedelta(hours=1)
        future = now + timedelta(hours=1)

        page = await self.queue.search_results(
            event_id__eq=1,
            worker_id__eq=1,
            created_at__gte=past,
            created_at__lte=future,
        )

        self.assertIsInstance(page, Page)

    async def test_count_results(self):
        """Test counting results"""
        count = await self.queue.count_results()
        self.assertIsInstance(count, int)
        self.assertGreaterEqual(count, 0)

    # Claim Tests

    async def test_create_claim(self):
        """Test creating a claim"""
        claim_id = "test_claim_1"
        result = await self.queue.create_claim(claim_id)

        self.assertTrue(result)

        # Try to create the same claim again - should return False
        result = await self.queue.create_claim(claim_id)
        self.assertFalse(result)

    async def test_create_claim_with_data(self):
        """Test creating a claim with data"""
        claim_id = "test_claim_with_data"
        data = "worker_info_123"

        result = await self.queue.create_claim(claim_id, data)
        self.assertTrue(result)

    async def test_get_claim(self):
        """Test getting a claim"""
        claim_id = "test_get_claim"
        data = "test_data"

        await self.queue.create_claim(claim_id, data)
        claim = await self.queue.get_claim(claim_id)

        self.assertIsInstance(claim, Claim)
        self.assertEqual(claim.id, claim_id)
        self.assertEqual(claim.data, data)
        self.assertEqual(claim.worker_id, self.queue.get_worker_id())
        self.assertIsInstance(claim.created_at, datetime)

    async def test_get_claim_nonexistent(self):
        """Test getting a non-existent claim raises error"""
        with self.assertRaises(EventyError):
            await self.queue.get_claim("nonexistent_claim")

    async def test_batch_get_claims(self):
        """Test batch getting claims"""
        claim_id1 = "batch_claim_1"
        claim_id2 = "batch_claim_2"
        nonexistent_id = "nonexistent"

        await self.queue.create_claim(claim_id1, "data1")
        await self.queue.create_claim(claim_id2, "data2")

        claims = await self.queue.batch_get_claims(
            [claim_id1, claim_id2, nonexistent_id]
        )

        self.assertEqual(len(claims), 3)
        self.assertIsNotNone(claims[0])
        self.assertIsNotNone(claims[1])
        self.assertIsNone(claims[2])

        self.assertEqual(claims[0].id, claim_id1)
        self.assertEqual(claims[1].id, claim_id2)

    async def test_search_claims(self):
        """Test searching claims"""
        claim_id = "search_claim_test"
        await self.queue.create_claim(claim_id)

        page = await self.queue.search_claims()

        self.assertIsInstance(page, Page)
        self.assertGreaterEqual(len(page.items), 1)

        for item in page.items:
            self.assertIsInstance(item, Claim)

    async def test_search_claims_with_filters(self):
        """Test searching claims with filters"""
        now = datetime.now(UTC)
        past = now - timedelta(hours=1)
        future = now + timedelta(hours=1)

        claim_id = "filter_claim_test"
        await self.queue.create_claim(claim_id)

        page = await self.queue.search_claims(
            worker_id__eq=self.queue.get_worker_id(),
            created_at__gte=past,
            created_at__lte=future,
        )

        self.assertIsInstance(page, Page)
        self.assertGreaterEqual(len(page.items), 1)

    async def test_count_claims(self):
        """Test counting claims"""
        initial_count = await self.queue.count_claims()

        claim_id = "count_claim_test"
        await self.queue.create_claim(claim_id)

        new_count = await self.queue.count_claims()
        self.assertEqual(new_count, initial_count + 1)

    async def test_count_claims_with_filters(self):
        """Test counting claims with filters"""
        now = datetime.now(UTC)
        past = now - timedelta(hours=1)
        future = now + timedelta(hours=1)

        claim_id = "count_filter_claim_test"
        await self.queue.create_claim(claim_id)

        count = await self.queue.count_claims(
            worker_id__eq=self.queue.get_worker_id(),
            created_at__gte=past,
            created_at__lte=future,
        )

        self.assertGreaterEqual(count, 1)

    # Advanced Pagination and Filtering Tests

    async def test_search_subscriptions_second_page_comprehensive(self):
        """Test comprehensive second page searching for subscriptions"""
        # Create enough subscriptions to ensure pagination
        subscribers = []
        for i in range(10):
            subscriber = MockSubscriber(f"page_test_sub_{i:02d}")
            subscription = await self.queue.subscribe(subscriber)
            subscribers.append((subscriber, subscription))

        # Get first page with small limit
        first_page = await self.queue.search_subscriptions(limit=3)
        self.assertIsInstance(first_page, Page)
        self.assertLessEqual(len(first_page.items), 3)

        if first_page.next_page_id:
            # Get second page
            second_page = await self.queue.search_subscriptions(
                page_id=first_page.next_page_id, limit=3
            )
            self.assertIsInstance(second_page, Page)
            self.assertLessEqual(len(second_page.items), 3)

            # Verify no overlap between pages
            first_page_ids = {sub.id for sub in first_page.items}
            second_page_ids = {sub.id for sub in second_page.items}
            self.assertEqual(len(first_page_ids.intersection(second_page_ids)), 0)

            # Test third page if available
            if second_page.next_page_id:
                third_page = await self.queue.search_subscriptions(
                    page_id=second_page.next_page_id, limit=3
                )
                self.assertIsInstance(third_page, Page)
                third_page_ids = {sub.id for sub in third_page.items}
                self.assertEqual(len(first_page_ids.intersection(third_page_ids)), 0)
                self.assertEqual(len(second_page_ids.intersection(third_page_ids)), 0)

    async def test_search_events_second_page_with_filters(self):
        """Test second page searching for events with date filters"""
        now = datetime.now(UTC)
        past = now - timedelta(minutes=30)
        future = now + timedelta(minutes=30)

        # Publish multiple events
        events = []
        for i in range(8):
            event = await self.queue.publish(f"filtered_event_{i:02d}")
            events.append(event)

        # Search with date filter and pagination
        first_page = await self.queue.search_events(
            limit=3, created_at__gte=past, created_at__lte=future
        )
        self.assertIsInstance(first_page, Page)
        self.assertLessEqual(len(first_page.items), 3)

        # Verify all items match the filter
        for event in first_page.items:
            # Handle timezone-aware vs timezone-naive datetime comparison
            event_time = event.created_at
            if event_time.tzinfo is None and past.tzinfo is not None:
                event_time = event_time.replace(tzinfo=past.tzinfo)
            elif event_time.tzinfo is not None and past.tzinfo is None:
                past = past.replace(tzinfo=event_time.tzinfo)
                future = future.replace(tzinfo=event_time.tzinfo)

            self.assertGreaterEqual(event_time, past)
            self.assertLessEqual(event_time, future)

        if first_page.next_page_id:
            # Get second page with same filters
            second_page = await self.queue.search_events(
                page_id=first_page.next_page_id,
                limit=3,
                created_at__gte=past,
                created_at__lte=future,
            )
            self.assertIsInstance(second_page, Page)

            # Verify no overlap and filters still apply
            first_page_ids = {event.id for event in first_page.items}
            second_page_ids = {event.id for event in second_page.items}
            self.assertEqual(len(first_page_ids.intersection(second_page_ids)), 0)

            for event in second_page.items:
                # Handle timezone-aware vs timezone-naive datetime comparison
                event_time = event.created_at
                if event_time.tzinfo is None and past.tzinfo is not None:
                    event_time = event_time.replace(tzinfo=past.tzinfo)
                elif event_time.tzinfo is not None and past.tzinfo is None:
                    past = past.replace(tzinfo=event_time.tzinfo)
                    future = future.replace(tzinfo=event_time.tzinfo)

                self.assertGreaterEqual(event_time, past)
                self.assertLessEqual(event_time, future)

    async def test_search_events_pagination_consistency(self):
        """Test that event pagination is consistent across multiple calls"""
        # Publish events with identifiable payloads
        for i in range(12):
            await self.queue.publish(f"consistency_test_{i:03d}")

        # Get first page multiple times - should be identical
        page1_call1 = await self.queue.search_events(limit=4)
        page1_call2 = await self.queue.search_events(limit=4)

        self.assertEqual(len(page1_call1.items), len(page1_call2.items))
        self.assertEqual(page1_call1.next_page_id, page1_call2.next_page_id)

        # Compare event IDs (should be same order)
        ids1 = [event.id for event in page1_call1.items]
        ids2 = [event.id for event in page1_call2.items]
        self.assertEqual(ids1, ids2)

    async def test_search_results_second_page_with_event_filter(self):
        """Test second page searching for results with event ID filter"""
        # This test assumes results exist or can be created
        # The implementation may vary based on how results are generated
        page = await self.queue.search_results(limit=2)
        self.assertIsInstance(page, Page)

        if page.next_page_id:
            second_page = await self.queue.search_results(
                page_id=page.next_page_id, limit=2
            )
            self.assertIsInstance(second_page, Page)

            # Verify no overlap
            if page.items and second_page.items:
                first_page_ids = {result.id for result in page.items}
                second_page_ids = {result.id for result in second_page.items}
                self.assertEqual(len(first_page_ids.intersection(second_page_ids)), 0)

    async def test_search_results_with_worker_id_filter(self):
        """Test searching results with worker ID filter"""
        worker_id = self.queue.get_worker_id()

        page = await self.queue.search_results(worker_id__eq=worker_id, limit=5)
        self.assertIsInstance(page, Page)

        # Verify all results match the worker ID filter (if any exist)
        for result in page.items:
            self.assertEqual(result.worker_id, worker_id)

        # Test second page if available
        if page.next_page_id:
            second_page = await self.queue.search_results(
                page_id=page.next_page_id, worker_id__eq=worker_id, limit=5
            )
            self.assertIsInstance(second_page, Page)

            for result in second_page.items:
                self.assertEqual(result.worker_id, worker_id)

    async def test_search_claims_second_page_with_worker_filter(self):
        """Test second page searching for claims with worker ID filter"""
        worker_id = self.queue.get_worker_id()

        # Create multiple claims
        claim_ids = []
        for i in range(8):
            claim_id = f"worker_filter_claim_{i:02d}"
            await self.queue.create_claim(claim_id, f"data_{i}")
            claim_ids.append(claim_id)

        # Search with worker filter and pagination
        first_page = await self.queue.search_claims(worker_id__eq=worker_id, limit=3)
        self.assertIsInstance(first_page, Page)
        self.assertLessEqual(len(first_page.items), 3)

        # Verify all claims match the worker ID filter
        for claim in first_page.items:
            self.assertEqual(claim.worker_id, worker_id)

        if first_page.next_page_id:
            # Get second page with same filter
            second_page = await self.queue.search_claims(
                page_id=first_page.next_page_id, worker_id__eq=worker_id, limit=3
            )
            self.assertIsInstance(second_page, Page)

            # Verify no overlap and filter still applies
            first_page_ids = {claim.id for claim in first_page.items}
            second_page_ids = {claim.id for claim in second_page.items}
            self.assertEqual(len(first_page_ids.intersection(second_page_ids)), 0)

            for claim in second_page.items:
                self.assertEqual(claim.worker_id, worker_id)

    async def test_search_claims_with_date_range_pagination(self):
        """Test claims pagination with date range filters"""
        now = datetime.now(UTC)
        past = now - timedelta(minutes=15)
        future = now + timedelta(minutes=15)

        # Create claims within the date range
        for i in range(6):
            claim_id = f"date_range_claim_{i:02d}"
            await self.queue.create_claim(claim_id, f"date_data_{i}")

        # Search with date filter and pagination
        first_page = await self.queue.search_claims(
            created_at__gte=past, created_at__lte=future, limit=2
        )
        self.assertIsInstance(first_page, Page)
        self.assertLessEqual(len(first_page.items), 2)

        # Verify date filter is applied
        for claim in first_page.items:
            # Handle timezone-aware vs timezone-naive datetime comparison
            claim_time = claim.created_at
            if claim_time.tzinfo is None and past.tzinfo is not None:
                claim_time = claim_time.replace(tzinfo=past.tzinfo)
            elif claim_time.tzinfo is not None and past.tzinfo is None:
                past = past.replace(tzinfo=claim_time.tzinfo)
                future = future.replace(tzinfo=claim_time.tzinfo)

            self.assertGreaterEqual(claim_time, past)
            self.assertLessEqual(claim_time, future)

        if first_page.next_page_id:
            second_page = await self.queue.search_claims(
                page_id=first_page.next_page_id,
                created_at__gte=past,
                created_at__lte=future,
                limit=2,
            )
            self.assertIsInstance(second_page, Page)

            # Verify date filter still applies on second page
            for claim in second_page.items:
                # Handle timezone-aware vs timezone-naive datetime comparison
                claim_time = claim.created_at
                if claim_time.tzinfo is None and past.tzinfo is not None:
                    claim_time = claim_time.replace(tzinfo=past.tzinfo)
                elif claim_time.tzinfo is not None and past.tzinfo is None:
                    past = past.replace(tzinfo=claim_time.tzinfo)
                    future = future.replace(tzinfo=claim_time.tzinfo)

                self.assertGreaterEqual(claim_time, past)
                self.assertLessEqual(claim_time, future)

            # Verify no overlap
            first_page_ids = {claim.id for claim in first_page.items}
            second_page_ids = {claim.id for claim in second_page.items}
            self.assertEqual(len(first_page_ids.intersection(second_page_ids)), 0)

    async def test_search_events_with_strict_date_boundaries(self):
        """Test event searching with very specific date boundaries"""
        # Create a specific time window
        base_time = datetime.now(UTC)
        start_time = base_time - timedelta(seconds=30)
        end_time = base_time + timedelta(seconds=30)

        # Publish events
        events_in_range = []
        for i in range(5):
            event = await self.queue.publish(f"boundary_test_{i}")
            events_in_range.append(event)

        # Search with tight date boundaries
        page = await self.queue.search_events(
            created_at__gte=start_time, created_at__lte=end_time, limit=2
        )
        self.assertIsInstance(page, Page)

        # All returned events should be within the boundary
        for event in page.items:
            # Handle timezone-aware vs timezone-naive datetime comparison
            event_time = event.created_at
            if event_time.tzinfo is None and start_time.tzinfo is not None:
                event_time = event_time.replace(tzinfo=start_time.tzinfo)
            elif event_time.tzinfo is not None and start_time.tzinfo is None:
                start_time = start_time.replace(tzinfo=event_time.tzinfo)
                end_time = end_time.replace(tzinfo=event_time.tzinfo)

            self.assertGreaterEqual(event_time, start_time)
            self.assertLessEqual(event_time, end_time)

        # Test pagination maintains the filter
        if page.next_page_id:
            next_page = await self.queue.search_events(
                page_id=page.next_page_id,
                created_at__gte=start_time,
                created_at__lte=end_time,
                limit=2,
            )
            for event in next_page.items:
                # Handle timezone-aware vs timezone-naive datetime comparison
                event_time = event.created_at
                if event_time.tzinfo is None and start_time.tzinfo is not None:
                    event_time = event_time.replace(tzinfo=start_time.tzinfo)
                elif event_time.tzinfo is not None and start_time.tzinfo is None:
                    start_time = start_time.replace(tzinfo=event_time.tzinfo)
                    end_time = end_time.replace(tzinfo=event_time.tzinfo)

                self.assertGreaterEqual(event_time, start_time)
                self.assertLessEqual(event_time, end_time)

    async def test_combined_filters_pagination(self):
        """Test pagination with multiple filters combined"""
        now = datetime.now(UTC)
        past = now - timedelta(minutes=10)
        future = now + timedelta(minutes=10)
        worker_id = self.queue.get_worker_id()

        # Create multiple claims for combined filter testing
        for i in range(7):
            claim_id = f"combined_filter_{i:02d}"
            await self.queue.create_claim(claim_id, f"combined_data_{i}")

        # Search with multiple filters
        page = await self.queue.search_claims(
            worker_id__eq=worker_id,
            created_at__gte=past,
            created_at__lte=future,
            limit=3,
        )
        self.assertIsInstance(page, Page)

        # Verify all filters are applied
        for claim in page.items:
            self.assertEqual(claim.worker_id, worker_id)
            # Handle timezone-aware vs timezone-naive datetime comparison
            claim_time = claim.created_at
            if claim_time.tzinfo is None and past.tzinfo is not None:
                claim_time = claim_time.replace(tzinfo=past.tzinfo)
            elif claim_time.tzinfo is not None and past.tzinfo is None:
                past = past.replace(tzinfo=claim_time.tzinfo)
                future = future.replace(tzinfo=claim_time.tzinfo)

            self.assertGreaterEqual(claim_time, past)
            self.assertLessEqual(claim_time, future)

        # Test second page maintains all filters
        if page.next_page_id:
            second_page = await self.queue.search_claims(
                page_id=page.next_page_id,
                worker_id__eq=worker_id,
                created_at__gte=past,
                created_at__lte=future,
                limit=3,
            )

            for claim in second_page.items:
                self.assertEqual(claim.worker_id, worker_id)
                # Handle timezone-aware vs timezone-naive datetime comparison
                claim_time = claim.created_at
                if claim_time.tzinfo is None and past.tzinfo is not None:
                    claim_time = claim_time.replace(tzinfo=past.tzinfo)
                elif claim_time.tzinfo is not None and past.tzinfo is None:
                    past = past.replace(tzinfo=claim_time.tzinfo)
                    future = future.replace(tzinfo=claim_time.tzinfo)

                self.assertGreaterEqual(claim_time, past)
                self.assertLessEqual(claim_time, future)

    async def test_empty_page_handling(self):
        """Test handling of empty pages in pagination"""
        # Search with filters that might return no results
        future_time = datetime.now(UTC) + timedelta(days=1)

        page = await self.queue.search_events(created_at__gte=future_time, limit=5)
        self.assertIsInstance(page, Page)
        self.assertEqual(len(page.items), 0)
        self.assertIsNone(page.next_page_id)

        # Same test for claims
        page = await self.queue.search_claims(created_at__gte=future_time, limit=5)
        self.assertIsInstance(page, Page)
        self.assertEqual(len(page.items), 0)
        self.assertIsNone(page.next_page_id)

    async def test_pagination_with_invalid_page_id(self):
        """Test pagination behavior with invalid page IDs"""
        # Try to get a page with a non-existent page ID
        try:
            page = await self.queue.search_events(page_id="invalid_page_id")
            # Some implementations might return empty page, others might raise exception
            self.assertIsInstance(page, Page)
        except Exception:
            # Exception is also acceptable behavior
            pass

        # Same test for other search methods
        try:
            page = await self.queue.search_claims(page_id="invalid_page_id")
            self.assertIsInstance(page, Page)
        except Exception:
            pass

        try:
            page = await self.queue.search_subscriptions(page_id="invalid_page_id")
            self.assertIsInstance(page, Page)
        except Exception:
            pass
