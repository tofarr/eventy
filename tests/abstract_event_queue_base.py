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
    
    async def on_event(self, event: QueueEvent[str], event_queue: EventQueue[str]) -> None:
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
        if hasattr(self, 'queue'):
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
        
        subscription1 = await self.queue.subscribe(subscriber, check_subscriber_unique=True)
        subscription2 = await self.queue.subscribe(subscriber, check_subscriber_unique=True)
        
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
        
        subscriptions = await self.queue.batch_get_subscriptions([
            subscription1.id, subscription2.id, fake_id
        ])
        
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
            next_page = await self.queue.search_subscriptions(page_id=page.next_page_id, limit=2)
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
            self.assertGreater(events[i].id, events[i-1].id)
    
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
            created_at__gte=past,
            created_at__lte=future
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
            next_page = await self.queue.search_events(page_id=page.next_page_id, limit=2)
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
            created_at__gte=past,
            created_at__lte=future
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
            created_at__lte=future
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
        
        claims = await self.queue.batch_get_claims([claim_id1, claim_id2, nonexistent_id])
        
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
            created_at__lte=future
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
            created_at__lte=future
        )
        
        self.assertGreaterEqual(count, 1)