"""
Unit tests for EventQueue default method implementations.

This module tests the non-abstract methods in EventQueue that provide default
implementations, which are typically overridden by concrete implementations.
"""

import pytest
from datetime import datetime
from typing import Optional
from uuid import UUID, uuid4
from unittest.mock import AsyncMock

from eventy.event_queue import EventQueue
from eventy.queue_event import QueueEvent
from eventy.event_result import EventResult
from eventy.claim import Claim
from eventy.subscription import Subscription
from eventy.subscribers.subscriber import Subscriber
from eventy.page import Page


class MockSubscriber(Subscriber[str]):
    """Mock subscriber for testing"""

    async def on_event(self, event, event_queue) -> None:
        pass


class MockEventQueue(EventQueue[str]):
    """Test implementation of EventQueue that doesn't override default methods"""

    def __init__(self):
        self.worker_id = uuid4()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        pass

    def get_worker_id(self) -> UUID:
        return self.worker_id

    def get_payload_type(self) -> type[str]:
        return str

    async def subscribe(
        self, subscriber, check_subscriber_unique=False, from_index=None
    ):
        pass

    async def unsubscribe(self, subscriber_id):
        pass

    async def get_subscriber(self, subscriber_id):
        """Abstract method implementation"""
        pass

    async def search_subscriptions(self, page_id=None, limit=100):
        """Abstract method implementation"""
        pass

    async def publish(self, payload):
        pass

    async def get_event(self, event_id):
        """Abstract method implementation"""
        pass

    async def search_events(
        self, page_id=None, limit=100, created_at__gte=None, created_at__lte=None
    ):
        """Abstract method implementation"""
        pass

    async def get_result(self, result_id):
        """Abstract method implementation"""
        pass

    async def search_results(
        self,
        page_id=None,
        limit=100,
        event_id__eq=None,
        worker_id__eq=None,
        created_at__gte=None,
        created_at__lte=None,
    ):
        """Abstract method implementation"""
        pass

    async def create_claim(self, claim_id, data=None):
        pass

    async def get_claim(self, claim_id):
        """Abstract method implementation"""
        pass

    async def search_claims(
        self,
        page_id=None,
        limit=100,
        worker_id__eq=None,
        created_at__gte=None,
        created_at__lte=None,
    ):
        """Abstract method implementation"""
        pass


class TestEventQueueDefaultMethods:
    """Test cases for EventQueue default method implementations"""

    @pytest.fixture
    def event_queue(self):
        """Create a test event queue instance"""
        return MockEventQueue()

    @pytest.mark.asyncio
    async def test_batch_get_subscriptions_success(self, event_queue):
        """Test batch_get_subscriptions with successful retrievals"""
        subscriber_id1 = uuid4()
        subscriber_id2 = uuid4()
        subscriber1 = MockSubscriber()
        subscriber2 = MockSubscriber()

        # Mock get_subscriber to return different subscribers
        async def mock_get_subscriber(sub_id):
            if sub_id == subscriber_id1:
                return subscriber1
            elif sub_id == subscriber_id2:
                return subscriber2
            raise Exception("Not found")

        # Replace the method with our mock
        event_queue.get_subscriber = mock_get_subscriber

        result = await event_queue.batch_get_subscriptions(
            [subscriber_id1, subscriber_id2]
        )

        assert len(result) == 2
        assert result[0].id == subscriber_id1
        assert result[0].subscriber == subscriber1
        assert result[1].id == subscriber_id2
        assert result[1].subscriber == subscriber2

    @pytest.mark.asyncio
    async def test_batch_get_subscriptions_with_failures(self, event_queue):
        """Test batch_get_subscriptions with some failures"""
        subscriber_id1 = uuid4()
        subscriber_id2 = uuid4()
        subscriber1 = MockSubscriber()

        # Mock get_subscriber to succeed for first, fail for second
        async def mock_get_subscriber(sub_id):
            if sub_id == subscriber_id1:
                return subscriber1
            raise Exception("Not found")

        event_queue.get_subscriber = mock_get_subscriber

        result = await event_queue.batch_get_subscriptions(
            [subscriber_id1, subscriber_id2]
        )

        assert len(result) == 2
        assert result[0].id == subscriber_id1
        assert result[0].subscriber == subscriber1
        assert result[1] is None

    @pytest.mark.asyncio
    async def test_count_subscriptions(self, event_queue):
        """Test count_subscriptions method"""
        # Mock search_subscriptions to return pages
        page1 = Page(
            items=[
                Subscription(id=uuid4(), subscriber=MockSubscriber()) for _ in range(3)
            ],
            next_page_id="page2",
        )
        page2 = Page(
            items=[
                Subscription(id=uuid4(), subscriber=MockSubscriber()) for _ in range(2)
            ],
            next_page_id=None,
        )

        async def mock_search_subscriptions(page_id=None, limit=100):
            if page_id is None:
                return page1
            elif page_id == "page2":
                return page2
            return Page(items=[], next_page_id=None)

        event_queue.search_subscriptions = mock_search_subscriptions

        count = await event_queue.count_subscriptions()
        assert count == 5

    @pytest.mark.asyncio
    async def test_batch_get_events_success(self, event_queue):
        """Test batch_get_events with successful retrievals"""
        event1 = QueueEvent(id=1, payload="test1", created_at=datetime.now())
        event2 = QueueEvent(id=2, payload="test2", created_at=datetime.now())

        async def mock_get_event(event_id):
            if event_id == 1:
                return event1
            elif event_id == 2:
                return event2
            raise Exception("Not found")

        event_queue.get_event = mock_get_event

        result = await event_queue.batch_get_events([1, 2])

        assert len(result) == 2
        assert result[0] == event1
        assert result[1] == event2

    @pytest.mark.asyncio
    async def test_batch_get_events_with_failures(self, event_queue):
        """Test batch_get_events with some failures"""
        event1 = QueueEvent(id=1, payload="test1", created_at=datetime.now())

        async def mock_get_event(event_id):
            if event_id == 1:
                return event1
            raise Exception("Not found")

        event_queue.get_event = mock_get_event

        result = await event_queue.batch_get_events([1, 2])

        assert len(result) == 2
        assert result[0] == event1
        assert result[1] is None

    @pytest.mark.asyncio
    async def test_count_claims(self, event_queue):
        """Test count_claims method (after fixing the bug)"""
        # Mock search_claims to return pages
        page1 = Page(
            items=[
                Claim(id="claim1", worker_id=uuid4(), created_at=datetime.now())
                for _ in range(3)
            ],
            next_page_id="page2",
        )
        page2 = Page(
            items=[
                Claim(id="claim2", worker_id=uuid4(), created_at=datetime.now())
                for _ in range(2)
            ],
            next_page_id=None,
        )

        async def mock_search_claims(
            page_id=None,
            limit=100,
            worker_id__eq=None,
            created_at__gte=None,
            created_at__lte=None,
        ):
            if page_id is None:
                return page1
            elif page_id == "page2":
                return page2
            return Page(items=[], next_page_id=None)

        event_queue.search_claims = mock_search_claims

        count = await event_queue.count_claims()
        assert count == 5

    @pytest.mark.asyncio
    async def test_batch_get_results_success(self, event_queue):
        """Test batch_get_results with successful retrievals"""
        result_id1 = uuid4()
        result_id2 = uuid4()
        result1 = EventResult(
            id=result_id1,
            event_id=1,
            worker_id=uuid4(),
            success=True,
            created_at=datetime.now(),
        )
        result2 = EventResult(
            id=result_id2,
            event_id=2,
            worker_id=uuid4(),
            success=True,
            created_at=datetime.now(),
        )

        async def mock_get_result(result_id):
            if result_id == result_id1:
                return result1
            elif result_id == result_id2:
                return result2
            raise Exception("Not found")

        event_queue.get_result = mock_get_result

        result = await event_queue.batch_get_results([result_id1, result_id2])

        assert len(result) == 2
        assert result[0] == result1
        assert result[1] == result2

    @pytest.mark.asyncio
    async def test_batch_get_results_with_failures(self, event_queue):
        """Test batch_get_results with some failures"""
        result_id1 = uuid4()
        result_id2 = uuid4()
        result1 = EventResult(
            id=result_id1,
            event_id=1,
            worker_id=uuid4(),
            success=True,
            created_at=datetime.now(),
        )

        async def mock_get_result(result_id):
            if result_id == result_id1:
                return result1
            raise Exception("Not found")

        event_queue.get_result = mock_get_result

        result = await event_queue.batch_get_results([result_id1, result_id2])

        assert len(result) == 2
        assert result[0] == result1
        assert result[1] is None

    @pytest.mark.asyncio
    async def test_batch_get_claims_success(self, event_queue):
        """Test batch_get_claims with successful retrievals"""
        claim1 = Claim(id="claim1", worker_id=uuid4(), created_at=datetime.now())
        claim2 = Claim(id="claim2", worker_id=uuid4(), created_at=datetime.now())

        async def mock_get_claim(claim_id):
            if claim_id == "claim1":
                return claim1
            elif claim_id == "claim2":
                return claim2
            raise Exception("Not found")

        event_queue.get_claim = mock_get_claim

        result = await event_queue.batch_get_claims(["claim1", "claim2"])

        assert len(result) == 2
        assert result[0] == claim1
        assert result[1] == claim2

    @pytest.mark.asyncio
    async def test_batch_get_claims_with_failures(self, event_queue):
        """Test batch_get_claims with some failures"""
        claim1 = Claim(id="claim1", worker_id=uuid4(), created_at=datetime.now())

        async def mock_get_claim(claim_id):
            if claim_id == "claim1":
                return claim1
            raise Exception("Not found")

        event_queue.get_claim = mock_get_claim

        result = await event_queue.batch_get_claims(["claim1", "claim2"])

        assert len(result) == 2
        assert result[0] == claim1
        assert result[1] is None

    @pytest.mark.asyncio
    async def test_count_events(self, event_queue):
        """Test count_events method"""
        # Mock search_events to return pages
        page1 = Page(
            items=[
                QueueEvent(id=i, payload=f"test{i}", created_at=datetime.now())
                for i in range(3)
            ],
            next_page_id="page2",
        )
        page2 = Page(
            items=[
                QueueEvent(id=i, payload=f"test{i}", created_at=datetime.now())
                for i in range(3, 5)
            ],
            next_page_id=None,
        )

        async def mock_search_events(
            page_id=None, limit=100, created_at__gte=None, created_at__lte=None
        ):
            if page_id is None:
                return page1
            elif page_id == "page2":
                return page2
            return Page(items=[], next_page_id=None)

        event_queue.search_events = mock_search_events

        count = await event_queue.count_events()
        assert count == 5

    @pytest.mark.asyncio
    async def test_count_results(self, event_queue):
        """Test count_results method"""
        # Mock search_results to return pages
        page1 = Page(
            items=[
                EventResult(
                    id=uuid4(),
                    event_id=i,
                    worker_id=uuid4(),
                    success=True,
                    created_at=datetime.now(),
                )
                for i in range(3)
            ],
            next_page_id="page2",
        )
        page2 = Page(
            items=[
                EventResult(
                    id=uuid4(),
                    event_id=i,
                    worker_id=uuid4(),
                    success=True,
                    created_at=datetime.now(),
                )
                for i in range(3, 5)
            ],
            next_page_id=None,
        )

        async def mock_search_results(
            page_id=None,
            limit=100,
            event_id__eq=None,
            worker_id__eq=None,
            created_at__gte=None,
            created_at__lte=None,
        ):
            if page_id is None:
                return page1
            elif page_id == "page2":
                return page2
            return Page(items=[], next_page_id=None)

        event_queue.search_results = mock_search_results

        count = await event_queue.count_results()
        assert count == 5
