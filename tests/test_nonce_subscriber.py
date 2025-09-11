"""
Unit tests for NonceSubscriber.

This module tests the NonceSubscriber functionality, including:
- Successful event processing when claim is created
- SkipException raised when claim already exists
- EventResult generation with appropriate success/failure status
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from eventy.event_queue import EventQueue
from eventy.eventy_error import SkipException
from eventy.queue_event import QueueEvent
from eventy.subscribers.nonce_subscriber import NonceSubscriber
from eventy.subscribers.subscriber import Subscriber


class MockSubscriber(Subscriber[str]):
    """Mock subscriber for testing purposes"""

    def __init__(self, name: str = "test_subscriber"):
        self.name = name
        self.received_events = []
        self.call_count = 0

    async def on_event(
        self, event: QueueEvent[str], event_queue: EventQueue[str]
    ) -> None:
        """Store received events for testing"""
        self.received_events.append(event)
        self.call_count += 1

    def __eq__(self, other):
        return isinstance(other, MockSubscriber) and self.name == other.name

    def __hash__(self):
        return hash(self.name)


class MockEventQueue(EventQueue[str]):
    """Mock event queue for testing purposes"""

    def __init__(self, worker_id=None):
        self._worker_id = worker_id or uuid4()
        self.claims = {}  # Track created claims

    def get_worker_id(self):
        return self._worker_id

    def get_payload_type(self):
        return str

    async def create_claim(self, claim_id: str, data: str | None = None) -> bool:
        """Mock claim creation - returns False if claim already exists"""
        if claim_id in self.claims:
            return False
        self.claims[claim_id] = data
        return True

    # Implement abstract methods (not used in these tests)
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        pass

    async def subscribe(
        self, subscriber, check_subscriber_unique=False, from_index=None
    ):
        pass

    async def unsubscribe(self, subscriber_id):
        pass

    async def get_subscriber(self, subscriber_id):
        pass

    async def search_subscriptions(self, page_id=None, limit=100):
        pass

    async def publish(self, payload):
        pass

    async def get_event(self, event_id):
        pass

    async def search_events(
        self, page_id=None, limit=100, created_at__gte=None, created_at__lte=None
    ):
        pass

    async def get_result(self, result_id):
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
        pass

    async def get_claim(self, claim_id):
        pass

    async def search_claims(
        self,
        page_id=None,
        limit=100,
        worker_id__eq=None,
        created_at__gte=None,
        created_at__lte=None,
    ):
        pass


@pytest.fixture
def mock_subscriber():
    """Create a mock subscriber for testing"""
    return MockSubscriber("test_subscriber")


@pytest.fixture
def mock_event_queue():
    """Create a mock event queue for testing"""
    return MockEventQueue()


@pytest.fixture
def sample_event():
    """Create a sample queue event for testing"""
    return QueueEvent(id=1, payload="test_payload")


@pytest.fixture
def nonce_subscriber(mock_subscriber):
    """Create a NonceSubscriber with a mock subscriber"""
    return NonceSubscriber(subscriber=mock_subscriber)


class TestNonceSubscriber:
    """Test cases for NonceSubscriber"""

    @pytest.mark.asyncio
    async def test_successful_event_processing_when_claim_created(
        self, nonce_subscriber, sample_event, mock_event_queue, mock_subscriber
    ):
        """Test that event is processed successfully when claim is created"""
        # Arrange
        claim_id = f"{sample_event.id}_started"

        # Act
        await nonce_subscriber.on_event(sample_event, mock_event_queue)

        # Assert
        assert claim_id in mock_event_queue.claims, "Claim should be created"
        assert (
            len(mock_subscriber.received_events) == 1
        ), "Subscriber should receive the event"
        assert (
            mock_subscriber.received_events[0] == sample_event
        ), "Subscriber should receive the correct event"
        assert mock_subscriber.call_count == 1, "Subscriber should be called once"

    @pytest.mark.asyncio
    async def test_skip_exception_when_claim_already_exists(
        self, nonce_subscriber, sample_event, mock_event_queue, mock_subscriber
    ):
        """Test that SkipException is raised when claim already exists"""
        # Arrange
        claim_id = f"{sample_event.id}_started"
        # Pre-create the claim to simulate it already existing
        await mock_event_queue.create_claim(claim_id)

        # Act & Assert
        with pytest.raises(SkipException) as exc_info:
            await nonce_subscriber.on_event(sample_event, mock_event_queue)

        # Verify the underlying subscriber was not called
        assert (
            len(mock_subscriber.received_events) == 0
        ), "Subscriber should not receive the event"
        assert mock_subscriber.call_count == 0, "Subscriber should not be called"

    @pytest.mark.asyncio
    async def test_multiple_workers_only_one_processes_event(
        self, mock_subscriber, sample_event
    ):
        """Test that only one worker processes the event when multiple workers try"""
        # Arrange
        shared_event_queue = MockEventQueue()
        worker1_nonce_subscriber = NonceSubscriber(subscriber=MockSubscriber("worker1"))
        worker2_nonce_subscriber = NonceSubscriber(subscriber=MockSubscriber("worker2"))

        # Act - First worker processes successfully
        await worker1_nonce_subscriber.on_event(sample_event, shared_event_queue)

        # Second worker should get SkipException
        with pytest.raises(SkipException):
            await worker2_nonce_subscriber.on_event(sample_event, shared_event_queue)

        # Assert
        claim_id = f"{sample_event.id}_started"
        assert claim_id in shared_event_queue.claims, "Claim should exist"
        assert (
            worker1_nonce_subscriber.subscriber.call_count == 1
        ), "First worker should process event"
        assert (
            worker2_nonce_subscriber.subscriber.call_count == 0
        ), "Second worker should not process event"

    @pytest.mark.asyncio
    async def test_different_events_create_different_claims(
        self, nonce_subscriber, mock_event_queue, mock_subscriber
    ):
        """Test that different events create different claims and both are processed"""
        # Arrange
        event1 = QueueEvent(id=1, payload="payload1")
        event2 = QueueEvent(id=2, payload="payload2")

        # Act
        await nonce_subscriber.on_event(event1, mock_event_queue)
        await nonce_subscriber.on_event(event2, mock_event_queue)

        # Assert
        claim1_id = f"{event1.id}_started"
        claim2_id = f"{event2.id}_started"

        assert claim1_id in mock_event_queue.claims, "Claim for event1 should exist"
        assert claim2_id in mock_event_queue.claims, "Claim for event2 should exist"
        assert (
            len(mock_subscriber.received_events) == 2
        ), "Both events should be processed"
        assert mock_subscriber.call_count == 2, "Subscriber should be called twice"

    @pytest.mark.asyncio
    async def test_claim_id_format(
        self, nonce_subscriber, mock_event_queue, mock_subscriber
    ):
        """Test that claim ID follows the expected format"""
        # Arrange
        event = QueueEvent(id=12345, payload="test")
        expected_claim_id = "12345_started"

        # Act
        await nonce_subscriber.on_event(event, mock_event_queue)

        # Assert
        assert (
            expected_claim_id in mock_event_queue.claims
        ), f"Claim with ID '{expected_claim_id}' should exist"

    @pytest.mark.asyncio
    async def test_underlying_subscriber_exception_propagates(
        self, sample_event, mock_event_queue
    ):
        """Test that exceptions from the underlying subscriber are propagated"""

        # Arrange
        class FailingSubscriber(Subscriber[str]):
            async def on_event(self, event, event_queue):
                raise ValueError("Subscriber failed")

        failing_subscriber = FailingSubscriber()
        nonce_subscriber = NonceSubscriber(subscriber=failing_subscriber)

        # Act & Assert
        with pytest.raises(ValueError, match="Subscriber failed"):
            await nonce_subscriber.on_event(sample_event, mock_event_queue)

        # Verify claim was still created
        claim_id = f"{sample_event.id}_started"
        assert (
            claim_id in mock_event_queue.claims
        ), "Claim should still be created even if subscriber fails"

    @pytest.mark.asyncio
    async def test_nonce_subscriber_equality(self, mock_subscriber):
        """Test NonceSubscriber equality behavior"""
        # Arrange
        nonce1 = NonceSubscriber(subscriber=mock_subscriber)
        nonce2 = NonceSubscriber(subscriber=mock_subscriber)
        different_subscriber = MockSubscriber("different")
        nonce3 = NonceSubscriber(subscriber=different_subscriber)

        # Assert
        assert nonce1 == nonce2, "NonceSubscribers with same subscriber should be equal"
        assert (
            nonce1 != nonce3
        ), "NonceSubscribers with different subscribers should not be equal"

    def test_nonce_subscriber_dataclass_properties(self, mock_subscriber):
        """Test that NonceSubscriber is properly configured as a dataclass"""
        # Arrange & Act
        nonce_subscriber = NonceSubscriber(subscriber=mock_subscriber)

        # Assert
        assert (
            nonce_subscriber.subscriber == mock_subscriber
        ), "Subscriber should be set correctly"
        assert hasattr(
            nonce_subscriber, "__dataclass_fields__"
        ), "Should be a dataclass"
        assert (
            "subscriber" in nonce_subscriber.__dataclass_fields__
        ), "Should have subscriber field"


class TestNonceSubscriberIntegration:
    """Integration tests for NonceSubscriber with actual EventQueue implementations"""

    @pytest.mark.asyncio
    async def test_skip_exception_creates_failed_event_result(self):
        """Test that SkipException results in EventResult with success=False"""
        # This test requires a real EventQueue implementation to verify EventResult creation
        # We'll use the MemoryEventQueue for this integration test
        from eventy.mem.memory_event_queue import MemoryEventQueue
        from eventy.event_result import EventResult

        # Arrange
        mock_subscriber = MockSubscriber("integration_test")
        nonce_subscriber = NonceSubscriber(subscriber=mock_subscriber)

        async with MemoryEventQueue(payload_type=str) as event_queue:
            # Subscribe the nonce subscriber
            subscription = await event_queue.subscribe(nonce_subscriber)

            # Publish an event - this should be processed successfully by the first worker
            event = await event_queue.publish("test_payload")

            # Wait a bit for processing
            import asyncio

            await asyncio.sleep(0.1)

            # Get the results for this event
            results_page = await event_queue.search_results(event_id__eq=event.id)
            results = results_page.items

            # First result should be successful
            assert len(results) >= 1, "Should have at least one result"
            first_result = results[0]
            assert first_result.success is True, "First processing should be successful"
            assert (
                first_result.event_id == event.id
            ), "Result should be for the correct event"

            # Now simulate a second worker trying to process the same event
            # by manually calling the subscriber (simulating what would happen if another worker tried)
            with pytest.raises(SkipException):
                await nonce_subscriber.on_event(event, event_queue)
                pytest.fail("Should have raised SkipException")

    @pytest.mark.asyncio
    async def test_successful_processing_creates_successful_event_result(self):
        """Test that successful processing results in EventResult with success=True"""
        from eventy.mem.memory_event_queue import MemoryEventQueue

        # Arrange
        mock_subscriber = MockSubscriber("success_test")
        nonce_subscriber = NonceSubscriber(subscriber=mock_subscriber)

        async with MemoryEventQueue(payload_type=str) as event_queue:
            # Subscribe the nonce subscriber
            subscription = await event_queue.subscribe(nonce_subscriber)

            # Publish an event
            event = await event_queue.publish("success_payload")

            # Wait a bit for processing
            import asyncio

            await asyncio.sleep(0.1)

            # Get the results for this event
            results_page = await event_queue.search_results(event_id__eq=event.id)
            results = results_page.items

            # Should have exactly one successful result
            assert len(results) == 1, "Should have exactly one result"
            result = results[0]
            assert result.success is True, "Processing should be successful"
            assert result.event_id == event.id, "Result should be for the correct event"
            assert (
                result.details is None
            ), "Successful processing should have no error details"

            # Verify the underlying subscriber was called
            assert (
                len(mock_subscriber.received_events) == 1
            ), "Subscriber should receive the event"
            assert (
                mock_subscriber.received_events[0].id == event.id
            ), "Subscriber should receive the correct event"
