"""
Unit tests for WorkerMatchSubscriber.

This module tests the WorkerMatchSubscriber functionality, including:
- Successful event processing when worker ID matches
- SkipException raised when worker ID doesn't match
- EventResult generation with appropriate success/failure status
- Integration with different worker IDs
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4, UUID

from eventy.event_queue import EventQueue
from eventy.eventy_error import SkipException
from eventy.queue_event import QueueEvent
from eventy.subscribers.worker_match_subscriber import WorkerMatchSubscriber
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

    def get_worker_id(self):
        return self._worker_id

    def get_payload_type(self):
        return str

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

    async def create_claim(self, claim_id: str, data: str | None = None) -> bool:
        pass


@pytest.fixture
def mock_subscriber():
    """Create a mock subscriber for testing"""
    return MockSubscriber("test_subscriber")


@pytest.fixture
def worker_id():
    """Create a consistent worker ID for testing"""
    return uuid4()


@pytest.fixture
def different_worker_id():
    """Create a different worker ID for testing"""
    return uuid4()


@pytest.fixture
def mock_event_queue(worker_id):
    """Create a mock event queue with a specific worker ID"""
    return MockEventQueue(worker_id=worker_id)


@pytest.fixture
def different_worker_event_queue(different_worker_id):
    """Create a mock event queue with a different worker ID"""
    return MockEventQueue(worker_id=different_worker_id)


@pytest.fixture
def sample_event():
    """Create a sample queue event for testing"""
    return QueueEvent(id=1, payload="test_payload")


@pytest.fixture
def worker_match_subscriber(mock_subscriber, worker_id):
    """Create a WorkerMatchSubscriber with a mock subscriber and worker ID"""
    return WorkerMatchSubscriber(subscriber=mock_subscriber, worker_id=worker_id)


class TestWorkerMatchSubscriber:
    """Test cases for WorkerMatchSubscriber"""

    @pytest.mark.asyncio
    async def test_successful_event_processing_when_worker_id_matches(
        self, worker_match_subscriber, sample_event, mock_event_queue, mock_subscriber
    ):
        """Test that event is processed successfully when worker ID matches"""
        # Act
        await worker_match_subscriber.on_event(sample_event, mock_event_queue)

        # Assert
        assert (
            len(mock_subscriber.received_events) == 1
        ), "Subscriber should receive the event"
        assert (
            mock_subscriber.received_events[0] == sample_event
        ), "Subscriber should receive the correct event"
        assert mock_subscriber.call_count == 1, "Subscriber should be called once"

    @pytest.mark.asyncio
    async def test_skip_exception_when_worker_id_does_not_match(
        self,
        worker_match_subscriber,
        sample_event,
        different_worker_event_queue,
        mock_subscriber,
    ):
        """Test that SkipException is raised when worker ID doesn't match"""
        # Act & Assert
        with pytest.raises(SkipException):
            await worker_match_subscriber.on_event(
                sample_event, different_worker_event_queue
            )

        # Verify the underlying subscriber was not called
        assert (
            len(mock_subscriber.received_events) == 0
        ), "Subscriber should not receive the event"
        assert mock_subscriber.call_count == 0, "Subscriber should not be called"

    @pytest.mark.asyncio
    async def test_multiple_events_with_matching_worker_id(
        self, worker_match_subscriber, mock_event_queue, mock_subscriber
    ):
        """Test that multiple events are processed when worker ID matches"""
        # Arrange
        event1 = QueueEvent(id=1, payload="payload1")
        event2 = QueueEvent(id=2, payload="payload2")
        event3 = QueueEvent(id=3, payload="payload3")

        # Act
        await worker_match_subscriber.on_event(event1, mock_event_queue)
        await worker_match_subscriber.on_event(event2, mock_event_queue)
        await worker_match_subscriber.on_event(event3, mock_event_queue)

        # Assert
        assert (
            len(mock_subscriber.received_events) == 3
        ), "All events should be processed"
        assert (
            mock_subscriber.call_count == 3
        ), "Subscriber should be called three times"
        assert mock_subscriber.received_events[0] == event1
        assert mock_subscriber.received_events[1] == event2
        assert mock_subscriber.received_events[2] == event3

    @pytest.mark.asyncio
    async def test_mixed_worker_ids_only_matching_processed(
        self, mock_subscriber, worker_id, different_worker_id
    ):
        """Test that only events from matching worker are processed"""
        # Arrange
        worker_match_subscriber = WorkerMatchSubscriber(
            subscriber=mock_subscriber, worker_id=worker_id
        )
        matching_queue = MockEventQueue(worker_id=worker_id)
        non_matching_queue = MockEventQueue(worker_id=different_worker_id)

        event1 = QueueEvent(id=1, payload="matching_event")
        event2 = QueueEvent(id=2, payload="non_matching_event")
        event3 = QueueEvent(id=3, payload="another_matching_event")

        # Act
        await worker_match_subscriber.on_event(event1, matching_queue)

        with pytest.raises(SkipException):
            await worker_match_subscriber.on_event(event2, non_matching_queue)

        await worker_match_subscriber.on_event(event3, matching_queue)

        # Assert
        assert (
            len(mock_subscriber.received_events) == 2
        ), "Only matching events should be processed"
        assert mock_subscriber.call_count == 2, "Subscriber should be called twice"
        assert mock_subscriber.received_events[0] == event1
        assert mock_subscriber.received_events[1] == event3

    @pytest.mark.asyncio
    async def test_underlying_subscriber_exception_propagates(
        self, sample_event, mock_event_queue, worker_id
    ):
        """Test that exceptions from the underlying subscriber are propagated"""

        # Arrange
        class FailingSubscriber(Subscriber[str]):
            async def on_event(self, event, event_queue):
                raise ValueError("Subscriber failed")

        failing_subscriber = FailingSubscriber()
        worker_match_subscriber = WorkerMatchSubscriber(
            subscriber=failing_subscriber, worker_id=worker_id
        )

        # Act & Assert
        with pytest.raises(ValueError, match="Subscriber failed"):
            await worker_match_subscriber.on_event(sample_event, mock_event_queue)

    @pytest.mark.asyncio
    async def test_worker_id_comparison_with_uuid_objects(self, mock_subscriber):
        """Test that worker ID comparison works correctly with UUID objects"""
        # Arrange
        worker_id = uuid4()
        same_worker_id = UUID(str(worker_id))  # Same UUID, different object
        different_worker_id = uuid4()

        worker_match_subscriber = WorkerMatchSubscriber(
            subscriber=mock_subscriber, worker_id=worker_id
        )

        matching_queue = MockEventQueue(worker_id=same_worker_id)
        non_matching_queue = MockEventQueue(worker_id=different_worker_id)

        event = QueueEvent(id=1, payload="test")

        # Act & Assert - Same UUID should work
        await worker_match_subscriber.on_event(event, matching_queue)
        assert mock_subscriber.call_count == 1, "Should process event with same UUID"

        # Different UUID should raise SkipException
        with pytest.raises(SkipException):
            await worker_match_subscriber.on_event(event, non_matching_queue)

    @pytest.mark.asyncio
    async def test_worker_match_subscriber_equality(self, mock_subscriber, worker_id):
        """Test WorkerMatchSubscriber equality behavior"""
        # Arrange
        worker_match1 = WorkerMatchSubscriber(
            subscriber=mock_subscriber, worker_id=worker_id
        )
        worker_match2 = WorkerMatchSubscriber(
            subscriber=mock_subscriber, worker_id=worker_id
        )
        different_subscriber = MockSubscriber("different")
        worker_match3 = WorkerMatchSubscriber(
            subscriber=different_subscriber, worker_id=worker_id
        )
        different_worker_id = uuid4()
        worker_match4 = WorkerMatchSubscriber(
            subscriber=mock_subscriber, worker_id=different_worker_id
        )

        # Assert
        assert (
            worker_match1 == worker_match2
        ), "WorkerMatchSubscribers with same subscriber and worker_id should be equal"
        assert (
            worker_match1 != worker_match3
        ), "WorkerMatchSubscribers with different subscribers should not be equal"
        assert (
            worker_match1 != worker_match4
        ), "WorkerMatchSubscribers with different worker_ids should not be equal"

    def test_worker_match_subscriber_dataclass_properties(
        self, mock_subscriber, worker_id
    ):
        """Test that WorkerMatchSubscriber is properly configured as a dataclass"""
        # Arrange & Act
        worker_match_subscriber = WorkerMatchSubscriber(
            subscriber=mock_subscriber, worker_id=worker_id
        )

        # Assert
        assert (
            worker_match_subscriber.subscriber == mock_subscriber
        ), "Subscriber should be set correctly"
        assert (
            worker_match_subscriber.worker_id == worker_id
        ), "Worker ID should be set correctly"
        assert hasattr(
            worker_match_subscriber, "__dataclass_fields__"
        ), "Should be a dataclass"
        assert (
            "subscriber" in worker_match_subscriber.__dataclass_fields__
        ), "Should have subscriber field"
        assert (
            "worker_id" in worker_match_subscriber.__dataclass_fields__
        ), "Should have worker_id field"

    @pytest.mark.asyncio
    async def test_worker_id_none_handling(self, mock_subscriber):
        """Test behavior when event queue returns None for worker ID"""

        # Arrange
        class NoneWorkerIdQueue(MockEventQueue):
            def get_worker_id(self):
                return None

        worker_id = uuid4()
        worker_match_subscriber = WorkerMatchSubscriber(
            subscriber=mock_subscriber, worker_id=worker_id
        )
        none_worker_queue = NoneWorkerIdQueue()
        event = QueueEvent(id=1, payload="test")

        # Act & Assert
        with pytest.raises(SkipException):
            await worker_match_subscriber.on_event(event, none_worker_queue)

        assert mock_subscriber.call_count == 0, "Subscriber should not be called"

    @pytest.mark.asyncio
    async def test_string_worker_id_comparison(self, mock_subscriber):
        """Test that worker ID comparison works with string representations"""
        # Arrange
        worker_id = uuid4()
        worker_match_subscriber = WorkerMatchSubscriber(
            subscriber=mock_subscriber, worker_id=worker_id
        )

        # Create a queue that returns string representation of UUID
        class StringWorkerIdQueue(MockEventQueue):
            def __init__(self, worker_id_str):
                self.worker_id_str = worker_id_str

            def get_worker_id(self):
                return self.worker_id_str

        string_worker_queue = StringWorkerIdQueue(str(worker_id))
        event = QueueEvent(id=1, payload="test")

        # Act & Assert - String representation should not match UUID object
        with pytest.raises(SkipException):
            await worker_match_subscriber.on_event(event, string_worker_queue)

        assert (
            mock_subscriber.call_count == 0
        ), "Subscriber should not be called with string worker ID"


class TestWorkerMatchSubscriberIntegration:
    """Integration tests for WorkerMatchSubscriber with actual EventQueue implementations"""

    @pytest.mark.asyncio
    async def test_skip_exception_creates_failed_event_result(self):
        """Test that SkipException results in EventResult with success=False"""
        # This test requires a real EventQueue implementation to verify EventResult creation
        from eventy.mem.memory_event_queue import MemoryEventQueue

        # Arrange
        mock_subscriber = MockSubscriber("integration_test")
        specific_worker_id = uuid4()
        worker_match_subscriber = WorkerMatchSubscriber(
            subscriber=mock_subscriber, worker_id=specific_worker_id
        )

        async with MemoryEventQueue(payload_type=str) as event_queue:
            # The event queue will have a different worker ID than our subscriber expects
            assert (
                event_queue.get_worker_id() != specific_worker_id
            ), "Worker IDs should be different"

            # Subscribe the worker match subscriber
            subscription = await event_queue.subscribe(worker_match_subscriber)

            # Publish an event - this should be skipped due to worker ID mismatch
            event = await event_queue.publish("test_payload")

            # Wait a bit for processing
            import asyncio

            await asyncio.sleep(0.1)

            # Get the results for this event
            results_page = await event_queue.search_results(event_id__eq=event.id)
            results = results_page.items

            # Should have one failed result due to SkipException
            assert len(results) >= 1, "Should have at least one result"
            failed_result = results[0]
            assert (
                failed_result.success is False
            ), "Processing should fail due to worker ID mismatch"
            assert (
                failed_result.event_id == event.id
            ), "Result should be for the correct event"

            # Verify the underlying subscriber was not called
            assert (
                len(mock_subscriber.received_events) == 0
            ), "Subscriber should not receive the event"
            assert mock_subscriber.call_count == 0, "Subscriber should not be called"

    @pytest.mark.asyncio
    async def test_successful_processing_with_matching_worker_id(self):
        """Test that successful processing occurs when worker IDs match"""
        from eventy.mem.memory_event_queue import MemoryEventQueue

        # Arrange
        mock_subscriber = MockSubscriber("success_test")

        async with MemoryEventQueue(payload_type=str) as event_queue:
            # Use the event queue's actual worker ID for our subscriber
            queue_worker_id = event_queue.get_worker_id()
            worker_match_subscriber = WorkerMatchSubscriber(
                subscriber=mock_subscriber, worker_id=queue_worker_id
            )

            # Subscribe the worker match subscriber
            subscription = await event_queue.subscribe(worker_match_subscriber)

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

    @pytest.mark.asyncio
    async def test_multiple_workers_with_different_worker_ids(self):
        """Test multiple WorkerMatchSubscribers with different worker IDs"""
        from eventy.mem.memory_event_queue import MemoryEventQueue

        # Arrange
        worker1_id = uuid4()
        worker2_id = uuid4()

        mock_subscriber1 = MockSubscriber("worker1")
        mock_subscriber2 = MockSubscriber("worker2")

        worker_match_subscriber1 = WorkerMatchSubscriber(
            subscriber=mock_subscriber1, worker_id=worker1_id
        )
        worker_match_subscriber2 = WorkerMatchSubscriber(
            subscriber=mock_subscriber2, worker_id=worker2_id
        )

        async with MemoryEventQueue(payload_type=str) as event_queue:
            # Subscribe both worker match subscribers
            subscription1 = await event_queue.subscribe(worker_match_subscriber1)
            subscription2 = await event_queue.subscribe(worker_match_subscriber2)

            # Publish an event
            event = await event_queue.publish("multi_worker_payload")

            # Wait a bit for processing
            import asyncio

            await asyncio.sleep(0.1)

            # Get the results for this event
            results_page = await event_queue.search_results(event_id__eq=event.id)
            results = results_page.items

            # Both subscribers should fail due to worker ID mismatch
            # (the event queue has its own worker ID, different from both subscribers)
            assert (
                len(results) == 2
            ), "Should have two results (one for each subscriber)"

            for result in results:
                assert (
                    result.success is False
                ), "Both results should fail due to worker ID mismatch"
                assert (
                    result.event_id == event.id
                ), "Results should be for the correct event"

            # Verify neither underlying subscriber was called
            assert (
                len(mock_subscriber1.received_events) == 0
            ), "First subscriber should not receive the event"
            assert (
                len(mock_subscriber2.received_events) == 0
            ), "Second subscriber should not receive the event"
            assert (
                mock_subscriber1.call_count == 0
            ), "First subscriber should not be called"
            assert (
                mock_subscriber2.call_count == 0
            ), "Second subscriber should not be called"
