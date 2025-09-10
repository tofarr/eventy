from dataclasses import dataclass, field
from datetime import datetime, UTC
from typing import Generic, TypeVar, Optional, Dict, List
from uuid import UUID, uuid4
import asyncio

from eventy.claim import Claim
from eventy.event_queue import EventQueue
from eventy.event_result import EventResult
from eventy.eventy_error import EventyError
from eventy.page import Page
from eventy.queue_event import QueueEvent
from eventy.serializers.serializer import Serializer, get_default_serializer
from eventy.subscribers.subscriber import Subscriber
from eventy.subscription import Subscription

T = TypeVar("T")


@dataclass
class MemoryEventQueue(Generic[T], EventQueue[T]):
    """In-memory implementation of EventQueue using dataclasses"""

    payload_type: type[T]
    serializer: Serializer = field(default_factory=get_default_serializer)
    worker_id: UUID = field(default_factory=uuid4)

    # Internal storage
    _events: Dict[int, bytes] = field(default_factory=dict, init=False)
    _event_metadata: Dict[int, datetime] = field(default_factory=dict, init=False)
    _results: Dict[UUID, EventResult] = field(default_factory=dict, init=False)
    _subscriptions: Dict[UUID, Subscription[T]] = field(
        default_factory=dict, init=False
    )
    _claims: Dict[str, Claim] = field(default_factory=dict, init=False)
    _next_event_id: int = field(default=1, init=False)
    _entered: bool = field(default=False, init=False)

    def _check_entered(self) -> None:
        """Check if the queue has been entered, raise error if not"""
        if not self._entered:
            raise EventyError(
                "EventQueue must be entered using async context manager before use"
            )

    async def __aenter__(self):
        """Start this event queue"""
        self._entered = True
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Close this event queue"""
        self._entered = False
        # Clean up resources if needed
        self._events.clear()
        self._event_metadata.clear()
        self._results.clear()
        self._subscriptions.clear()
        self._claims.clear()

    def get_worker_id(self) -> UUID:
        """Get the id of the current worker"""
        return self.worker_id

    def get_payload_type(self) -> type[T]:
        """Get the type of payload handled by this queue"""
        return self.payload_type

    async def subscribe(
        self,
        subscriber: Subscriber[T],
        check_subscriber_unique: bool = False,
        from_index: int | None = None,
    ) -> Subscription[T]:
        """Add a subscriber to this queue"""
        self._check_entered()

        # Check for existing subscriber if requested
        if check_subscriber_unique:
            for subscription in self._subscriptions.values():
                if subscription.subscriber == subscriber:
                    return subscription

        # Create new subscription
        subscription_id = uuid4()
        subscription = Subscription(id=subscription_id, subscriber=subscriber)
        self._subscriptions[subscription_id] = subscription

        # If from_index is specified, process existing events
        if from_index is not None:
            for event_id in sorted(self._events.keys()):
                if event_id >= from_index:
                    serialized_payload = self._events[event_id]
                    payload = self.serializer.deserialize(serialized_payload)
                    event = QueueEvent(
                        id=event_id,
                        payload=payload,
                        created_at=self._event_metadata[event_id],
                    )
                    # Process event asynchronously (fire and forget)
                    asyncio.create_task(subscriber.on_event(event, self))

        return subscription

    async def unsubscribe(self, subscriber_id: UUID) -> bool:
        """Remove a subscriber from this queue"""
        self._check_entered()

        if subscriber_id in self._subscriptions:
            del self._subscriptions[subscriber_id]
            return True
        return False

    async def get_subscriber(self, subscriber_id: UUID) -> Subscriber[T]:
        """Get subscriber with id given"""
        self._check_entered()

        if subscriber_id not in self._subscriptions:
            raise EventyError(f"Subscriber {subscriber_id} not found")

        return self._subscriptions[subscriber_id].subscriber

    async def search_subscriptions(
        self, page_id: Optional[str] = None, limit: int = 100
    ) -> Page[Subscription[T]]:
        """Get all subscribers along with their IDs"""
        self._check_entered()

        subscriptions = list(self._subscriptions.values())

        # Simple pagination using page_id as offset
        start_index = 0
        if page_id:
            try:
                start_index = int(page_id)
            except ValueError:
                start_index = 0

        end_index = start_index + limit
        page_items = subscriptions[start_index:end_index]

        # Calculate next page ID
        next_page_id = None
        if end_index < len(subscriptions):
            next_page_id = str(end_index)

        return Page(items=page_items, next_page_id=str(next_page_id))

    async def publish(self, payload: T) -> QueueEvent[T]:
        """Publish an event to this queue"""
        self._check_entered()

        # Serialize payload to insulate from external changes
        serialized_payload = self.serializer.serialize(payload)

        # Create event
        event_id = self._next_event_id
        self._next_event_id += 1
        created_at = datetime.now(UTC)

        # Store serialized payload and metadata
        self._events[event_id] = serialized_payload
        self._event_metadata[event_id] = created_at

        # Deserialize for the event object (creates a copy)
        deserialized_payload = self.serializer.deserialize(serialized_payload)
        event = QueueEvent(
            id=event_id, payload=deserialized_payload, created_at=created_at
        )

        # Notify all subscribers asynchronously
        for subscription in self._subscriptions.values():
            asyncio.create_task(subscription.subscriber.on_event(event, self))

        return event

    async def get_event(self, event_id: int) -> QueueEvent[T]:
        """Get an event given its id."""
        self._check_entered()

        if event_id not in self._events:
            raise EventyError(f"Event {event_id} not found")

        # Deserialize the payload
        serialized_payload = self._events[event_id]
        payload = self.serializer.deserialize(serialized_payload)
        created_at = self._event_metadata[event_id]

        return QueueEvent(id=event_id, payload=payload, created_at=created_at)

    async def search_events(
        self,
        page_id: Optional[str] = None,
        limit: int = 100,
        created_at__gte: Optional[datetime] = None,
        created_at__lte: Optional[datetime] = None,
    ) -> Page[QueueEvent[T]]:
        """Get existing events with optional paging parameters"""
        self._check_entered()

        # Filter events based on criteria
        filtered_events = []
        for event_id in sorted(self._events.keys()):
            created_at = self._event_metadata[event_id]

            if created_at__gte is not None and created_at < created_at__gte:
                continue
            if created_at__lte is not None and created_at > created_at__lte:
                continue

            # Deserialize the payload
            serialized_payload = self._events[event_id]
            payload = self.serializer.deserialize(serialized_payload)
            event = QueueEvent(id=event_id, payload=payload, created_at=created_at)
            filtered_events.append(event)

        # Simple pagination
        start_index = page_id or 0
        end_index = start_index + limit
        page_items = filtered_events[start_index:end_index]

        # Calculate next page ID
        next_page_id = None
        if end_index < len(filtered_events):
            next_page_id = end_index

        return Page(items=page_items, next_page_id=str(next_page_id))

    async def get_result(self, result_id: UUID) -> EventResult:
        """Get an event result given its id"""
        self._check_entered()

        if result_id not in self._results:
            raise EventyError(f"Result {result_id} not found")

        return self._results[result_id]

    async def search_results(
        self,
        page_id: Optional[str] = None,
        limit: int = 100,
        event_id__eq: Optional[int] = None,
        worker_id__eq: Optional[int] = None,
        created_at__gte: Optional[datetime] = None,
        created_at__lte: Optional[datetime] = None,
    ) -> Page[EventResult]:
        """Get existing results with optional paging parameters"""
        self._check_entered()

        # Filter results based on criteria
        filtered_results = []
        for result in self._results.values():
            if event_id__eq is not None and result.event_id != event_id__eq:
                continue
            if worker_id__eq is not None and result.worker_id != worker_id__eq:
                continue
            if created_at__gte is not None and result.created_at < created_at__gte:
                continue
            if created_at__lte is not None and result.created_at > created_at__lte:
                continue
            filtered_results.append(result)

        # Sort by created_at for consistent ordering
        filtered_results.sort(key=lambda r: r.created_at)

        # Simple pagination
        start_index = page_id or 0
        end_index = start_index + limit
        page_items = filtered_results[start_index:end_index]

        # Calculate next page ID
        next_page_id = None
        if end_index < len(filtered_results):
            next_page_id = str(end_index)

        return Page(items=page_items, next_page_id=str(next_page_id))

    async def count_results(
        self,
        event_id__eq: Optional[int] = None,
        worker_id__eq: Optional[int] = None,
        created_at__gte: Optional[datetime] = None,
        created_at__lte: Optional[datetime] = None,
    ) -> int:
        """Get the number of events matching the criteria given"""
        self._check_entered()

        count = 0
        for result in self._results.values():
            if event_id__eq is not None and result.event_id != event_id__eq:
                continue
            if worker_id__eq is not None and result.worker_id != worker_id__eq:
                continue
            if created_at__gte is not None and result.created_at < created_at__gte:
                continue
            if created_at__lte is not None and result.created_at > created_at__lte:
                continue
            count += 1

        return count

    # Additional method to add results (for testing/internal use)
    async def add_result(self, result: EventResult) -> None:
        """Add a result to the queue (internal method)"""
        self._check_entered()
        self._results[result.id] = result

    async def create_claim(self, claim_id: str, data: str | None = None) -> bool:
        """Create a claim with the given ID."""
        self._check_entered()

        if claim_id in self._claims:
            return False

        claim = Claim(id=claim_id, worker_id=self.worker_id, data=data)
        self._claims[claim_id] = claim
        return True

    async def get_claim(self, claim_id: str) -> Claim:
        """Get a claim by its ID."""
        self._check_entered()

        if claim_id not in self._claims:
            raise EventyError(f"Claim {claim_id} not found")

        return self._claims[claim_id]

    async def search_claims(
        self,
        page_id: Optional[str] = None,
        limit: int = 100,
        worker_id__eq: Optional[UUID] = None,
        created_at__gte: Optional[datetime] = None,
        created_at__lte: Optional[datetime] = None,
    ) -> Page[Claim]:
        """Search for claims with optional filtering and pagination."""
        self._check_entered()

        # Filter claims based on criteria
        filtered_claims = []
        for claim in self._claims.values():
            if worker_id__eq is not None and claim.worker_id != worker_id__eq:
                continue
            if created_at__gte is not None and claim.created_at < created_at__gte:
                continue
            if created_at__lte is not None and claim.created_at > created_at__lte:
                continue
            filtered_claims.append(claim)

        # Sort by created_at for consistent ordering
        filtered_claims.sort(key=lambda c: c.created_at)

        # Simple pagination
        start_index = 0
        if page_id:
            try:
                start_index = int(page_id)
            except ValueError:
                start_index = 0

        end_index = start_index + limit
        page_items = filtered_claims[start_index:end_index]

        # Calculate next page ID
        next_page_id = None
        if end_index < len(filtered_claims):
            next_page_id = str(end_index)

        return Page(items=page_items, next_page_id=str(next_page_id))

    async def count_claims(
        self,
        worker_id__eq: Optional[UUID] = None,
        created_at__gte: Optional[datetime] = None,
        created_at__lte: Optional[datetime] = None,
    ) -> int:
        """Count claims matching the given criteria."""
        self._check_entered()

        count = 0
        for claim in self._claims.values():
            if worker_id__eq is not None and claim.worker_id != worker_id__eq:
                continue
            if created_at__gte is not None and claim.created_at < created_at__gte:
                continue
            if created_at__lte is not None and claim.created_at > created_at__lte:
                continue
            count += 1

        return count
