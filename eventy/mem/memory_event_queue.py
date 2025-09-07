import asyncio
from dataclasses import dataclass, field
from datetime import datetime, UTC
import logging
from typing import TypeVar, Optional

from eventy.event_queue import EventQueue
from eventy.page import Page
from eventy.queue_event import QueueEvent, EventStatus
from eventy.subscriber import Subscriber
from eventy.serializers.serializer import Serializer, get_default_serializer

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


@dataclass
class StoredEvent:
    """Internal representation of a stored event with metadata and serialized payload"""
    serialized_payload: bytes
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    status: EventStatus = EventStatus.PROCESSING


@dataclass
class MemoryEventQueue(EventQueue[T]):
    """
    In-memory implementation of EventQueue with payload-only serialization suitable for single process
    with small number of events.
    """

    event_type: type[T]
    serializer: Serializer[T] = field(default_factory=get_default_serializer)
    events: list[StoredEvent] = field(default_factory=list)
    subscribers: list[Subscriber[T]] = field(default_factory=list)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def _reconstruct_event(self, event_id: int, stored_event: StoredEvent) -> QueueEvent[T]:
        """Reconstruct a QueueEvent from a StoredEvent"""
        try:
            payload = self.serializer.deserialize(stored_event.serialized_payload)
            return QueueEvent(
                id=event_id, 
                status=stored_event.status,
                payload=payload, 
                created_at=stored_event.created_at,
            )
        except Exception:
            # If deserialization fails, we can't reconstruct the event
            raise ValueError(
                f"Failed to deserialize payload for event {stored_event.id}"
            )

    async def subscribe(self, subscriber: Subscriber[T]) -> None:
        """Add a subscriber to this queue"""
        async with self.lock:
            self.subscribers.append(subscriber)

    async def publish(self, payload: T) -> None:
        """Publish an event to this queue"""

        async with self.lock:

            # Serialize only the payload before storing to prevent mutations
            serialized_payload = self.serializer.serialize(payload)
            
            stored_event = StoredEvent(
                serialized_payload=serialized_payload,
            )
            self.events.append(stored_event)

            # Notify all subscribers
            event = self._reconstruct_event(len(self.events), stored_event)
            final_status = EventStatus.PROCESSING
            for subscriber in self.subscribers:
                try:
                    await subscriber.on_event(event)
                except Exception:
                    final_status = EventStatus.ERROR
                    # Log and continue notifying other subscribers even if one fails
                    _LOGGER.error("subscriber_error", exc_info=True, stack_info=True)
            stored_event.status = final_status

    async def get_events(
        self,
        page_id: Optional[str] = None,
        limit: Optional[int] = 100,
        created_at__min: Optional[datetime] = None,
        created_at__max: Optional[datetime] = None,
        status__eq: Optional[EventStatus] = None,
    ) -> Page[QueueEvent[T]]:
        """Get existing events from the queue with optional paging parameters"""
        async with self.lock:

            # Reconstruct events for filtering
            all_events = []
            id = 1
            for stored_event in self.events:
                try:
                    event = self._reconstruct_event(id, stored_event)
                    all_events.append(event)
                    id += 1
                except Exception:
                    # Skip corrupted events
                    continue

            # Apply filters
            filtered_events = []
            for event in all_events:
                if created_at__min and event.created_at < created_at__min:
                    continue
                if created_at__max and event.created_at > created_at__max:
                    continue
                if status__eq and event.status != status__eq:
                    continue
                filtered_events.append(event)

            # Sort by created_at desc for consistent ordering
            filtered_events.reverse()

            # Handle pagination
            start_index = 0
            if page_id:
                try:
                    start_index = int(page_id)
                except (ValueError, TypeError):
                    start_index = 0

            # Get the page of events
            end_index = start_index + (limit or 100)
            page_events = filtered_events[start_index:end_index]

            # Determine next page ID
            next_page_id = None
            if end_index < len(filtered_events):
                next_page_id = str(end_index)

            return Page(items=page_events, next_page_id=next_page_id)

    async def count_events(
        self,
        created_at__min: Optional[datetime] = None,
        created_at__max: Optional[datetime] = None,
        status__eq: Optional[EventStatus] = None,
    ) -> int:
        """Get the number of events matching the criteria given"""
        async with self.lock:

            # Count events using stored metadata (no need to deserialize payload)
            count = 0
            for stored_event in self.events:
                # Apply filters using stored metadata
                if created_at__min and stored_event.created_at < created_at__min:
                    continue
                if created_at__max and stored_event.created_at > created_at__max:
                    continue
                if status__eq and stored_event.status != status__eq:
                    continue
                count += 1
            return count

    async def get_event(self, id: int) -> QueueEvent[T]:
        """Get an event given its id."""
        index = id - 1
        async with self.lock:
            stored_event = self.events[index]
        return self._reconstruct_event(id, stored_event)
