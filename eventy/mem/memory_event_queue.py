import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
import logging
from typing import TypeVar, Optional
from uuid import UUID, uuid4

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
    id: UUID
    serialized_payload: bytes
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    status: EventStatus = EventStatus.PROCESSING


@dataclass
class MemoryEventQueue(EventQueue[T]):
    """
    In-memory implementation of EventQueue with payload-only serialization suitable for single process
    with small number of events
    """

    event_type: type[T]
    max_age: timedelta | None = None
    serializer: Serializer[T] = field(default_factory=get_default_serializer)
    events: list[StoredEvent] = field(default_factory=list)
    subscribers: list[Subscriber[T]] = field(default_factory=list)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def _reconstruct_event(self, stored_event: StoredEvent) -> QueueEvent[T]:
        """Reconstruct a QueueEvent from a StoredEvent"""
        try:
            payload = self.serializer.deserialize(stored_event.serialized_payload)
            return QueueEvent(
                id=stored_event.id, 
                status=stored_event.status,
                payload=payload, 
                created_at=stored_event.created_at,
            )
        except Exception:
            # If deserialization fails, we can't reconstruct the event
            raise ValueError(
                f"Failed to deserialize payload for event {stored_event.id}"
            )

    async def _cleanup_old_events(self) -> None:
        """Remove events older than max_age if max_age is set"""
        if self.max_age is None:
            return

        cutoff_time = datetime.now(UTC) - self.max_age
        events_to_keep = []

        for stored_event in self.events:
            if stored_event.created_at >= cutoff_time:
                events_to_keep.append(stored_event)

        if len(events_to_keep) != len(self.events):
            removed_count = len(self.events) - len(events_to_keep)
            self.events = events_to_keep
            _LOGGER.debug(f"Cleaned up {removed_count} old events from queue")

    async def subscribe(self, subscriber: Subscriber[T]) -> None:
        """Add a subscriber to this queue"""
        async with self.lock:
            await self._cleanup_old_events()
            self.subscribers.append(subscriber)

    async def publish(self, payload: T) -> None:
        """Publish an event to this queue"""

        async with self.lock:
            # Clean up old events before adding new ones
            await self._cleanup_old_events()

            # Serialize only the payload before storing to prevent mutations
            event_id = uuid4()
            serialized_payload = self.serializer.serialize(payload)
            
            stored_event = StoredEvent(
                id=event_id,
                serialized_payload=serialized_payload,
            )
            self.events.append(stored_event)

            # Notify all subscribers
            event = self._reconstruct_event(stored_event)
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
            # Clean up old events before retrieving
            await self._cleanup_old_events()

            # Reconstruct events for filtering
            all_events = []
            for stored_event in self.events:
                try:
                    event = self._reconstruct_event(stored_event)
                    all_events.append(event)
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
            filtered_events.sort(key=lambda e: e.created_at, reverse=True)

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
            # Clean up old events before counting
            await self._cleanup_old_events()

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

    async def get_event(self, id: UUID) -> QueueEvent[T]:
        """Get an event given its id."""
        async with self.lock:
            # Clean up old events before searching
            await self._cleanup_old_events()

            # Search through all events to find the one with matching ID
            for stored_event in self.events:
                if stored_event.id == id:
                    try:
                        return self._reconstruct_event(stored_event)
                    except Exception:
                        # If reconstruction fails, treat as not found
                        break

            # If we get here, the event was not found
            raise ValueError(f"Event with id {id} not found")
