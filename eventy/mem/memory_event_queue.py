import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
import logging
from typing import TypeVar, Optional, List, NamedTuple
from uuid import UUID

from eventy.event_queue import EventQueue
from eventy.page import Page
from eventy.queue_event import QueueEvent, EventStatus
from eventy.subscriber import Subscriber
from eventy.serializers.serializer import Serializer, get_default_serializer

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


class StoredEvent(NamedTuple):
    """Internal representation of a stored event with metadata and serialized payload"""

    id: UUID
    created_at: datetime
    serialized_payload: bytes
    status: EventStatus


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
                payload=payload, 
                created_at=stored_event.created_at,
                status=stored_event.status
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
        # Create event with PROCESSING status initially
        event = QueueEvent(payload=payload, status=EventStatus.PROCESSING)
        
        async with self.lock:
            # Clean up old events before adding new ones
            await self._cleanup_old_events()

            # Serialize only the payload before storing to prevent mutations
            try:
                serialized_payload = self.serializer.serialize(payload)
                final_status = EventStatus.PROCESSED
            except Exception:
                # If serialization fails, mark as ERROR
                serialized_payload = b''  # Empty bytes for failed serialization
                final_status = EventStatus.ERROR
                _LOGGER.error(f"Failed to serialize payload for event {event.id}", exc_info=True)
            
            stored_event = StoredEvent(
                id=event.id,
                created_at=event.created_at,
                serialized_payload=serialized_payload,
                status=final_status,
            )
            self.events.append(stored_event)

            # Create final event with correct status for subscribers
            final_event = QueueEvent(
                id=event.id,
                payload=payload,
                created_at=event.created_at,
                status=final_status
            )

            # Notify all subscribers
            for subscriber in self.subscribers:
                try:
                    await subscriber.on_event(final_event)
                except Exception:
                    # Log and continue notifying other subscribers even if one fails
                    _LOGGER.error("subscriber_error", exc_info=True, stack_info=True)

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
