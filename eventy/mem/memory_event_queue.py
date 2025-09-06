import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
import logging
from typing import TypeVar, Optional, List
from uuid import UUID

from eventy.event_queue import EventQueue
from eventy.page import Page
from eventy.queue_event import QueueEvent
from eventy.subscriber import Subscriber
from eventy.serializers.serializer import Serializer, get_default_serializer

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


@dataclass
class MemoryEventQueue(EventQueue[T]):
    """In-memory implementation of EventQueue with serialization support"""

    event_type: type[T]
    max_age: timedelta | None = None
    serializer: Serializer[QueueEvent[T]] = field(
        default_factory=get_default_serializer
    )
    events: list[bytes] = field(default_factory=list)
    subscribers: list[Subscriber[T]] = field(default_factory=list)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def _cleanup_old_events(self) -> None:
        """Remove events older than max_age if max_age is set"""
        if self.max_age is None:
            return
            
        cutoff_time = datetime.now(UTC) - self.max_age
        events_to_keep = []
        
        for serialized_event in self.events:
            try:
                event = self.serializer.deserialize(serialized_event)
                if event.created_at >= cutoff_time:
                    events_to_keep.append(serialized_event)
            except Exception:
                # Skip corrupted events (they'll be removed)
                continue
        
        if len(events_to_keep) != len(self.events):
            removed_count = len(self.events) - len(events_to_keep)
            self.events = events_to_keep
            _LOGGER.debug(f"Cleaned up {removed_count} old events from queue")

    async def subscribe(self, subscriber: Subscriber[T]) -> None:
        """Add a subscriber to this queue"""
        async with self.lock:
            await self._cleanup_old_events()
            self.subscribers.append(subscriber)

    async def publish(self, event: QueueEvent[T]) -> None:
        """Publish an event to this queue"""
        async with self.lock:
            # Clean up old events before adding new ones
            await self._cleanup_old_events()
            
            # Serialize the event before storing to prevent mutations
            serialized_event = self.serializer.serialize(event)
            self.events.append(serialized_event)

            # Notify all subscribers
            for subscriber in self.subscribers:
                try:
                    await subscriber.on_event(event)
                except Exception:
                    # Log and continue notifying other subscribers even if one fails
                    _LOGGER.error("subscriber_error", exc_info=True, stack_info=True)

    async def get_events(
        self,
        page_id: Optional[str] = None,
        limit: Optional[int] = 100,
        created_at__min: Optional[datetime] = None,
        created_at__max: Optional[datetime] = None,
    ) -> Page[QueueEvent[T]]:
        """Get existing events from the queue with optional paging parameters"""
        async with self.lock:
            # Clean up old events before retrieving
            await self._cleanup_old_events()
            
            # Deserialize all events for filtering
            all_events = []
            for serialized_event in self.events:
                try:
                    event = self.serializer.deserialize(serialized_event)
                    all_events.append(event)
                except Exception:
                    # Skip corrupted events
                    continue

            # Apply datetime filters
            filtered_events = []
            for event in all_events:
                if created_at__min and event.created_at < created_at__min:
                    continue
                if created_at__max and event.created_at > created_at__max:
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
    ) -> int:
        """Get the number of events matching the criteria given"""
        async with self.lock:
            # Clean up old events before counting
            await self._cleanup_old_events()
            
            # Deserialize all events for filtering
            count = 0
            for serialized_event in self.events:
                try:
                    event = self.serializer.deserialize(serialized_event)
                    # Apply datetime filters
                    if created_at__min and event.created_at < created_at__min:
                        continue
                    if created_at__max and event.created_at > created_at__max:
                        continue
                    count += 1
                except Exception:
                    # Skip corrupted events
                    continue
            return count

    async def get_event(self, id: UUID) -> QueueEvent[T]:
        """Get an event given its id."""
        async with self.lock:
            # Clean up old events before searching
            await self._cleanup_old_events()
            
            # Search through all events to find the one with matching ID
            for serialized_event in self.events:
                try:
                    event = self.serializer.deserialize(serialized_event)
                    if event.id == id:
                        return event
                except Exception:
                    # Skip corrupted events
                    continue
            
            # If we get here, the event was not found
            raise ValueError(f"Event with id {id} not found")
