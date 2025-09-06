import asyncio
from datetime import datetime
from typing import TypeVar, Optional, List
from uuid import UUID

from eventy.event_queue import EventQueue
from eventy.page import Page
from eventy.queue_event import QueueEvent
from eventy.subscriber import Subscriber
from eventy.mem.serializer import Serializer
from eventy.mem.pickle_serializer import PickleSerializer

T = TypeVar("T")


class MemoryEventQueue(EventQueue[T]):
    """In-memory implementation of EventQueue with serialization support"""

    def __init__(
        self,
        event_type: type[T],
        serializer: Optional[Serializer[QueueEvent[T]]] = None
    ):
        """Initialize the memory event queue
        
        Args:
            event_type: The type of events this queue handles
            serializer: Optional serializer for events (defaults to PickleSerializer)
        """
        self.event_type = event_type
        self._serializer = serializer or PickleSerializer[QueueEvent[T]]()
        self._events: List[bytes] = []  # Store serialized events
        self._subscribers: List[Subscriber[T]] = []
        self._lock = asyncio.Lock()

    async def subscribe(self, subscriber: Subscriber[T]) -> None:
        """Add a subscriber to this queue"""
        async with self._lock:
            self._subscribers.append(subscriber)

    async def publish(self, event: QueueEvent[T]) -> None:
        """Publish an event to this queue"""
        async with self._lock:
            # Serialize the event before storing to prevent mutations
            serialized_event = self._serializer.serialize(event)
            self._events.append(serialized_event)
            
            # Notify all subscribers
            for subscriber in self._subscribers:
                try:
                    await subscriber.on_event(event)
                except Exception:
                    # Continue notifying other subscribers even if one fails
                    pass

    async def get_events(
        self,
        page_id: Optional[str] = None,
        limit: Optional[int] = 100,
        created_at__min: Optional[datetime] = None,
        created_at__max: Optional[datetime] = None,
    ) -> Page[QueueEvent[T]]:
        """Get existing events from the queue with optional paging parameters"""
        async with self._lock:
            # Deserialize all events for filtering
            all_events = []
            for serialized_event in self._events:
                try:
                    event = self._serializer.deserialize(serialized_event)
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

            # Sort by created_at for consistent ordering
            filtered_events.sort(key=lambda e: e.created_at)

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

    def clear(self) -> None:
        """Clear all events from the queue (useful for testing)"""
        self._events.clear()

    def get_event_count(self) -> int:
        """Get the total number of events in the queue"""
        return len(self._events)