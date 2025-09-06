from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Generic, TypeVar, Optional, AsyncIterator
from uuid import UUID
from eventy.page import Page
from eventy.queue_event import QueueEvent
from eventy.subscriber import Subscriber

T = TypeVar("T")


class EventQueue(Generic[T], ABC):
    """Event queue for distributed processing."""

    event_type: type[T]
    """ Type of event handled by this queue """
    max_age: timedelta | None = None
    """ Events older than this will be purged from the queue """

    @abstractmethod
    async def subscribe(self, subscriber: Subscriber[T]) -> None:
        """Add a subscriber to this queue"""

    @abstractmethod
    async def publish(self, event: QueueEvent[T]) -> None:
        """Publish an event to this queue"""

    @abstractmethod
    async def get_events(
        self,
        page_id: Optional[str] = None,
        limit: Optional[int] = 100,
        created_at__min: Optional[datetime] = None,
        created_at__max: Optional[datetime] = None,
    ) -> Page[QueueEvent[T]]:
        """Get existing events from the queue with optional paging parameters

        Args:
            page_id: Optional page_id (typically extracted from a previous page)
            limit: Optional maximum number of events to return
            created_at__min: Optionally filter out events created before this
            created_at__max: Optionally filter out events created after this

        Returns:
            List of events matching the criteria
        """

    async def count_events(
        self,
        created_at__min: Optional[datetime] = None,
        created_at__max: Optional[datetime] = None,
    ) -> int:
        """Get the number of events matching the criteria given"""

    async def publish_payload(self, payload: T) -> None:
        """Wrap the payload given in a new event and pubish it"""
        await self.publish(QueueEvent(payload=payload))

    async def iter_events(
        self,
        created_at__min: Optional[datetime] = None,
        created_at__max: Optional[datetime] = None,
        limit: Optional[int] = 100,
    ) -> AsyncIterator[QueueEvent[T]]:
        """Create an async iterator over events in the queue

        Args:
            created_at__min: Optionally filter out events created before this datetime
            created_at__max: Optionally filter out events created after this datetime
            limit: Optional maximum number of events to return per page (default: 100)

        Yields:
            QueueEvent[T]: Individual events from the queue matching the criteria
        """
        page_id: Optional[str] = None

        while True:
            page = await self.get_events(
                page_id=page_id,
                limit=limit,
                created_at__min=created_at__min,
                created_at__max=created_at__max,
            )

            for event in page.items:
                yield event

            # If there's no next page, we're done
            if page.next_page_id is None:
                break

            page_id = page.next_page_id

    async def get_event(self, id: UUID) -> QueueEvent[T]:
        """ Get an event given its id. """