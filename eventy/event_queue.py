from abc import ABC, abstractmethod
from datetime import datetime
from typing import Generic, TypeVar, Optional, AsyncIterator
from uuid import UUID
from eventy.event_status import EventStatus
from eventy.page import Page
from eventy.queue_event import QueueEvent
from eventy.subscriber import Subscriber

T = TypeVar("T")


class EventQueue(Generic[T], ABC):
    """Event queue for distributed processing."""

    event_type: type[T]
    """ Type of event handled by this queue """

    @abstractmethod
    async def subscribe(self, subscriber: Subscriber[T]) -> UUID:
        """Add a subscriber to this queue

        Returns:
            UUID: A unique identifier for the subscriber that can be used to unsubscribe
        """

    @abstractmethod
    async def unsubscribe(self, subscriber_id: UUID) -> bool:
        """Remove a subscriber from this queue

        Args:
            subscriber_id: The UUID returned by subscribe()

        Returns:
            bool: True if the subscriber was found and removed, False otherwise
        """

    @abstractmethod
    async def list_subscribers(self) -> dict[UUID, Subscriber[T]]:
        """List all subscribers along with their IDs

        Returns:
            dict[UUID, Subscriber[T]]: A dictionary mapping subscriber IDs to their subscriber objects
        """

    @abstractmethod
    async def publish(self, payload: T) -> None:
        """Publish an event to this queue"""

    @abstractmethod
    async def get_events(
        self,
        page_id: Optional[str] = None,
        limit: Optional[int] = 100,
        created_at__min: Optional[datetime] = None,
        created_at__max: Optional[datetime] = None,
        status__eq: Optional[EventStatus] = None,
    ) -> Page[QueueEvent[T]]:
        """Get existing events from the queue with optional paging parameters

        Args:
            page_id: Optional page_id (typically extracted from a previous page)
            limit: Optional maximum number of events to return
            created_at__min: Optionally filter out events created before this
            created_at__max: Optionally filter out events created after this
            status__eq: Optionally filter events by status

        Returns:
            List of events matching the criteria
        """

    async def count_events(
        self,
        created_at__min: Optional[datetime] = None,
        created_at__max: Optional[datetime] = None,
        status__eq: Optional[EventStatus] = None,
    ) -> int:
        """Get the number of events matching the criteria given"""

    async def iter_events(
        self,
        created_at__min: Optional[datetime] = None,
        created_at__max: Optional[datetime] = None,
        status__eq: Optional[EventStatus] = None,
    ) -> AsyncIterator[QueueEvent[T]]:
        """Create an async iterator over events in the queue

        Args:
            created_at__min: Optionally filter out events created before this datetime
            created_at__max: Optionally filter out events created after this datetime
            status__eq: Optionally filter events by status

        Yields:
            QueueEvent[T]: Individual events from the queue matching the criteria
        """
        page_id: Optional[str] = None

        while True:
            page = await self.get_events(
                page_id=page_id,
                created_at__min=created_at__min,
                created_at__max=created_at__max,
                status__eq=status__eq,
            )

            for event in page.items:
                yield event

            # If there's no next page, we're done
            if page.next_page_id is None:
                break

            page_id = page.next_page_id

    @abstractmethod
    async def get_event(self, event_id: int) -> QueueEvent[T]:
        """Get an event given its id."""

    async def get_all_events(self, event_ids: list[int]) -> list[QueueEvent[T]]:
        results = [self.get_event(event_id) for event_id in event_ids]
        return results
