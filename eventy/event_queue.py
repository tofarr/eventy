from abc import ABC, abstractmethod
from datetime import datetime
from typing import Generic, TypeVar, Optional
from uuid import UUID
from eventy.page import Page
from eventy.queue_event import QueueEvent
from eventy.subscriber import Subscriber

T = TypeVar("T")


class EventQueue(Generic[T], ABC):

    event_type: type[T]

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

    async def publish_payload(self, payload: T) -> None:
        """Wrap the payload given in a new event and pubish it"""
        await self.publish(EventQueue(payload=payload))
