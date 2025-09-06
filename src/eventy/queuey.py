from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Optional
from uuid import UUID
from eventy.eventy import Eventy
from eventy.subscriber import Subscriber

T = TypeVar("T")


class EventQueue(Generic[T], ABC):

    event_type: type[T]

    @abstractmethod
    async def subscribe(self, subscriber: Subscriber[T]) -> None:
        """Add a subscriber to this queue"""

    @abstractmethod
    async def publish(self, event: Eventy[T]) -> None:
        """Publish an event to this queue"""

    @abstractmethod
    async def get_events(self, after_id: Optional[UUID] = None, limit: Optional[int] = None) -> list[Eventy[T]]:
        """Get existing events from the queue with optional paging parameters
        
        Args:
            after_id: Optional UUID to get events after this ID
            limit: Optional maximum number of events to return
            
        Returns:
            List of events matching the criteria
        """

    async def publish_payload(self, payload: T) -> None:
        await self.publish(Eventy(payload=payload))
