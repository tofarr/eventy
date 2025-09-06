from abc import ABC, abstractmethod
from typing import Generic, TypeVar
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

    async def publish_payload(self, payload: T) -> None:
        await self.publish(Eventy(payload=payload))
