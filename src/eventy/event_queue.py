from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from eventy.subscriber import Subscriber

T = TypeVar("T")


class EventQueue(Generic[T], ABC):

    event_type: type[T]

    @abstractmethod
    async def subscribe(self, subscriber: Subscriber[T]):
        """Add a subscriber to this queue"""

    @abstractmethod
    async def publish(self, event: T):
        """Publish an event to this queue"""
