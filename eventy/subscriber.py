from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from eventy.queue_event import QueueEvent

T = TypeVar("T")


class Subscriber(Generic[T], ABC):

    payload_type: type[T]

    @abstractmethod
    async def on_event(self, event: QueueEvent[T]) -> None:
        """Callback for when an event occurs"""
