from abc import ABC, abstractmethod
from typing import Generic, TypeVar

T = TypeVar("T")


class Subscriber(Generic[T], ABC):

    event_type: type[T]

    @abstractmethod
    async def on_event(self, event: T) -> None:
        """Callback for when an event occurs"""
