from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from uuid import UUID

from eventy.queue_event import QueueEvent

T = TypeVar("T")


class Subscriber(Generic[T], ABC):
    """
    Subscriber for an event queue. The subscriber object should be stateless, though it may load
    data from a persistence layer.
    """

    payload_type: type[T]

    @abstractmethod
    async def on_event(self, event: QueueEvent[T], current_worker_id: UUID, primary_worker_id: UUID) -> None:
        """Callback for when an event occurs"""
