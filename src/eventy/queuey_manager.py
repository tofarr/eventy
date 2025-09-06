from abc import ABC, abstractmethod
from typing import TypeVar

from eventy.queuey import EventQueue

T = TypeVar("T")


class QueueyManager(ABC):
    """Manager for coordinating access to event queues. Event queues are typically global within an application."""

    @abstractmethod
    async def get_event_queue(self, payload_type: type[T]) -> EventQueue[T]:
        """Get an event queue for the event type given."""
