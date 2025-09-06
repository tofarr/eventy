from abc import ABC, abstractmethod
from typing import TypeVar

from eventy.event_queue import EventQueue

T = TypeVar("T")


class EventQueueManager(ABC):
    """Manager for coordinating access to event queues. Event queues are typically global within an application."""

    @abstractmethod
    async def get_event_queue(self, event_type: type[T]) -> EventQueue[T]:
        """Get an event queue for the event type given."""
