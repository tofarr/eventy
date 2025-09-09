from abc import ABC, abstractmethod
from typing import Generic, TypeVar, TYPE_CHECKING

from eventy.queue_event import QueueEvent

if TYPE_CHECKING:
    from eventy.event_queue import EventQueue

T = TypeVar("T")


class Subscriber(Generic[T], ABC):
    """
    Subscriber for an event queue. The subscriber object should be stateless, though it may load
    data from a persistence layer.
    """

    payload_type: type[T]

    @abstractmethod
    async def on_event(self, event: QueueEvent[T], event_queue: "EventQueue[T]") -> None:
        """Callback for when an event occurs
        
        Args:
            event: The queue event that occurred
            event_queue: The event queue instance that can be used to access queue functionality
        """
