from abc import ABC, abstractmethod
from typing import Generic, TypeVar, TYPE_CHECKING, get_args, get_origin

from eventy.queue_event import QueueEvent

if TYPE_CHECKING:
    from eventy.event_queue import EventQueue

T = TypeVar("T")


class Subscriber(Generic[T], ABC):
    """
    Subscriber for an event queue. The subscriber object should be stateless, though it may load
    data from a persistence layer.
    """

    @abstractmethod
    async def on_event(
        self, event: QueueEvent[T], event_queue: "EventQueue[T]"
    ) -> None:
        """Callback for when an event occurs

        Args:
            event: The queue event that occurred
            event_queue: The event queue instance that can be used to access queue functionality
        """


def get_payload_type(subscriber_type: type):
    # Check if the type is in the format: SomeSubscriber[PayloadType]
    subscriber_payload_types = get_args(subscriber_type)
    if subscriber_payload_types:
        return subscriber_payload_types[0]

    # Check if type is a class that extends Subscriber[PayloadType]
    for base in subscriber_type.__orig_bases__:
        origin = get_origin(base)
        if origin is None and issubclass(base, Subscriber):
            return get_payload_type(base)
        if origin is not None and issubclass(origin, Subscriber):
            args = get_args(base)
            return args[0]

    raise TypeError(f"Could not get payload type for {subscriber_type}")
