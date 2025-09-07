import logging
import os
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from eventy.queue_event import QueueEvent
from eventy.util import get_impls, import_from

T = TypeVar("T")

logger = logging.getLogger(__name__)


class Subscriber(Generic[T], ABC):
    """
    Subscriber for an event queue. The subscriber object should be stateless, though it may load
    data from a persistence layer.
    """

    payload_type: type[T]

    @abstractmethod
    async def on_event(self, event: QueueEvent[T]) -> None:
        """Callback for when an event occurs"""


def get_public_subscriber_types() -> list[type[Subscriber]]:
    return get_impls("EVENTY_SUBSCRIBER_TYPES", Subscriber)
