import logging
import os
from abc import ABC, abstractmethod
from typing import Generic, TypeVar
from uuid import UUID

from eventy.queue_event import QueueEvent

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

    async def on_worker_event(
        self, event: QueueEvent[T], current_worker_id: UUID, primary_worker_id: UUID
    ) -> None:
        """
        By default, run the callback only if the current worker matches the primary_worker_id worker.
        Other implementations may vary (e.g.: checking against a predefined worker id, or always executing
        the subscriber no matter the assigned worker)
        """
        if current_worker_id == primary_worker_id:
            await self.on_event(event)
