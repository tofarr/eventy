from abc import ABC, abstractmethod
from typing import TypeVar, Optional

from eventy.event_queue import EventQueue
from eventy.util import get_impl

T = TypeVar("T")


class QueueManager(ABC):
    """Manager for coordinating access to event queues. Event queues are typically global within an application."""

    @abstractmethod
    async def get_event_queue(self, payload_type: type[T]) -> EventQueue[T]:
        """Get an event queue for the event type given."""

    @abstractmethod
    async def get_queue_types(self) -> list[type]:
        """List all available event queues."""

    @abstractmethod
    async def register(self, payload_type: type[T]) -> None:
        """Register a payload type (Create an event queue)"""

    @abstractmethod
    async def deregister(self, payload_type: type[T]) -> None:
        """Deregister a payload type (Shut down an event queue)"""

    async def __aenter__(self):
        """Begin using this queue manager"""

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Finish using this queue manager"""


# Global cache for the default queue manager instance
_default_queue_manager: Optional[QueueManager] = None


def get_default_queue_manager() -> QueueManager:
    """Get the default queue manager instance with caching.

    The implementation can be overridden by setting the EVENTY_QUEUE_MANAGER
    environment variable to a fully qualified class name.

    Returns:
        QueueManager: The cached default queue manager instance
    """
    global _default_queue_manager

    if _default_queue_manager is None:
        from eventy.mem.memory_queue_manager import MemoryQueueManager

        manager_class = get_impl(
            "EVENTY_QUEUE_MANAGER", QueueManager, MemoryQueueManager
        )
        _default_queue_manager = manager_class()

    return _default_queue_manager
