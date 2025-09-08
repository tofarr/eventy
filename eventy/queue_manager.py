from abc import ABC, abstractmethod
import logging
import os
from typing import TypeVar, Optional

from eventy.constants import EVENTY_QUEUE_MANAGER, EVENTY_ROOT_DIR
from eventy.event_queue import EventQueue
from eventy.eventy_config import get_config
from eventy.util import get_impl

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


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
        try:
            # Check environment...
            manager_class = get_impl(
                EVENTY_QUEUE_MANAGER, QueueManager
            )
            _default_queue_manager = manager_class()
        except ValueError:
            root_dir = os.getenv(EVENTY_ROOT_DIR)
            if root_dir:
                from eventy.fs.filesystem_queue_manager import FilesystemQueueManager
                _default_queue_manager = FilesystemQueueManager(root_dir=root_dir)
            else:
                from eventy.mem.memory_queue_manager import MemoryQueueManager
                _default_queue_manager = MemoryQueueManager()

        try:
            config = get_config()
            for payload_type in config.get_payload_types():
                _default_queue_manager.register(payload_type)
        except ValueError:
            _LOGGER.info('no_initial_payload_types')

    return _default_queue_manager
