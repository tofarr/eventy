from dataclasses import dataclass, field
from typing import TypeVar, Dict
import logging

from eventy.event_queue import EventQueue
from eventy.eventy_error import EventyError
from eventy.mem.memory_event_queue import MemoryEventQueue
from eventy.queue_manager import QueueManager
from eventy.serializers.serializer import Serializer, get_default_serializer

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


@dataclass
class MemoryQueueManager(QueueManager):
    """In-memory implementation of QueueManager using dataclasses"""

    serializer: Serializer = field(default_factory=get_default_serializer)

    # Internal storage
    _queues: Dict[type, EventQueue] = field(default_factory=dict, init=False)
    _entered: bool = field(default=False, init=False)

    def _check_entered(self) -> None:
        """Check if the manager has been entered, raise error if not"""
        if not self._entered:
            raise EventyError(
                "QueueManager must be entered using async context manager before use"
            )

    async def __aenter__(self):
        """Begin using this queue manager"""
        self._entered = True
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Finish using this queue manager"""
        self._entered = False

        # Close all queues
        for queue in self._queues.values():
            try:
                await queue.__aexit__(exc_type, exc_value, traceback)
            except Exception as e:
                _LOGGER.warning(f"Error closing queue: {e}", exc_info=True)

        self._queues.clear()

    async def get_event_queue(self, payload_type: type[T]) -> EventQueue[T]:
        """Get an event queue for the event type given"""
        self._check_entered()

        if payload_type not in self._queues:
            raise EventyError(f"No queue registered for payload type: {payload_type}")

        return self._queues[payload_type]

    async def get_queue_types(self) -> list[type]:
        """List all available event queues"""
        self._check_entered()

        return list(self._queues.keys())

    async def register(self, payload_type: type[T]) -> None:
        """Register a payload type (Create an event queue)"""
        self._check_entered()

        if payload_type in self._queues:
            _LOGGER.info(f"Queue for payload type {payload_type} already registered")
            return

        # Create new memory event queue
        queue = MemoryEventQueue(payload_type=payload_type, serializer=self.serializer)

        # Enter the queue context manually
        queue._entered = True

        self._queues[payload_type] = queue
        _LOGGER.info(f"Registered queue for payload type: {payload_type}")

    async def deregister(self, payload_type: type[T]) -> None:
        """Deregister a payload type (Shut down an event queue)"""
        self._check_entered()

        if payload_type not in self._queues:
            _LOGGER.warning(f"No queue found for payload type: {payload_type}")
            return

        # Close the queue
        queue = self._queues[payload_type]
        try:
            await queue.__aexit__(None, None, None)
        except Exception as e:
            _LOGGER.warning(
                f"Error closing queue for {payload_type}: {e}", exc_info=True
            )

        del self._queues[payload_type]
        _LOGGER.info(f"Deregistered queue for payload type: {payload_type}")
