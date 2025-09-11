import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path
import shutil
from typing import TypeVar, Dict

from eventy.event_queue import EventQueue
from eventy.eventy_error import EventyError
from eventy.fs.abstract_file_event_queue import AbstractFileEventQueue
from eventy.fs.polling_file_event_queue import PollingFileEventQueue
from eventy.queue_manager import QueueManager
from eventy.serializers.serializer import Serializer, get_default_serializer

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


@dataclass
class FileEventQueueManager(QueueManager):
    """
    File-based implementation of QueueManager.

    Automatically chooses between WatchdogFileEventQueue (if watchdog is available)
    and PollingFileEventQueue (fallback) when creating queue instances.
    """

    root_dir: Path
    serializer: Serializer = field(default_factory=get_default_serializer)
    polling_interval: float = field(default=1.0)

    # Internal storage
    _queues: Dict[type, EventQueue] = field(default_factory=dict, init=False)
    _entered: bool = field(default=False, init=False)
    _use_watchdog: bool = field(default=None, init=False)

    def __post_init__(self):
        """Initialize the manager and detect watchdog availability"""
        # Convert string path to Path object if needed
        if isinstance(self.root_dir, str):
            self.root_dir = Path(self.root_dir)

        self.root_dir.mkdir(parents=True, exist_ok=True)

        # Detect watchdog availability once during initialization
        if self._use_watchdog is None:
            try:
                # Try to import watchdog to check availability
                import watchdog  # noqa: F401

                self._use_watchdog = True
                _LOGGER.info(
                    "Watchdog library detected - will use WatchdogFileEventQueue"
                )
            except ImportError:
                self._use_watchdog = False
                _LOGGER.info(
                    "Watchdog library not available - will use PollingFileEventQueue"
                )

    def _check_entered(self) -> None:
        """Check if the manager has been entered, raise error if not"""
        if not self._entered:
            raise EventyError(
                "FileEventQueueManager must be entered using async context manager before use"
            )

    async def __aenter__(self):
        """Begin using this queue manager"""
        self._entered = True

        # Ensure root directory exists
        self.root_dir.mkdir(parents=True, exist_ok=True)

        await asyncio.gather(*[queue.__aenter__() for queue in self._queues.values()])

        _LOGGER.info(f"Started FileEventQueueManager at {self.root_dir}")
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Finish using this queue manager"""
        self._entered = False

        # Close all queues
        await asyncio.gather(*[queue.__aexit__(exc_type, exc_value, traceback) for queue in self._queues.values()])

        self._queues.clear()
        _LOGGER.info(f"Stopped FileEventQueueManager at {self.root_dir}")

    def _create_queue(self, payload_type: type[T]) -> EventQueue[T]:
        """Create a new event queue instance based on watchdog availability"""
        queue_dir = self.root_dir / payload_type.__name__
        if self._use_watchdog:
            # Use inline import to avoid import errors when watchdog is not available
            try:
                from eventy.fs.watchdog_file_event_queue import WatchdogFileEventQueue

                queue = WatchdogFileEventQueue(
                    root_dir=queue_dir,
                    payload_type=payload_type,
                    event_serializer=self.serializer,
                    result_serializer=self.serializer,
                    subscriber_serializer=self.serializer,
                    claim_serializer=self.serializer,
                )
                _LOGGER.debug(f"Created WatchdogFileEventQueue for {payload_type}")
                return queue
            except ImportError as e:
                _LOGGER.warning(f"Failed to import WatchdogFileEventQueue: {e}")
                # Fall back to polling queue
                self._use_watchdog = False

        # Use polling queue as fallback or primary choice
        queue = PollingFileEventQueue(
            root_dir=queue_dir,
            payload_type=payload_type,
            event_serializer=self.serializer,
            result_serializer=self.serializer,
            subscriber_serializer=self.serializer,
            claim_serializer=self.serializer,
            polling_interval=self.polling_interval,
        )
        _LOGGER.debug(f"Created PollingFileEventQueue for {payload_type}")
        return queue

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

        if payload_type in self._queues:
            _LOGGER.info(f"Queue for payload type {payload_type} already registered")
            return

        # Create new file event queue
        queue = self._create_queue(payload_type)

        if self._entered:
            await queue.__aenter__()

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
            if self._entered:
                await queue.__aexit__(None, None, None)
        except Exception as e:
            _LOGGER.warning(
                f"Error closing queue for {payload_type}: {e}", exc_info=True
            )

        del self._queues[payload_type]
        _LOGGER.info(f"Deregistered queue for payload type: {payload_type}")

    async def reset(self, payload_type: type[T]):
        queue: AbstractFileEventQueue[T] = self._queues[payload_type]
        shutil.rmtree(queue.events_dir)
        shutil.rmtree(queue.results_dir)
        shutil.rmtree(queue.claims_dir)
        queue.processed_event_id = 0
        queue.next_event_id = 1
