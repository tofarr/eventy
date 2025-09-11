import asyncio
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import TypeVar, Dict
from uuid import UUID, uuid4

from eventy.event_queue import EventQueue
from eventy.eventy_error import EventyError
from eventy.queue_manager import QueueManager
from eventy.redis.redis_file_event_queue import RedisFileEventQueue
from eventy.serializers.serializer import Serializer, get_default_serializer

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


@dataclass
class RedisQueueManager(QueueManager):
    """
    Redis-enhanced file-based implementation of QueueManager.

    Creates RedisFileEventQueue instances that use Redis for coordination
    and distributed event processing while maintaining filesystem persistence.
    """

    root_dir: Path
    redis_url: str = field(default="redis://localhost:6379")
    redis_db: int = field(default=1)
    redis_password: str | None = field(default=None)
    redis_prefix: str = field(default="eventy")
    worker_id: UUID = field(default_factory=uuid4)
    serializer: Serializer = field(default_factory=get_default_serializer)

    # Redis-specific configuration
    claim_expiration_seconds: int = field(default=300)  # 5 minutes
    resync_lock_timeout_seconds: int = field(default=30)
    enable_pubsub_monitor: bool = field(default=True)
    resync: bool = field(default=False)

    # Internal storage
    _queues: Dict[type, EventQueue] = field(default_factory=dict, init=False)
    _entered: bool = field(default=False, init=False)

    def __post_init__(self):
        """Initialize the manager"""
        # Convert string path to Path object if needed
        if isinstance(self.root_dir, str):
            self.root_dir = Path(self.root_dir)

        self.root_dir.mkdir(parents=True, exist_ok=True)
        _LOGGER.info(f"Initialized RedisQueueManager with Redis at {self.redis_url}")

    def _check_entered(self) -> None:
        """Check if the manager has been entered, raise error if not"""
        if not self._entered:
            raise EventyError(
                "RedisQueueManager must be entered using async context manager before use"
            )

    async def __aenter__(self):
        """Begin using this queue manager"""
        self._entered = True

        # Ensure root directory exists
        self.root_dir.mkdir(parents=True, exist_ok=True)

        # Start all registered queues
        await asyncio.gather(*[queue.__aenter__() for queue in self._queues.values()])

        _LOGGER.info(f"Started RedisQueueManager at {self.root_dir}")
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Finish using this queue manager"""
        self._entered = False

        # Close all queues
        await asyncio.gather(
            *[
                queue.__aexit__(exc_type, exc_value, traceback)
                for queue in self._queues.values()
            ]
        )

        self._queues.clear()
        _LOGGER.info(f"Stopped RedisQueueManager at {self.root_dir}")

    def _create_queue(self, payload_type: type[T]) -> EventQueue[T]:
        """Create a new Redis-enhanced event queue instance"""
        queue_dir = self.root_dir / payload_type.__name__

        queue = RedisFileEventQueue(
            root_dir=queue_dir,
            payload_type=payload_type,
            redis_url=self.redis_url,
            redis_db=self.redis_db,
            redis_password=self.redis_password,
            redis_prefix=self.redis_prefix,
            worker_id=self.worker_id,
            event_serializer=self.serializer,
            result_serializer=self.serializer,
            subscriber_serializer=self.serializer,
            claim_serializer=self.serializer,
            claim_expiration_seconds=self.claim_expiration_seconds,
            resync_lock_timeout_seconds=self.resync_lock_timeout_seconds,
            enable_pubsub_monitor=self.enable_pubsub_monitor,
            resync=self.resync,
        )

        _LOGGER.debug(f"Created RedisFileEventQueue for {payload_type}")
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

        # Create new Redis-enhanced file event queue
        queue = self._create_queue(payload_type)

        if self._entered:
            await queue.__aenter__()  # pylint: disable=unnecessary-dunder-call

        self._queues[payload_type] = queue
        _LOGGER.info(f"Registered Redis queue for payload type: {payload_type}")

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
        _LOGGER.info(f"Deregistered Redis queue for payload type: {payload_type}")

    async def reset(self, payload_type: type[T]):
        """Clear all Events, Results and claims for a payload type"""
        self._check_entered()

        if payload_type not in self._queues:
            raise EventyError(f"No queue found for payload type: {payload_type}")

        queue: RedisFileEventQueue[T] = self._queues[payload_type]

        # Reset the queue (this will clear filesystem and Redis data)
        await queue.reset()

        _LOGGER.info(f"Reset Redis queue for payload type: {payload_type}")

    async def set_resync_for_all_queues(self, resync: bool) -> None:
        """Enable or disable resync for all registered queues"""
        self.resync = resync

        for queue in self._queues.values():
            if isinstance(queue, RedisFileEventQueue):
                queue.resync = resync

        _LOGGER.info(f"Set resync={resync} for all queues")

    async def get_redis_connection_status(self) -> Dict[type, bool]:
        """Get Redis connection status for all registered queues"""
        status = {}

        for payload_type, queue in self._queues.items():
            if isinstance(queue, RedisFileEventQueue):
                # Check if Redis is available by checking if _redis is not None
                status[payload_type] = queue._redis is not None  # pylint: disable=protected-access
            else:
                status[payload_type] = False

        return status
