import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import TypeVar, Dict, Type, cast

from eventy.event_queue import EventQueue
from eventy.queue_manager import QueueManager
from eventy.fs.filesystem_event_queue import FilesystemEventQueue
from eventy.serializers.serializer import Serializer, get_default_serializer

T = TypeVar("T")


@dataclass
class FilesystemQueueManager(QueueManager):
    """
    Manager for filesystem event queues.
    Each payload type gets its own subdirectory under the root directory.
    """

    root_dir: Path
    serializer: Serializer = field(default_factory=get_default_serializer)
    max_age: timedelta | None = None
    max_events_per_page: int = 25
    max_page_size_bytes: int = 1024 * 1024  # 1MB
    queues: Dict[Type, FilesystemEventQueue] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def __post_init__(self):
        """Initialize root directory"""
        self.root_dir = Path(self.root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)

    def _get_queue_directory(self, payload_type: Type[T]) -> Path:
        """Get the directory path for a specific payload type"""
        # Use the fully qualified name to avoid conflicts
        type_name = f"{payload_type.__module__}.{payload_type.__qualname__}"
        # Replace dots and other problematic characters with underscores
        safe_name = type_name.replace(".", "_").replace("<", "_").replace(">", "_")
        return self.root_dir / safe_name

    async def get_event_queue(self, payload_type: Type[T]) -> EventQueue[T]:
        """Get an event queue for the specified payload type"""
        async with self.lock:
            if payload_type not in self.queues:
                raise ValueError(
                    f"Queue for payload type {payload_type} not registered"
                )
            return self.queues[payload_type]

    async def get_queue(self, payload_type: Type[T]) -> FilesystemEventQueue[T]:
        """Get a filesystem event queue for the specified payload type"""
        return cast(FilesystemEventQueue[T], await self.get_event_queue(payload_type))

    async def register(self, payload_type: type[T]) -> None:
        """Register a payload type (Create an event queue)"""
        async with self.lock:
            if payload_type not in self.queues:
                # Create a new queue for this payload type
                queue_dir = self._get_queue_directory(payload_type)
                queue = FilesystemEventQueue[T](
                    event_type=payload_type,
                    root_path=queue_dir,
                    max_events_per_page=self.max_events_per_page,
                    max_page_size_bytes=self.max_page_size_bytes,
                    serializer=cast(Serializer[T], self.serializer),
                )
                self.queues[payload_type] = queue

    async def deregister(self, payload_type: type[T]) -> None:
        """Deregister a payload type (Shut down an event queue)"""
        async with self.lock:
            if payload_type in self.queues:
                # For filesystem queues, we just remove from memory
                # The files remain on disk
                del self.queues[payload_type]

    async def get_queue_types(self) -> list[Type]:
        """List all available event queues"""
        async with self.lock:
            return list(self.queues.keys())
