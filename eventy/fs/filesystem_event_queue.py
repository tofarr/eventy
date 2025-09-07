import asyncio
import logging
import os
import json
import fcntl
import base64
from dataclasses import dataclass, field
from datetime import datetime, timedelta, UTC
from pathlib import Path
import pickle
import random
import time
from typing import Generator, Iterator, TypeVar, Optional, List, AsyncIterator
from uuid import UUID, uuid4

from eventy.event_queue import EventQueue
from eventy.fs.filesystem_page import FilesystemPage
from eventy.event_status import EventStatus
from eventy.page import Page
from eventy.queue_event import QueueEvent
from eventy.serializers.serializer import Serializer, get_default_serializer
from eventy.subscriber import Subscriber

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


@dataclass
class FilesystemEventQueue(EventQueue[T]):
    """
    Filesystem-based event queue implementation.

    Events are stored as individual files in an 'events' directory.
    When a threshold is reached (by count or size), events are consolidated
    into numbered page files in a 'pages' directory.
    """

    event_type: type[T]
    root_path: Path
    bg_task_delay: int = 15
    subscriber_task_delay: int = 3
    worker_id: UUID = field(default_factory=uuid4)
    max_events_per_page: int = 25
    max_page_size_bytes: int = 1024 * 1024  # 1MB
    serializer: Serializer[T] = field(default_factory=get_default_serializer)
    page_serializer: Serializer[FilesystemPage[T]] = field(
        default_factory=get_default_serializer
    )
    subscribers: list[Subscriber[T]] = field(default_factory=list)

    _event_dir: Path | None = None
    _meta_dir: Path | None = None
    _page_dir: Path | None = None
    _worker_dir: Path | None = None
    _assignment_dir: Path | None = None
    _worker_ids: list[UUID] | None = None
    _next_event_id: int | None = None
    _bg_task: asyncio.Task | None = None

    def __post_init__(self) -> None:
        """Initialize directory structure"""
        self._event_dir = self.root_path / "event"
        self._meta_dir = self.root_path / "status"
        self._page_dir = self.root_path / "page"
        self._worker_dir = self.root_path / "worker"
        self._assignment_dir = self.root_path / "assignment"

        # Create directories if they don't exist
        self._event_dir.mkdir(parents=True, exist_ok=True)
        self._meta_dir.mkdir(parents=True, exist_ok=True)
        self._page_dir.mkdir(parents=True, exist_ok=True)
        self._worker_dir.mkdir(parents=True, exist_ok=True)
        self._assignment_dir.mkdir(parents=True, exist_ok=True)

        self.recalculate_next_event_id()

    def recalculate_next_event_id(self):
        self._next_event_id = len(os.listdir(self._event_dir)) + 1

    async def publish(self, payload: T) -> None:
        """Publish an event to the queue"""
        event_id = self._create_event_files(payload)
        self._assign_event(event_id)

    async def subscribe(self, subscriber: Subscriber[T]) -> None:
        """Subscribe to events"""
        self.subscribers.append(subscriber)
        if self._bg_task and not self._subscriber_task:
            self._subscriber_task = asyncio.create_task(self._run_subscribers())

    def _get_page_indexes(self) -> list[tuple[int, int]]:
        page_names = os.listdir(self._page_dir)
        page_indexes = [
            tuple(int(i) for i in page_name.split("-")) for page_name in page_names
        ]
        page_indexes.sort(key=lambda n: n[0], reverse=True)
        return page_indexes

    def _iter_events_from(self, current_event_id: int) -> Iterator[QueueEvent[T]]:
        page_indexes = self._get_page_indexes()
        if page_indexes:
            first_id_in_next_page = page_indexes[0][1]
            while current_event_id >= first_id_in_next_page:
                event = self.get_event(current_event_id)
                yield event
                current_event_id -= 1

        for start, end in page_indexes:
            with open(self._page_dir / f"{start}-{end}") as f:
                page: FilesystemPage = self.page_serializer.deserialize(f.read())
            while current_event_id >= page.offset:
                event = page.events[current_event_id - page.offset]
                yield event
                current_event_id -= 1

    async def get_events(
        self,
        page_id: Optional[str] = None,
        limit: Optional[int] = 100,
        created_at__min: Optional[datetime] = None,
        created_at__max: Optional[datetime] = None,
        status__eq: Optional[EventStatus] = None,
    ) -> Page[QueueEvent[T]]:
        """Get events matching the criteria"""
        current_event_id = self._next_event_id
        if page_id:
            current_event_id = int(page_id)

        items = []
        next_page_id = None
        for event in self._iter_events_from(current_event_id):
            if created_at__min and event.created_at < created_at__min:
                continue
            if created_at__max and event.created_at > created_at__max:
                continue
            if status__eq and event.status != status__eq:
                continue
            if len(items) >= limit:
                next_page_id = str(event.id)
                break
            items.append(event)

        return Page(items=items, next_page_id=next_page_id)

    async def count_events(
        self,
        created_at__min: Optional[datetime] = None,
        created_at__max: Optional[datetime] = None,
        status__eq: Optional[EventStatus] = None,
    ) -> int:
        """Get the number of events matching the criteria"""
        if created_at__max is None and created_at__min is None and status__eq is None:
            result = len(os.listdir(self._event_dir))
            return result
        result = sum(
            1 for _ in self.iter_events(created_at__min, created_at__max, status__eq)
        )
        return result

    async def get_event(self, id: int) -> QueueEvent[T]:
        """Get an event by its ID"""
        event_file = self._event_dir / str(id)
        with open(event_file, "r") as f:
            payload = self.serializer.deserialize(f.read())
        meta_file = self._meta_dir / str(id)
        with open(meta_file, "r") as f:
            meta: dict[str, str | int] = json.load(f)
        return QueueEvent(
            id=id,
            payload=payload,
            status=EventStatus[meta.get("status")],
            created_at=datetime.fromisoformat(meta.get("created_at")),
        )

    async def __aenter__(self):
        self._bg_task = asyncio.create_task(self._run_in_bg())
        if self.subscribers:
            self._subscriber_task = asyncio.create_task(self._run_subscribers())

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._bg_task:
            self._bg_task.cancel()

    def _create_event_files(self, payload: T) -> int:
        event_data = self.serializer.serialize(payload)
        while True:
            event_id = self._next_event_id
            try:
                meta_file = self._meta_dir / event_id
                with open(meta_file, "x") as f:
                    json.dump(
                        {
                            "status": EventStatus.PENDING.value,
                            "created_at": datetime.now(UTC).isoformat(),
                            "size_in_bytes": len(event_data),
                        },
                        f,
                    )

                event_file = self._event_dir / event_id
                with open(event_file, "x") as f:
                    f.write(event_data)
                self._next_event_id = event_id + 1
                return event_id
            except FileExistsError:
                self.recalculate_next_event_id()

    def _refresh_worker_ids(self) -> None:
        """
        Workers periodically register themselves by writing a file WorkerID_timestamp into workers/
        (The EventQueueManager periodically deletes old entries from this file.)
        """
        threshold = int(time.time()) - (self.bg_task_delay * 2)
        worker_ids = set()
        removed_ids = set()
        for worker_name in os.listdir(self._worker_dir):
            try:
                worker_id, timestamp = worker_name.split("_")
                if int(timestamp) < threshold:
                    os.remove(self._worker_dir / worker_name)
                    removed_ids.add(worker_id)
                    continue
                worker_ids.add(UUID(worker_id))
            except Exception:
                _LOGGER.warning(
                    "invalid_worker_name:{worker_name}", exc_info=True, stack_info=True
                )
        removed_ids -= worker_ids
        self.worker_ids = list(worker_ids)
        for worker_id in removed_ids:
            # Any events which are still running here may not have been processed as the node may have died so we repeat.
            self._reassign_worker_events(worker_id)

    async def _run_in_bg(self):
        try:
            while True:
                await self._refresh_worker_ids()
                await self._maybe_build_page()
                await asyncio.sleep(self.bg_task_delay)
        except asyncio.CancelledError:
            _LOGGER.info(f"file_system_event_queue_suspended")

    def _get_assigned_events(self, worker_id: UUID) -> set[int]:
        try:
            worker_assignment_dir = self._assignment_dir / worker_id.hex
            events = set()
            for filename in os.listdir(worker_assignment_dir):
                try:
                    events.add(int(filename))
                except ValueError:
                    _LOGGER.error(
                        f"invalid_file_name:{worker_assignment_dir}/{filename}"
                    )
            return events
        except FileNotFoundError:
            return set

    def _remove_assigned_event(self, worker_id: UUID, event_id: int) -> None:
        event_file = self._assignment_dir / worker_id.hex / event_id
        try:
            os.remove(event_file)
        except FileNotFoundError:
            _LOGGER.error(f"no_assignment:{event_file}")

    def _reassign_worker_events(self, worker_id: UUID):
        worker_assignment_dir = self._assignment_dir / worker_id.hex

        # Reassign all events - delete and recreate
        events = os.listdir(worker_assignment_dir)
        for event in events:
            try:
                event_id = UUID(event)
                os.remove(worker_assignment_dir / event)
                self._assign_event(event_id)
            except Exception:
                _LOGGER.error(f"error_reassigning:{worker_assignment_dir / event}")

        # Completely remove the assignment directory for the worker
        try:
            os.rmdir(worker_assignment_dir)
        except FileNotFoundError:
            _LOGGER.error(f"no_assignment_dir:{worker_assignment_dir}")

    def _assign_event(self, id: str) -> None:
        worker_id: UUID = random.choices(self.worker_ids)
        worker_assignment_dir = self._assignment_dir / worker_id.hex
        os.makedirs(worker_assignment_dir, exist_ok=True)
        assignment = worker_assignment_dir / str(id)
        assignment.touch(exist_ok=True)

    async def _run_subscribers(self):
        try:
            while True:
                for event_id in self._get_assigned_events(self.worker_id):
                    event = await self.get_event(event_id)
                    size_in_bytes = len(self.serializer.serialize(event))
                    meta_file = self._meta_dir / event_id
                    with open(meta_file, "x") as f:
                        json.dump(
                            {
                                "status": EventStatus.PROCESSING.value,
                                "created_at": event.created_at.isoformat(),
                                "size_in_bytes": size_in_bytes,
                            }
                        )
                    final_status = EventStatus.PROCESSED
                    for subscriber in list(self.subscribers):
                        try:
                            await subscriber.on_event(event)
                        except Exception:
                            final_status = EventStatus.ERROR
                            # Log and continue notifying other subscribers even if one fails
                            _LOGGER.error(
                                "subscriber_error", exc_info=True, stack_info=True
                            )
                    with open(meta_file, "x") as f:
                        json.dump(
                            {
                                "status": final_status.value,
                                "created_at": event.created_at.isoformat(),
                            }
                        )
                await asyncio.sleep(self.subscriber_task_delay)
        except asyncio.CancelledError:
            _LOGGER.info(f"file_system_event_queue_suspended")

    async def _maybe_build_page(self) -> None:
        page_indexes = self._get_page_indexes()
        first_event_id = page_indexes[0][1] if page_indexes else 0
        events = os.listdir(self._event_dir)
        event_ids = []
        for event in events:
            event_id = int(event)
            if event_id < first_event_id:
                continue
            event_ids.append(event_id)
        event_ids.sort()

        total_event_size = 0
        for event_id in event_ids:
            if (event_id - first_event_id) >= self.max_events_per_page:
                self._build_page(
                    first_event_id, first_event_id + self.max_events_per_page
                )
                return
            with open(self._meta_dir / event) as f:
                meta: dict[str, str | int] = json.load(f)
            total_event_size += meta["size_in_bytes"]
            if total_event_size >= self.max_page_size_bytes:
                self._build_page(first_event_id, event_id + 1)
                return

    async def _build_page(self, start: int, end: int) -> None:
        page_name = f"{start}-{end}"
        events = [self.get_event(id) for id in range(start, end)]
        page = FilesystemPage(start, events)
        with open(self._page_dir / page_name) as f:
            f.write(self.page_serializer.serialize(page))
