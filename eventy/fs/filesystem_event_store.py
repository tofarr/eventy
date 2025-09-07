import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime
import json
import logging
import os
from pathlib import Path
from typing import Generic, Iterator, Tuple, TypeVar
from uuid import UUID

from eventy.event_status import EventStatus
from eventy.fs.filesystem_event_meta import FilesystemEventMeta, from_json
from eventy.fs.filesystem_page import FilesystemPage
from eventy.queue_event import QueueEvent
from eventy.serializers.serializer import Serializer, get_default_serializer

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


@dataclass
class FilesystemEventStore(Generic[T]):
    payload_dir: Path
    page_dir: Path
    meta_dir: Path

    max_events_per_page: int = 25
    max_page_size_bytes: int = 1024 * 1024  # 1MB

    payload_serializer: Serializer[T] = field(default_factory=get_default_serializer)
    page_serializer: Serializer[FilesystemPage[T]] = field(
        default_factory=get_default_serializer
    )
    bg_task_delay: int = 15
    _bg_task: asyncio.Task | None = None

    def __post_init__(self):
        self.payload_dir.mkdir(parents=True, exist_ok=True)
        self.page_dir.mkdir(parents=True, exist_ok=True)
        self.meta_dir.mkdir(parents=True, exist_ok=True)
        event_ids = list(self._get_all_event_ids())
        self.next_event_id = max(event_ids) + 1 if event_ids else 1

    async def __aenter__(self):
        if self._bg_task is None:
            self._bg_task = asyncio.create_task(self._run_in_bg())

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._bg_task:
            self._bg_task.cancel()

    def add_event(self, payload: T) -> Tuple[int, FilesystemEventMeta]:
        payload_data = self.payload_serializer.serialize(payload)
        while True:
            event_id = self.next_event_id
            try:
                payload_file = self.payload_dir / str(event_id)
                with open(payload_file, "xb") as f:
                    f.write(payload_data)
                self.next_event_id = event_id + 1
            except FileExistsError:
                self.next_event_id = max(self._get_all_event_ids()) + 1
                continue

            meta = FilesystemEventMeta(
                status=EventStatus.PROCESSING,
                created_at=datetime.now(UTC),
                size_in_bytes=len(payload_data),
            )

            meta_file = self.meta_dir / str(event_id)
            with open(meta_file, "w") as f:
                json.dump(meta.to_json(), f)

            return event_id, meta

    def iter_events_from(self, current_event_id: int) -> Iterator[QueueEvent[T]]:
        page_indexes = self._get_page_indexes()
        
        if page_indexes:
            # Handle events newer than the most recent page
            first_id_in_next_page = page_indexes[0][1]
            while current_event_id >= first_id_in_next_page:
                event = self.get_event(current_event_id)
                yield event
                current_event_id -= 1

            # Handle events in pages
            for start, end in page_indexes:
                with open(self.page_dir / f"{start}-{end}", "rb") as f:
                    page: FilesystemPage = self.page_serializer.deserialize(f.read())
                while current_event_id >= page.offset:
                    event = page.events[current_event_id - page.offset]
                    yield event
                    current_event_id -= 1
        else:
            # No pages exist yet, iterate through individual event files
            while current_event_id >= 1:
                try:
                    event = self.get_event(current_event_id)
                    yield event
                    current_event_id -= 1
                except FileNotFoundError:
                    # Event doesn't exist, skip it
                    current_event_id -= 1

    def count_events(self):
        return sum(1 for _ in self.payload_dir.iterdir())

    def get_event(self, event_id: int) -> QueueEvent[T]:
        payload_file = self.payload_dir / str(event_id)
        with open(payload_file, "rb") as f:
            payload = self.payload_serializer.deserialize(f.read())
        meta = self._get_meta(event_id)
        return QueueEvent(
            id=event_id,
            payload=payload,
            status=meta.status,
            created_at=meta.created_at,
        )

    def update_event_status(self, event_id: int, status: EventStatus):
        if status == EventStatus.PENDING:
            return  # Cant go back to pending
        meta = self._get_meta(event_id)
        if meta.status == status:
            return
        if meta.status in (EventStatus.ERROR, EventStatus.PROCESSED):
            return
        meta.status = status
        meta_file = self.meta_dir / str(event_id)
        with open(meta_file, "w") as f:
            json.dump(meta.to_json(), f)

    def _get_page_indexes(self) -> list[tuple[int, int]]:
        page_indexes = [
            tuple(int(i) for i in page.name.split("-"))
            for page in self.page_dir.iterdir()
        ]
        page_indexes.sort(key=lambda n: n[0], reverse=True)
        return page_indexes

    def _get_all_event_ids(self) -> Iterator[int]:
        for file in self.payload_dir.iterdir():
            try:
                yield int(file.name)
            except Exception:
                _LOGGER.error(
                    "error_calculating_next_page", exc_info=True, stack_info=True
                )

    async def _run_in_bg(self):
        try:
            while True:
                await self._maybe_build_page()
                await asyncio.sleep(self.bg_task_delay)
        except asyncio.CancelledError:
            _LOGGER.info("file_system_event_queue_suspended")

    async def _maybe_build_page(self) -> None:
        page_indexes = self._get_page_indexes()
        first_event_id = page_indexes[0][1] if page_indexes else 0
        event_ids = []
        for payload_file in self.payload_dir.iterdir():
            try:
                event_id = int(payload_file.name)
                if event_id < first_event_id:
                    continue
                event_ids.append(event_id)
            except Exception:
                _LOGGER.error("error_checking_page", exc_info=True, stack_info=True)
        event_ids.sort()

        total_event_size = 0
        for event_id in event_ids:
            if (event_id - first_event_id) >= self.max_events_per_page:
                await self._build_page(
                    first_event_id, first_event_id + self.max_events_per_page
                )
                return
            with open(self.meta_dir / str(event_id)) as f:
                meta: dict[str, str | int] = json.load(f)
            total_event_size += meta["size_in_bytes"]
            if total_event_size >= self.max_page_size_bytes:
                await self._build_page(first_event_id, event_id + 1)
                return

    async def _build_page(self, start: int, end: int) -> None:
        page_name = f"{start}-{end}"
        events = [await self.get_event(id) for id in range(start, end)]
        page = FilesystemPage(start, events)
        page_data = self.page_serializer.serialize(page)
        with open(self.page_dir / page_name, "wb") as f:
            f.write(page_data)

    def _get_meta(self, event_id: int) -> FilesystemEventMeta:
        meta_file = self.meta_dir / str(event_id)
        with open(meta_file, "r") as f:
            meta = from_json(json.load(f))
            return meta
