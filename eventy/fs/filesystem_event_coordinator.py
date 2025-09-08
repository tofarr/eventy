import asyncio
from dataclasses import dataclass
import json
import logging
from pathlib import Path
import random
from typing import Generic, Iterator, Tuple, TypeVar
from uuid import UUID

from eventy.event_status import EventStatus
from eventy.fs.filesystem_event_store import FilesystemEventStore
from eventy.fs.filesystem_process_meta import FilesystemProcessMeta, from_json
from eventy.fs.filesystem_worker_registry import FilesystemWorkerRegistry
from eventy.queue_event import QueueEvent

_LOGGER = logging.getLogger(__name__)
T = TypeVar("T")


@dataclass
class FilesystemEventCoordinator(Generic[T]):
    worker_registry: FilesystemWorkerRegistry
    event_store: FilesystemEventStore
    process_dir: Path
    worker_event_dir: Path
    sleep_time: int = 5
    _bg_task: asyncio.Task | None = None

    def __post_init__(self):
        self.process_dir.mkdir(parents=True, exist_ok=True)
        self.worker_event_dir.mkdir(parents=True, exist_ok=True)

    async def __aenter__(self):
        if self._bg_task is None:
            self._bg_task = asyncio.create_task(self._run_bg_task())

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._bg_task:
            self._bg_task.cancel()

    def schedule_event_for_processing(self, event_id: int) -> FilesystemProcessMeta:
        # Write the process file containing the id of all assigned workers and the primary
        worker_statuses = self.worker_registry.worker_statuses or {}
        worker_ids = list(worker_statuses.keys())
        # If no workers are available, use the current worker as fallback
        if not worker_ids:
            worker_ids = [self.worker_registry.worker_id]
        primary_worker_id = random.choice(worker_ids)
        process_file = self.process_dir / str(event_id)
        event_process = FilesystemProcessMeta(
            primary_worker_id=primary_worker_id,
            worker_ids=worker_ids,
        )
        with open(process_file, "x") as f:
            json.dump(event_process.to_json(), f)

        # Place a file so each worker knows to pick up the event
        for worker_id in worker_ids:
            worker_event_file = self.worker_event_dir / worker_id.hex / str(event_id)
            worker_event_file.parent.mkdir(parents=True, exist_ok=True)
            worker_event_file.touch()

    def get_event_ids_to_process_for_current_worker(self) -> set[int]:
        event_ids = []
        worker_event_dir = self.worker_event_dir / self.worker_registry.worker_id.hex
        if worker_event_dir.exists():
            for event_file in worker_event_dir.iterdir():
                try:
                    event_ids.append(int(event_file.name))
                except Exception:
                    _LOGGER.error(
                        "get_event_ids_to_process_for_current_worker",
                        exc_info=True,
                        stack_info=True,
                    )
        event_ids.sort()
        return set(event_ids)

    def mark_event_processed_for_current_worker(self, event_id: int) -> None:
        worker_event_file = (
            self.worker_event_dir / self.worker_registry.worker_id.hex / str(event_id)
        )
        worker_event_file.unlink(missing_ok=True)

    def get_process_meta(self, event_id: int) -> FilesystemProcessMeta:
        """Get process metadata for a specific event"""
        process_file = self.process_dir / str(event_id)
        with open(process_file) as f:
            return from_json(json.load(f))

    def get_all_process_meta(self) -> Iterator[Tuple[int, FilesystemProcessMeta]]:
        for process_file in self.process_dir.iterdir():
            try:
                with open(process_file) as f:
                    process_meta = from_json(json.load(f))
                    yield int(process_file.name), process_meta
            except Exception:
                _LOGGER.error(
                    f"get_all_process_meta:{process_file}",
                    exc_info=True,
                    stack_info=True,
                )

    def get_status(self, event_id: int, meta: FilesystemProcessMeta) -> EventStatus:
        result = EventStatus.PROCESSED
        for worker_id in meta.worker_ids:

            # If an assigned worker is no longer running, this is an error...
            if worker_id not in self.worker_registry.worker_statuses:
                result = EventStatus.ERROR
                continue

            # If a processing file still exists, we are still processing
            worker_event_file = self.worker_event_dir / worker_id.hex / str(event_id)
            if worker_event_file.exists():
                return EventStatus.PROCESSING

        return result

    def unscheduled_event(self, event_id: int) -> None:
        process_file = self.process_dir / str(event_id)
        process_file.unlink(missing_ok=True)

    def cleanup(self) -> None:
        for event_id, meta in self.get_all_process_meta():
            status = self.get_status(event_id, meta)
            if status != EventStatus.PROCESSING:
                process_file = self.process_dir / str(event_id)
                process_file.unlink(missing_ok=True)
                self.event_store.update_event_status(event_id, status)

    async def _run_bg_task(self):
        _LOGGER.info("event_coordinator_started")
        try:
            while True:
                self.cleanup()
                await asyncio.sleep(self.sleep_time)
        except asyncio.CancelledError:
            _LOGGER.info("event_coordinator_stopped")
