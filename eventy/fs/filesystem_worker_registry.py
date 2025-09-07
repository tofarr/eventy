import asyncio
from dataclasses import dataclass
import logging
from pathlib import Path
import time
from uuid import UUID

from eventy.fs.filesystem_worker_status import FilesystemWorkerStatus

_LOGGER = logging.getLogger(__name__)


@dataclass
class FilesystemWorkerRegistry:
    """
    Class for tracking concurrent workers using the file system. Each worker periodically
    writes a file indicating it is alive
    """

    worker_id: UUID
    worker_dir: Path
    accepting_events: bool = True
    sleep_time: int = 5
    active_time: int | None = None
    delete_time: int | None = None
    worker_statuses: dict[UUID, FilesystemWorkerStatus] | None = None
    _bg_task: asyncio.Task | None = None

    def __post_init__(self):
        """Validate and set reasonable defaults for active_time and delete_time"""
        self.worker_dir.mkdir(parents=True, exist_ok=True)
        assert self.sleep_time > 0
        if self.active_time is None:
            self.active_time = self.sleep_time * 3
        if self.delete_time is None:
            self.delete_time = self.active_time * 2

    async def __aenter__(self):
        if self._bg_task is None:
            self._bg_task = asyncio.create_task(self._run_bg_task())

    async def __aexit__(self, exc_type, exc_value, traceback):
        if self._bg_task:
            self._bg_task.cancel()

    async def _run_bg_task(self):
        _LOGGER.info("worker_registry_started")
        try:
            while True:
                self._add_status()
                self._refresh_worker_ids()
                await asyncio.sleep(self.sleep_time)
        except asyncio.CancelledError:
            _LOGGER.info("worker_registry_stopped")

    def _add_status(self):
        status_file = (
            self.worker_dir
            / f"{self.worker_id.hex}-{int(time.time())}-{self.accepting_events}"
        )
        status_file.touch()

    def _get_worker_statuses(self) -> list[FilesystemWorkerStatus]:
        statuses = []
        for status_file in self.worker_dir.iterdir():
            try:
                id, timestamp, accepting_events = status_file.name.split("-")
                statuses.append(
                    FilesystemWorkerStatus(
                        id=UUID(id),
                        timestamp=int(timestamp),
                        accepting_events=accepting_events == "1",
                    )
                )
            except Exception:
                _LOGGER.error(
                    f"error_getting_worker_status:{status_file}",
                    exc_info=True,
                    stack_info=True,
                )
        return statuses

    def _refresh_worker_ids(self) -> None:
        """
        Workers periodically register themselves by writing a file WorkerID_timestamp into the workers dir
        """
        statuses = self._get_worker_statuses()
        now = int(time.time())
        active_threshold = now - self.active_time
        delete_threshold = now - self.delete_time
        worker_statuses: dict[UUID, FilesystemWorkerStatus] = {}
        for status in statuses:
            if status.timestamp < delete_threshold:
                status_file = self.worker_dir / f"{status.worker_id}-{status.timestamp}"
                try:
                    status_file.unlink()
                except Exception:
                    _LOGGER.error(
                        f"error_removing_ping:{status_file}",
                        exc_info=True,
                        stack_info=True,
                    )
            elif status.timestamp >= active_threshold:
                existing = worker_statuses.get(status.worker_id)
                if not existing or status.timestamp > existing.timestamp:
                    worker_statuses[status.worker_id] = status
        self.worker_statuses = worker_statuses
