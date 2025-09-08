from dataclasses import dataclass
from typing import Any
from uuid import UUID


@dataclass
class FilesystemProcessMeta:
    """Meta about an event stored in the filesystem"""

    primary_worker_id: UUID
    worker_ids: list[UUID]

    def to_json(self):
        return {
            "primary_worker_id": self.primary_worker_id.hex,
            "worker_ids": [worker_id.hex for worker_id in self.worker_ids],
        }


def from_json(value: dict[str, Any]):
    return FilesystemProcessMeta(
        primary_worker_id=UUID(value["primary_worker_id"]),
        worker_ids=[UUID(worker_id) for worker_id in value["worker_ids"]],
    )
