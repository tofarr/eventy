from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from eventy.event_status import EventStatus


@dataclass
class FilesystemEventMeta:
    """Meta about an event stored in the filesystem"""

    status: EventStatus
    created_at: datetime
    size_in_bytes: int

    def to_json(self):
        return {
            "status": self.status.value,
            "created_at": self.created_at.isoformat(),
            "size_in_bytes": self.size_in_bytes,
        }


def from_json(value: dict[str, Any]) -> FilesystemEventMeta:
    return FilesystemEventMeta(
        status=EventStatus(value["status"]),
        created_at=datetime.fromisoformat(value["created_at"]),
        size_in_bytes=value["size_in_bytes"],
    )
