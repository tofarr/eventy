from dataclasses import dataclass
from typing import Generic, TypeVar

from eventy.queue_event import QueueEvent

T = TypeVar("T")


@dataclass
class FilesystemPage(Generic[T]):
    offset: int
    events: list[QueueEvent[T]]
