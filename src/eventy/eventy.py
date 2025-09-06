from abc import ABC
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Generic, TypeVar
from uuid import UUID, uuid4

T = TypeVar("T")


@dataclass
class Eventy(Generic[T], ABC):
    """Object representing an event"""

    payload: T
    id: UUID = field(default_factory=uuid4)
    created_at = field(default_factory=lambda: datetime.now(UTC))
