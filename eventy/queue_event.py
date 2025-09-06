from abc import ABC
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from typing import Generic, TypeVar
from uuid import UUID, uuid4

T = TypeVar("T")


class EventStatus(Enum):
    """Status of an event in the queue"""
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    PROCESSED = "PROCESSED"
    ERROR = "ERROR"


@dataclass(frozen=True)
class QueueEvent(Generic[T], ABC):
    """Object representing an event"""

    payload: T
    id: UUID = field(default_factory=uuid4)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    status: EventStatus = field(default=EventStatus.PENDING)
