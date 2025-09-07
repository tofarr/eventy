from abc import ABC
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Generic, TypeVar

from eventy.event_status import EventStatus

T = TypeVar("T")


@dataclass(frozen=True)
class QueueEvent(Generic[T], ABC):
    """Object representing an event"""

    id: int
    payload: T
    status: EventStatus = field(default=EventStatus.PENDING)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    
