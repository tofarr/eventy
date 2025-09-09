from dataclasses import dataclass, field
from datetime import UTC, datetime
from uuid import UUID, uuid4


@dataclass(frozen=True)
class Claim:
    """A claim represents a worker's claim on a job or resource."""
    
    id: str
    worker_id: UUID
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))