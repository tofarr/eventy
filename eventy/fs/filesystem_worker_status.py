from dataclasses import dataclass
from uuid import UUID


@dataclass
class FilesystemWorkerStatus:
    """Status of a worker - typically embedded in a file name so all can be read with a single os.listdir"""

    worker_id: UUID
    timestamp: int
    """Timestamp when this worker last declared its status"""
    accepting_events: bool
    """A worker may be present but not accepting events (starting up / shutting down)"""
