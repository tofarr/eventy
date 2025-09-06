from enum import Enum


class EventStatus(Enum):
    """Status of an event in the queue"""

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    PROCESSED = "PROCESSED"
    ERROR = "ERROR"
