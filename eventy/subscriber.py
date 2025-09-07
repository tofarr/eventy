import logging
import os
from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from eventy.queue_event import QueueEvent
from eventy.util import import_from

T = TypeVar("T")

logger = logging.getLogger(__name__)


class Subscriber(Generic[T], ABC):
    """
    Subscriber for an event queue. The subscriber object should be stateless, though it may load
    data from a persistence layer.
    """

    payload_type: type[T]

    @abstractmethod
    async def on_event(self, event: QueueEvent[T]) -> None:
        """Callback for when an event occurs"""


def get_public_subscriber_types() -> list[type[Subscriber]]:
    """
    Load public subscriber types from the EVENTY_SUBSCRIBER_TYPES environment variable.
    
    The environment variable should contain a comma-separated list of fully qualified names.
    Each name should either:
    1. Point to a Subscriber class directly, or
    2. Point to a function that returns a list of Subscriber classes when called without arguments
    
    Returns:
        A list of Subscriber class types
    """
    env_value = os.getenv("EVENTY_SUBSCRIBER_TYPES", "")
    if not env_value.strip():
        return []
    
    subscriber_types = []
    
    # Split by comma and strip whitespace from each part
    type_names = [name.strip() for name in env_value.split(",") if name.strip()]
    
    for type_name in type_names:
        try:
            imported_item = import_from(type_name)
            
            # Check if it's a Subscriber class
            if (isinstance(imported_item, type) and 
                issubclass(imported_item, Subscriber)):
                subscriber_types.append(imported_item)
            else:
                # Assume it's a function that returns a list of Subscriber classes
                result = imported_item()
                if isinstance(result, list):
                    for item in result:
                        if (isinstance(item, type) and 
                            issubclass(item, Subscriber)):
                            subscriber_types.append(item)
        except Exception as e:
            # Log error for invalid imports but continue processing
            logger.error(f"Failed to import subscriber type '{type_name}': {e}")
            continue
    
    return subscriber_types
