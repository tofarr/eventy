from abc import ABC, abstractmethod
from eventy.constants import EVENTY_CONFIG
from eventy.subscribers.subscriber import Subscriber
from eventy.util import get_impl


class EventyConfig(ABC):
    """Configuration object for eventy"""

    @abstractmethod
    def get_subscriber_types(self) -> list[type[Subscriber]]:
        """Get subscriber types"""

    @abstractmethod
    def get_payload_types(self) -> list[type]:
        """Get all payload types"""


def get_config() -> EventyConfig:
    config_type = get_impl(EVENTY_CONFIG, EventyConfig)
    return config_type()
