from abc import ABC, abstractmethod
from eventy.subscriber.subscriber import Subscriber
from eventy.util import get_impl


class EventyConfig(ABC):
    """Configuration object for eventy"""

    @abstractmethod
    def get_subscriber_types(self) -> list[type[Subscriber]]:
        """Get subscriber types"""

    @abstractmethod
    def get_payload_types(self) -> list[type]:
        """Get all payload types"""


config: EventyConfig | None = None


def get_config() -> EventyConfig:
    global config
    if config is None:
        config_type = get_impl("EVENTY_CONFIG", EventyConfig)
        config = config_type()
    return config
