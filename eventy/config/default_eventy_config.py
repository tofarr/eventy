from dataclasses import dataclass, field
from eventy.config.eventy_config import EventyConfig
from eventy.subscribers.subscriber import Subscriber


@dataclass
class DefaultEventyConfig(EventyConfig):
    """Configuration object for eventy"""
    payload_types: list[type]
    subscriber_types: list[type[Subscriber]] = field(default_factory=list)

    def get_subscriber_types(self) -> list[type[Subscriber]]:
        return self.subscriber_types

    def get_payload_types(self) -> list[type]:
        return self.payload_types
