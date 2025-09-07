


from abc import ABC
from eventy.subscriber.subscriber import Subscriber
from eventy.util import get_impls


class EventyConfig(ABC):
    """ Configuration object for eventy """

    def get_subscriber_types(self) -> list[type[Subscriber]]:
        """ Get subscriber types """

    def get_payload_types(self) -> list[type]:
        """ Get all payload types """


def get_configs() -> list[EventyConfig]:
    config_types = get_impls("EVENTY_CONFIG", EventyConfig)
    configs = [config_type() for config_type in config_types]
    return configs
