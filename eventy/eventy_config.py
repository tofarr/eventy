


from abc import ABC
from eventy.subscriber.subscriber import Subscriber
from eventy.util import get_impl


class EventyConfig(ABC):
    """ Configuration object for eventy """

    def get_subscriber_types(self) -> list[type[Subscriber]]:
        """ Get subscriber types """

    def get_payload_types(self) -> list[type]:
        """ Get all payload types """


def get_config() -> EventyConfig:
    config_type = get_impl("EVENTY_CONFIG", EventyConfig)
    return config_type()


def get_configs() -> list[EventyConfig]:
    """Get all eventy configurations.
    
    By default, returns a single configuration instance from get_config().
    This can be overridden by setting EVENTY_CONFIGS environment variable
    to a comma-separated list of fully qualified class names.
    
    Returns:
        list[EventyConfig]: List of configuration instances
    """
    import os
    from eventy.util import import_from
    
    configs_env = os.getenv("EVENTY_CONFIGS")
    if configs_env:
        # Parse comma-separated list of config class names
        config_instances = []
        for config_name in configs_env.split(","):
            config_name = config_name.strip()
            if config_name:
                try:
                    config_type = import_from(config_name)
                    if not issubclass(config_type, EventyConfig):
                        raise TypeError(f"{config_name} is not a subclass of EventyConfig")
                    config_instances.append(config_type())
                except Exception as e:
                    import logging
                    logging.getLogger(__name__).warning(f"Failed to load config {config_name}: {e}")
        return config_instances
    else:
        # Default behavior: return single config if available
        try:
            return [get_config()]
        except Exception:
            # If no config is available, return empty list
            return []
