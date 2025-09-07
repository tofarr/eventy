import importlib
import logging
import os
from typing import TypeVar

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


def import_from(qual_name: str):
    """Import a value from its fully qualified name.

    This function is a utility to dynamically import any Python value (class, function, variable)
    from its fully qualified name. For example, 'openhands.server.user_auth.UserAuth' would
    import the UserAuth class from the openhands.server.user_auth module.

    Args:
        qual_name: A fully qualified name in the format 'module.submodule.name'
                  e.g. 'openhands.server.user_auth.UserAuth'

    Returns:
        The imported value (class, function, or variable)

    Example:
        >>> UserAuth = import_from('openhands.server.user_auth.UserAuth')
        >>> auth = UserAuth()
    """
    parts = qual_name.split(".")
    module_name = ".".join(parts[:-1])
    module = importlib.import_module(module_name)
    result = getattr(module, parts[-1])
    return result


def get_impl(key: str, base_type: type[T], default_type: type) -> type[T]:
    value = os.getenv(key)
    if not value:
        assert issubclass(default_type, base_type)
        return default_type
    imported_type = import_from(value)
    assert issubclass(imported_type, base_type)
    return imported_type


def get_impls(key: str, base_type: type[T], default_types: list[type] | None = None) -> list[type[T]]:
    """
    Load types from the key environment variable.
    
    The environment variable should contain a comma-separated list of fully qualified names.
    Each name should either:
    1. Point to a class directly, or
    2. Point to a function that returns a list of classes when called without arguments
    
    Returns:
        A list of class types
    """
    env_value = os.getenv(key, "")
    if not env_value.strip():
        return default_types or []
    
    implementing_types = []
    
    # Split by comma and strip whitespace from each part
    type_names = [name.strip() for name in env_value.split(",") if name.strip()]
    
    for type_name in type_names:
        try:
            imported_item = import_from(type_name)
            
            # Check if it's an implementation class
            if (isinstance(imported_item, type) and 
                issubclass(imported_item, base_type)):
                implementing_types.append(imported_item)
            else:
                # Assume it's a function that returns a list of implementation classes
                result = imported_item()
                if isinstance(result, list):
                    for item in result:
                        if (isinstance(item, type) and 
                            issubclass(item, base_type)):
                            implementing_types.append(item)
        except Exception as e:
            # Log error for invalid imports but continue processing
            _LOGGER.error(f"Failed to import type '{type_name}': {e}")
            continue
    
    return implementing_types