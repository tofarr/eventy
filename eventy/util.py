

import importlib
import os
from typing import TypeVar

T = TypeVar('T')


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
    parts = qual_name.split('.')
    module_name = '.'.join(parts[:-1])
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

