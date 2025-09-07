import json
from typing import TypeVar, Any

from eventy.serializers.serializer import Serializer

T = TypeVar("T")


class JsonSerializer(Serializer[T]):
    """JSON serializer implementation for objects that can be JSON-serialized"""

    is_json: bool = True

    def __init__(self, ensure_ascii: bool = False, indent: int | None = None):
        """Initialize the JSON serializer

        Args:
            ensure_ascii: If True, escape non-ASCII characters in JSON strings
            indent: Number of spaces for indentation (None for compact output)
        """
        self.ensure_ascii = ensure_ascii
        self.indent = indent

    def serialize(self, obj: T) -> bytes:
        """Serialize an object to JSON bytes

        Args:
            obj: The object to serialize (must be JSON-serializable)

        Returns:
            bytes: The JSON representation as UTF-8 bytes

        Raises:
            TypeError: If the object is not JSON-serializable
        """
        json_str = json.dumps(
            obj,
            ensure_ascii=self.ensure_ascii,
            indent=self.indent,
            default=self._default_handler,
        )
        return json_str.encode("utf-8")

    def deserialize(self, data: bytes) -> T:
        """Deserialize JSON bytes back to an object

        Args:
            data: The JSON bytes (UTF-8 encoded)

        Returns:
            T: The deserialized object

        Raises:
            json.JSONDecodeError: If the data is not valid JSON
            UnicodeDecodeError: If the data is not valid UTF-8
        """
        json_str = data.decode("utf-8")
        return json.loads(json_str)

    def _default_handler(self, obj: Any) -> Any:
        """Default handler for non-JSON-serializable objects

        This method can be overridden in subclasses to handle custom types.

        Args:
            obj: The object that couldn't be serialized

        Returns:
            A JSON-serializable representation of the object

        Raises:
            TypeError: If the object cannot be made JSON-serializable
        """
        # Handle common non-JSON types
        if hasattr(obj, "__dict__"):
            return obj.__dict__
        elif hasattr(obj, "isoformat"):  # datetime objects
            return obj.isoformat()
        else:
            raise TypeError(
                f"Object of type {type(obj).__name__} is not JSON serializable"
            )
