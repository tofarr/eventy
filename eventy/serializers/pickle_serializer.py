import pickle
from typing import TypeVar

from eventy.serializers.serializer import Serializer

T = TypeVar("T")


class PickleSerializer(Serializer[T]):
    """Default serializer implementation using Python's pickle module"""

    def serialize(self, obj: T) -> bytes:
        """Serialize an object to bytes using pickle

        Args:
            obj: The object to serialize

        Returns:
            bytes: The pickled representation
        """
        return pickle.dumps(obj)

    def deserialize(self, data: bytes) -> T:
        """Deserialize bytes back to an object using pickle

        Args:
            data: The pickled bytes

        Returns:
            T: The unpickled object
        """
        return pickle.loads(data)
