from abc import ABC, abstractmethod
from typing import TypeVar, Generic

from eventy.util import get_impl

T = TypeVar("T")


class Serializer(Generic[T], ABC):
    """Abstract serializer for converting objects to/from bytes for storage"""

    @abstractmethod
    def serialize(self, obj: T) -> bytes:
        """Serialize an object to bytes

        Args:
            obj: The object to serialize

        Returns:
            bytes: The serialized representation
        """

    @abstractmethod
    def deserialize(self, data: bytes) -> T:
        """Deserialize bytes back to an object

        Args:
            data: The serialized bytes

        Returns:
            T: The deserialized object
        """


def get_default_serializer():
    from eventy.serializers.pickle_serializer import PickleSerializer

    serializer_class = get_impl("EVENTY_SERIALIZER", Serializer, PickleSerializer)
    return serializer_class()
