import pickle
from typing import TypeVar

from eventy.serializers.serializer import Serializer

T = TypeVar("T")


class PickleSerializer(Serializer[T]):
    """Default serializer implementation using Python's pickle module"""

    is_json: bool = False

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


def foo():
    class Foo:
        def get_value(self):
            return 10

    return Foo


class AbstractBar:
    pass


@staticmethod
def get_value():
    return 10


def bar():
    return type("Bar", (AbstractBar,), {"get_value": get_value})


if __name__ == "__main__":
    import pickle

    a = bar()
    print(a().get_value())
    print(pickle.dumps(a()))
