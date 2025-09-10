from dataclasses import dataclass
from typing import TypeVar

from pydantic import TypeAdapter
from eventy.serializers.serializer import Serializer

T = TypeVar("T")


@dataclass
class PydanticSerializer(Serializer[T]):
    type_adapter: TypeAdapter[T]
    is_json: bool = True

    def serialize(self, obj: T) -> bytes:
        result = self.type_adapter.dump_json(obj)
        return result

    def deserialize(self, data: bytes) -> T:
        result = self.type_adapter.validate_json(data)
        return result
