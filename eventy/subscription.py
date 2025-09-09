from dataclasses import dataclass
from typing import Generic, TypeVar
from uuid import UUID

from eventy.subscribers.subscriber import Subscriber

T = TypeVar("T")


@dataclass
class Subscription(Generic[T]):
    id: UUID
    subscriber: Subscriber[T]
