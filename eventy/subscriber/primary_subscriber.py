from dataclasses import dataclass
from typing import TypeVar
from uuid import UUID

from eventy.queue_event import QueueEvent
from eventy.subscriber.subscriber import Subscriber

T = TypeVar("T")


@dataclass
class PrimarySubscriber(Subscriber[T]):
    """
    Subscriber for an event queue which only executes its callback if the current worker
    is the primary for the event.
    """

    subscriber: Subscriber[T]

    async def on_event(
        self, event: QueueEvent[T], current_worker_id: UUID, primary_worker_id: UUID
    ) -> None:
        if current_worker_id == primary_worker_id:
            await self.subscriber.on_event(event, current_worker_id, primary_worker_id)
