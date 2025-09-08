from typing import TypeVar
from uuid import UUID

from eventy.queue_event import QueueEvent
from eventy.subscriber.subscriber import Subscriber

T = TypeVar("T")


class PrimarySubscriber(Subscriber[T]):
    """
    Subscriber for an event queue which only executes its callback if the current worker
    is the primary for the event.
    """

    async def on_worker_event(
        self, event: QueueEvent[T], current_worker_id: UUID, primary_worker_id: UUID
    ) -> None:
        if current_worker_id == primary_worker_id:
            await self.on_event(event)
