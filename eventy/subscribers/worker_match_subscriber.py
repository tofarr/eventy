

from dataclasses import dataclass
from typing import TypeVar
from uuid import UUID
from eventy.event_queue import EventQueue
from eventy.queue_event import QueueEvent
from eventy.subscribers.subscriber import Subscriber

T = TypeVar('T')


@dataclass
class WorkerMatchSubscriber(Subscriber[T]):
    subscriber: Subscriber[T]
    worker_id: UUID

    @property
    def payload_type(self) -> type[T]:
        return self.subscriber.payload_type    

    async def on_event(
        self, event: QueueEvent[T], event_queue: EventQueue[T]
    ) -> None:
        """Subscriber which makes sure the current worker matches a preset

        Args:
            event: The queue event that occurred
            event_queue: The event queue instance that can be used to access queue functionality
        """
        if event_queue.get_worker_id() == self.worker_id:
            await self.subscriber.on_event(event, event_queue)
