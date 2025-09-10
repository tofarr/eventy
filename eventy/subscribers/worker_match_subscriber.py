from dataclasses import dataclass
from typing import TypeVar
from uuid import UUID
from eventy.event_queue import EventQueue
from eventy.queue_event import QueueEvent
from eventy.subscribers.subscriber import Subscriber

T = TypeVar("T")



def worker_match_subscriber(wrap_type: type[Subscriber]):

    @dataclass
    class WorkerMatchSubscriber(Subscriber[T]):
        subscriber: Subscriber[T]
        worker_id: UUID

        def get_payload_type(self) -> type[T]:
            return wrap_type.get_payload_type()

        async def on_event(self, event: QueueEvent[T], event_queue: EventQueue[T]) -> None:
            """Subscriber which makes sure the current worker matches a preset

            Args:
                event: The queue event that occurred
                event_queue: The event queue instance that can be used to access queue functionality
            """
            if event_queue.get_worker_id() == self.worker_id:
                await self.subscriber.on_event(event, event_queue)

    WorkerMatchSubscriber.__name__ = f"{wrap_type.get_payload_type().__name__}WorkerMatchSubscriber"
    return WorkerMatchSubscriber
