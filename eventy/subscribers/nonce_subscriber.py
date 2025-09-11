from dataclasses import dataclass
from typing import Generic, TypeVar
from eventy.event_queue import EventQueue
from eventy.eventy_error import SkipException
from eventy.queue_event import QueueEvent
from eventy.subscribers.subscriber import Subscriber

T = TypeVar("T")


@dataclass
class NonceSubscriber(Generic[T], Subscriber[T]):
    """A subscriber which ensures that the nested subscriber is called only once no matter how many workers are attached"""

    subscriber: Subscriber[T]

    async def on_event(self, event: QueueEvent[T], event_queue: EventQueue[T]) -> None:
        claim_id = f"{event.id}_started"
        claim_created = await event_queue.create_claim(claim_id)
        if claim_created:
            await self.subscriber.on_event(event, event_queue)
        else:
            raise SkipException(
                f"Event {event.id} is being skipped on worker {event_queue.get_worker_id()} - claim already exists"
            )
