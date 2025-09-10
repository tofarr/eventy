from dataclasses import dataclass
from typing import Generic, TypeVar
from eventy.event_queue import EventQueue
from eventy.queue_event import QueueEvent
from eventy.subscribers.subscriber import Subscriber

T = TypeVar("T")


def nonce_subscriber(wrap_type: type[Subscriber[T]]):
    
    @dataclass
    class NonceSubscriber(Generic[T], wrap_type):
        """A subscriber which ensures that the nested subscriber is called only once no matter how many workers are attached"""

        subscriber: Subscriber[T]

        @staticmethod
        @property
        def get_payload_type() -> type[T]:
            return wrap_type.get_payload_type()

        async def on_event(self, event: QueueEvent[T], event_queue: EventQueue[T]) -> None:
            claim_id = f"{event.id}_started"
            claim_created = event_queue.create_claim(claim_id)
            if claim_created:
                await self.subscriber.on_event(event)

    NonceSubscriber.__name__ = f"{wrap_type.get_payload_type().__name__}NonceSubscriber"
    return NonceSubscriber