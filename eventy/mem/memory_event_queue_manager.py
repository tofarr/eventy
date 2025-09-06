import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TypeVar, Dict, Type, cast

from eventy.event_queue import EventQueue
from eventy.queue_event import QueueEvent
from eventy.queue_manager import QueueManager
from eventy.subscriber import Subscriber
from eventy.mem.memory_event_queue import MemoryEventQueue
from eventy.serializers.serializer import Serializer, get_default_serializer

T = TypeVar("T")


@dataclass
class MemoryEventQueueManager(QueueManager):
    """
    Manager for memory event queues that creates them lazily by payload type.
    Suitable for use by a single process and small queue.
    """

    serializer: Serializer = field(default_factory=get_default_serializer)
    max_age: timedelta | None = None
    queues: Dict[Type, MemoryEventQueue] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def get_queue(self, payload_type: Type[T]) -> MemoryEventQueue[T]:
        """Get an event queue for the specified payload type

        Args:
            payload_type: The type of payload this queue will handle

        Returns:
            MemoryEventQueue[T]: The event queue for this payload type

        Raises:
            KeyError: If no queue exists for the given payload type
        """
        async with self.lock:
            if payload_type not in self.queues:
                raise KeyError(f"No queue registered for payload type {payload_type}")
            return cast(MemoryEventQueue[T], self.queues[payload_type])

    async def get_event_queue(self, payload_type: Type[T]) -> EventQueue[T]:
        """Get an event queue for the event type given (QueueManager interface)"""
        return await self.get_queue(payload_type)

    async def publish(self, payload_type: Type[T], payload: T) -> None:
        """Publish a payload to the queue for the specified payload type

        Args:
            payload_type: The type of payload
            payload: The payload to publish
        """
        queue = await self.get_queue(payload_type)
        await queue.publish(payload)

    async def subscribe(self, payload_type: Type[T], subscriber: Subscriber[T]) -> None:
        """Subscribe to events for the specified payload type

        Args:
            payload_type: The type of payload to subscribe to
            subscriber: The subscriber to add
        """
        queue = await self.get_queue(payload_type)
        await queue.subscribe(subscriber)

    async def get_queue_types(self) -> list[type]:
        """List all available event queues."""
        return list(self.queues.keys())

    async def register(self, payload_type: type[T]) -> None:
        """Register a payload type (Create an event queue)"""
        async with self.lock:
            if payload_type not in self.queues:
                # Create a new queue for this payload type
                queue = MemoryEventQueue[T](
                    event_type=payload_type,
                    max_age=self.max_age,
                    serializer=cast(Serializer[T], self.serializer),
                )
                self.queues[payload_type] = queue

    async def deregister(self, payload_type: type[T]) -> None:
        """Deregister a payload type (Shut down an event queue)"""
        async with self.lock:
            if payload_type in self.queues:
                # Remove the queue for this payload type
                del self.queues[payload_type]
