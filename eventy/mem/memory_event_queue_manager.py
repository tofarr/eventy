import asyncio
from dataclasses import dataclass, field
from typing import TypeVar, Dict, Type, Optional, cast

from eventy.event_queue import EventQueue
from eventy.queue_event import QueueEvent
from eventy.subscriber import Subscriber
from eventy.mem.memory_event_queue import MemoryEventQueue
from eventy.mem.serializer import Serializer, get_default_serializer

T = TypeVar("T")


@dataclass
class MemoryEventQueueManager:
    """Manager for memory event queues that creates them lazily by payload type"""
    
    serializer: Serializer = field(default_factory=get_default_serializer)
    queues: Dict[Type, MemoryEventQueue] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def get_queue(self, payload_type: Type[T]) -> MemoryEventQueue[T]:
        """Get or create an event queue for the specified payload type
        
        Args:
            payload_type: The type of payload this queue will handle
            
        Returns:
            MemoryEventQueue[T]: The event queue for this payload type
        """
        async with self.lock:
            if payload_type not in self.queues:
                # Create a new queue for this payload type
                queue = MemoryEventQueue[T](
                    event_type=payload_type,
                    serializer=cast(Serializer[QueueEvent[T]], self.serializer)
                )
                self.queues[payload_type] = queue
            
            return cast(MemoryEventQueue[T], self.queues[payload_type])

    async def publish(self, payload_type: Type[T], event: QueueEvent[T]) -> None:
        """Publish an event to the queue for the specified payload type
        
        Args:
            payload_type: The type of payload
            event: The event to publish
        """
        queue = await self.get_queue(payload_type)
        await queue.publish(event)

    async def subscribe(self, payload_type: Type[T], subscriber: Subscriber[T]) -> None:
        """Subscribe to events for the specified payload type
        
        Args:
            payload_type: The type of payload to subscribe to
            subscriber: The subscriber to add
        """
        queue = await self.get_queue(payload_type)
        await queue.subscribe(subscriber)

    def get_queue_types(self) -> list[Type]:
        """Get all payload types that have queues created
        
        Returns:
            List of payload types with existing queues
        """
        return list(self.queues.keys())

    def get_queue_count(self) -> int:
        """Get the number of queues currently managed
        
        Returns:
            Number of queues
        """
        return len(self.queues)

    async def clear_queue(self, payload_type: Type[T]) -> None:
        """Clear all events from a specific queue
        
        Args:
            payload_type: The payload type whose queue to clear
        """
        async with self.lock:
            if payload_type in self.queues:
                self.queues[payload_type].clear()

    async def clear_all_queues(self) -> None:
        """Clear all events from all queues"""
        async with self.lock:
            for queue in self.queues.values():
                queue.clear()

    async def remove_queue(self, payload_type: Type[T]) -> bool:
        """Remove a queue entirely
        
        Args:
            payload_type: The payload type whose queue to remove
            
        Returns:
            True if queue was removed, False if it didn't exist
        """
        async with self.lock:
            if payload_type in self.queues:
                del self.queues[payload_type]
                return True
            return False

    def get_queue_stats(self) -> Dict[Type, int]:
        """Get event count statistics for all queues
        
        Returns:
            Dictionary mapping payload types to their event counts
        """
        return {
            payload_type: queue.get_event_count()
            for payload_type, queue in self.queues.items()
        }