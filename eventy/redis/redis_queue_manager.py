import asyncio
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TypeVar, Dict, Type, cast, Optional

from eventy.event_queue import EventQueue
from eventy.queue_manager import QueueManager
from eventy.redis.redis_event_queue import RedisEventQueue
from eventy.serializers.serializer import Serializer, get_default_serializer

try:
    import redis.asyncio as redis
    from redis.asyncio import Redis
except ImportError:
    redis = None
    Redis = None

T = TypeVar("T")


@dataclass
class RedisQueueManager(QueueManager):
    """
    Manager for Redis event queues that creates them lazily by payload type.
    Uses Redis for distributed queue management with pub/sub notifications
    and expiring keys for automatic cleanup.
    """

    redis_client: Optional[Redis] = field(default=None)
    serializer: Serializer = field(default_factory=get_default_serializer)
    key_prefix: str = field(default="eventy")
    event_ttl: int = field(default=86400)  # 24 hours in seconds
    queues: Dict[Type, RedisEventQueue] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def __post_init__(self):
        if redis is None:
            raise ImportError(
                "Redis is not installed. Install it with: pip install redis>=5.0.0"
            )
        
        if self.redis_client is None:
            self.redis_client = redis.Redis(
                host='localhost',
                port=6379,
                decode_responses=False  # We handle bytes for serialization
            )

    def _get_queue_registry_key(self) -> str:
        """Generate Redis key for queue type registry"""
        return f"{self.key_prefix}:queue_types"

    async def get_queue(self, payload_type: Type[T]) -> RedisEventQueue[T]:
        """Get an event queue for the specified payload type

        Args:
            payload_type: The type of payload this queue will handle

        Returns:
            RedisEventQueue[T]: The event queue for this payload type

        Raises:
            KeyError: If no queue exists for the given payload type
        """
        async with self.lock:
            if payload_type not in self.queues:
                raise KeyError(f"No queue registered for payload type {payload_type}")
            return cast(RedisEventQueue[T], self.queues[payload_type])

    async def get_event_queue(self, payload_type: Type[T]) -> EventQueue[T]:
        """Get an event queue for the event type given (QueueManager interface)"""
        return await self.get_queue(payload_type)

    async def get_queue_types(self) -> list[type]:
        """List all available event queues."""
        # Get from local cache first
        local_types = list(self.queues.keys())
        
        # Also check Redis registry for queues registered by other instances
        registry_key = self._get_queue_registry_key()
        redis_types = await self.redis_client.smembers(registry_key)
        
        # Combine and deduplicate
        all_type_names = set()
        all_type_names.update(t.__name__ for t in local_types)
        all_type_names.update(t.decode() for t in redis_types)
        
        # For now, return local types only since we can't easily reconstruct
        # type objects from names without additional metadata
        return local_types

    async def register(self, payload_type: type[T]) -> None:
        """Register a payload type (Create an event queue)"""
        async with self.lock:
            if payload_type not in self.queues:
                # Create a new queue for this payload type
                queue = RedisEventQueue[T](
                    event_type=payload_type,
                    redis_client=self.redis_client,
                    serializer=cast(Serializer[T], self.serializer),
                    key_prefix=self.key_prefix,
                    event_ttl=self.event_ttl,
                )
                self.queues[payload_type] = queue
                
                # Register in Redis for other instances to discover
                registry_key = self._get_queue_registry_key()
                await self.redis_client.sadd(registry_key, payload_type.__name__)
                await self.redis_client.expire(registry_key, self.event_ttl)

    async def deregister(self, payload_type: type[T]) -> None:
        """Deregister a payload type (Shut down an event queue)"""
        async with self.lock:
            if payload_type in self.queues:
                # Clean up the queue
                queue = self.queues[payload_type]
                if hasattr(queue, '__aexit__'):
                    await queue.__aexit__(None, None, None)
                
                # Remove from local cache
                del self.queues[payload_type]
                
                # Remove from Redis registry
                registry_key = self._get_queue_registry_key()
                await self.redis_client.srem(registry_key, payload_type.__name__)

    async def __aenter__(self):
        """Begin using this queue manager"""
        # Test Redis connection
        try:
            await self.redis_client.ping()
        except Exception as e:
            raise ConnectionError(f"Failed to connect to Redis: {e}")
        
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Finish using this queue manager"""
        # Clean up all queues
        async with self.lock:
            for queue in self.queues.values():
                if hasattr(queue, '__aexit__'):
                    try:
                        await queue.__aexit__(exc_type, exc_value, traceback)
                    except Exception:
                        pass  # Best effort cleanup
        
        # Close Redis connection
        if self.redis_client:
            await self.redis_client.close()