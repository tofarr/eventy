import asyncio
import json
from dataclasses import dataclass, field
from datetime import datetime, UTC
import logging
from typing import TypeVar, Optional, Any, Dict
from uuid import UUID, uuid4

from eventy.event_queue import EventQueue
from eventy.page import Page
from eventy.queue_event import QueueEvent, EventStatus
from eventy.subscriber.subscriber import Subscriber
from eventy.subscriber.subscription import Subscription
from eventy.serializers.serializer import Serializer, get_default_serializer

try:
    import redis.asyncio as redis
    from redis.asyncio import Redis
except ImportError:
    redis = None
    Redis = None

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


@dataclass
class RedisEventQueue(EventQueue[T]):
    """
    Redis-based implementation of EventQueue using Redis pub/sub for notifications
    and expiring keys for event storage.
    """

    event_type: type[T]
    redis_client: Redis = field(default=None)
    serializer: Serializer[T] = field(default_factory=get_default_serializer)
    worker_id: UUID = field(default_factory=uuid4)
    key_prefix: str = field(default="eventy")
    event_ttl: int = field(default=86400)  # 24 hours in seconds
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    _subscribers: Dict[UUID, Subscriber[T]] = field(default_factory=dict)
    _pubsub: Optional[Any] = field(default=None, init=False)

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

    def _get_event_key(self, event_id: int) -> str:
        """Generate Redis key for an event"""
        return f"{self.key_prefix}:events:{self.event_type.__name__}:{event_id}"

    def _get_subscriber_key(self, subscriber_id: UUID) -> str:
        """Generate Redis key for a subscriber"""
        return f"{self.key_prefix}:subscribers:{self.event_type.__name__}:{subscriber_id}"

    def _get_event_counter_key(self) -> str:
        """Generate Redis key for event counter"""
        return f"{self.key_prefix}:counter:{self.event_type.__name__}"

    def _get_event_index_key(self) -> str:
        """Generate Redis key for event index (sorted set)"""
        return f"{self.key_prefix}:index:{self.event_type.__name__}"

    def _get_channel_name(self) -> str:
        """Generate Redis channel name for pub/sub"""
        return f"{self.key_prefix}:channel:{self.event_type.__name__}"

    async def _ensure_pubsub(self):
        """Ensure pub/sub connection is established"""
        if self._pubsub is None:
            self._pubsub = self.redis_client.pubsub()
            await self._pubsub.subscribe(self._get_channel_name())

    async def subscribe(self, subscriber: Subscriber[T]) -> UUID:
        """Add a subscriber to this queue"""
        # Validate that subscriber's payload_type is compatible with queue's event_type
        if not hasattr(subscriber, "payload_type") or subscriber.payload_type is None:
            raise TypeError(
                f"Subscriber {subscriber} must have a payload_type attribute"
            )

        if not issubclass(self.event_type, subscriber.payload_type):
            raise TypeError(
                f"Subscriber payload_type {subscriber.payload_type.__name__} is not compatible "
                f"with queue event_type {self.event_type.__name__}. The queue's event_type must "
                f"be the same as or a subclass of the subscriber's payload_type."
            )

        subscriber_id = uuid4()
        async with self.lock:
            # Store subscriber in memory for fast access
            self._subscribers[subscriber_id] = subscriber
            
            # Also store in Redis for persistence across restarts
            subscriber_key = self._get_subscriber_key(subscriber_id)
            subscriber_data = {
                'id': str(subscriber_id),
                'type': subscriber.__class__.__module__ + '.' + subscriber.__class__.__name__,
                'created_at': datetime.now(UTC).isoformat()
            }
            await self.redis_client.hset(
                subscriber_key,
                mapping={k: json.dumps(v) if not isinstance(v, str) else v 
                        for k, v in subscriber_data.items()}
            )
            await self.redis_client.expire(subscriber_key, self.event_ttl)

        return subscriber_id

    async def unsubscribe(self, subscriber_id: UUID) -> bool:
        """Remove a subscriber from this queue"""
        async with self.lock:
            # Remove from memory
            removed_from_memory = subscriber_id in self._subscribers
            if removed_from_memory:
                del self._subscribers[subscriber_id]
            
            # Remove from Redis
            subscriber_key = self._get_subscriber_key(subscriber_id)
            removed_from_redis = await self.redis_client.delete(subscriber_key) > 0
            
            return removed_from_memory or removed_from_redis

    async def get_subscriber(self, subscriber_id: UUID) -> Subscriber[T]:
        """Get subscriber with id given"""
        async with self.lock:
            if subscriber_id not in self._subscribers:
                raise KeyError(f"Subscriber {subscriber_id} not found")
            return self._subscribers[subscriber_id]

    async def search_subscribers(
        self, page_id: Optional[str], limit: int = 100
    ) -> Page[Subscription[T]]:
        """Get all subscribers along with their IDs"""
        async with self.lock:
            # Convert subscribers dict to list of Subscription objects
            all_subscriptions = [
                Subscription(id=sub_id, subscription=subscriber)
                for sub_id, subscriber in self._subscribers.items()
            ]

            # Handle pagination
            start_index = 0
            if page_id:
                try:
                    start_index = int(page_id)
                except (ValueError, TypeError):
                    start_index = 0

            # Get the page of subscriptions
            end_index = start_index + limit
            page_subscriptions = all_subscriptions[start_index:end_index]

            # Determine next page ID
            next_page_id = None
            if end_index < len(all_subscriptions):
                next_page_id = str(end_index)

            return Page(items=page_subscriptions, next_page_id=next_page_id)

    async def publish(self, payload: T) -> QueueEvent[T]:
        """Publish an event to this queue"""
        async with self.lock:
            # Get next event ID
            counter_key = self._get_event_counter_key()
            event_id = await self.redis_client.incr(counter_key)
            
            # Serialize the payload
            serialized_payload = self.serializer.serialize(payload)
            
            # Create event data
            created_at = datetime.now(UTC)
            event_data = {
                'id': event_id,
                'payload': serialized_payload,
                'status': EventStatus.PROCESSING.value,
                'created_at': created_at.isoformat(),
                'worker_id': str(self.worker_id)
            }
            
            # Store event in Redis with TTL
            event_key = self._get_event_key(event_id)
            await self.redis_client.hset(
                event_key,
                mapping={k: v if isinstance(v, (str, bytes)) else json.dumps(v) 
                        for k, v in event_data.items()}
            )
            await self.redis_client.expire(event_key, self.event_ttl)
            
            # Add to sorted index for efficient querying
            index_key = self._get_event_index_key()
            await self.redis_client.zadd(
                index_key, 
                {str(event_id): created_at.timestamp()}
            )
            await self.redis_client.expire(index_key, self.event_ttl)
            
            # Create event object for notification
            event = QueueEvent(
                id=event_id,
                payload=payload,
                status=EventStatus.PROCESSING,
                created_at=created_at
            )
            
            # Notify all subscribers
            final_status = EventStatus.PROCESSING
            for subscriber in list(self._subscribers.values()):
                try:
                    await subscriber.on_worker_event(
                        event, self.worker_id, self.worker_id
                    )
                except Exception:
                    final_status = EventStatus.ERROR
                    _LOGGER.error("subscriber_error", exc_info=True, stack_info=True)
            
            # Update status in Redis
            if final_status != EventStatus.PROCESSING:
                await self.redis_client.hset(
                    event_key, 
                    'status', 
                    final_status.value
                )
            
            # Publish notification to Redis channel
            channel_name = self._get_channel_name()
            notification = {
                'event_id': event_id,
                'status': final_status.value,
                'worker_id': str(self.worker_id)
            }
            await self.redis_client.publish(
                channel_name, 
                json.dumps(notification)
            )
            
            return event

    async def search_events(
        self,
        page_id: Optional[str] = None,
        limit: Optional[int] = 100,
        created_at__min: Optional[datetime] = None,
        created_at__max: Optional[datetime] = None,
        status__eq: Optional[EventStatus] = None,
    ) -> Page[QueueEvent[T]]:
        """Get existing events from the queue with optional paging parameters"""
        index_key = self._get_event_index_key()
        
        # Determine score range for time filtering
        min_score = created_at__min.timestamp() if created_at__min else '-inf'
        max_score = created_at__max.timestamp() if created_at__max else '+inf'
        
        # Get event IDs from sorted set
        start_index = 0
        if page_id:
            try:
                start_index = int(page_id)
            except (ValueError, TypeError):
                start_index = 0
        
        end_index = start_index + (limit or 100) - 1
        
        # Get event IDs in reverse chronological order
        event_ids = await self.redis_client.zrevrangebyscore(
            index_key,
            max_score,
            min_score,
            start=start_index,
            num=limit or 100
        )
        
        # Fetch event data
        events = []
        for event_id_bytes in event_ids:
            event_id = int(event_id_bytes.decode())
            try:
                event = await self.get_event(event_id)
                
                # Apply status filter
                if status__eq and event.status != status__eq:
                    continue
                    
                events.append(event)
            except Exception:
                # Skip corrupted or expired events
                continue
        
        # Determine next page ID
        next_page_id = None
        if len(event_ids) == (limit or 100):
            next_page_id = str(end_index + 1)
        
        return Page(items=events, next_page_id=next_page_id)

    async def count_events(
        self,
        created_at__min: Optional[datetime] = None,
        created_at__max: Optional[datetime] = None,
        status__eq: Optional[EventStatus] = None,
    ) -> int:
        """Get the number of events matching the criteria given"""
        index_key = self._get_event_index_key()
        
        # Determine score range for time filtering
        min_score = created_at__min.timestamp() if created_at__min else '-inf'
        max_score = created_at__max.timestamp() if created_at__max else '+inf'
        
        if status__eq is None:
            # Simple count by time range
            return await self.redis_client.zcount(index_key, min_score, max_score)
        else:
            # Need to check each event's status
            event_ids = await self.redis_client.zrangebyscore(
                index_key, min_score, max_score
            )
            
            count = 0
            for event_id_bytes in event_ids:
                event_id = int(event_id_bytes.decode())
                event_key = self._get_event_key(event_id)
                status_bytes = await self.redis_client.hget(event_key, 'status')
                if status_bytes:
                    status = EventStatus(status_bytes.decode())
                    if status == status__eq:
                        count += 1
            
            return count

    async def get_event(self, event_id: int) -> QueueEvent[T]:
        """Get an event given its id."""
        event_key = self._get_event_key(event_id)
        event_data = await self.redis_client.hgetall(event_key)
        
        if not event_data:
            raise KeyError(f"Event {event_id} not found")
        
        # Deserialize event data
        payload_bytes = event_data[b'payload']
        payload = self.serializer.deserialize(payload_bytes)
        
        status = EventStatus(event_data[b'status'].decode())
        created_at = datetime.fromisoformat(event_data[b'created_at'].decode())
        
        return QueueEvent(
            id=event_id,
            payload=payload,
            status=status,
            created_at=created_at
        )

    async def __aenter__(self):
        """Begin using this event queue"""
        await self._ensure_pubsub()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Finish using this event queue"""
        if self._pubsub:
            await self._pubsub.close()
            self._pubsub = None