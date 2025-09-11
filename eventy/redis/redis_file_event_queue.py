import asyncio
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import TypeVar, Optional
from uuid import UUID, uuid4

try:
    import redis.asyncio as redis
except ImportError as exc:
    raise ImportError(
        "Redis is required for RedisFileEventQueue. Install with: pip install 'eventy[redis]'"
    ) from exc

from eventy.claim import Claim
from eventy.event_result import EventResult
from eventy.eventy_error import EventyError
from eventy.fs.abstract_file_event_queue import AbstractFileEventQueue
from eventy.queue_event import QueueEvent
from eventy.subscribers.subscriber import Subscriber
from eventy.subscription import Subscription

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


@dataclass
class RedisFileEventQueue(AbstractFileEventQueue[T]):
    """
    Redis-enhanced file event queue implementation.

    This implementation extends AbstractFileEventQueue with Redis-based coordination:
    - Next event ID is stored in Redis and regenerated from filesystem if missing
    - Claims use Redis exclusive variables with expiration times
    - Publishing, claims, and subscriber changes publish to Redis pubsub
    - Background monitoring of Redis pubsub for distributed coordination
    - Resync mechanism for distributed state synchronization
    """

    # Redis connection configuration
    redis_url: str = field(default="redis://localhost:6379")
    redis_db: int = field(default=0)
    redis_password: str | None = field(default=None)

    # Redis key prefixes
    redis_prefix: str = field(default="eventy")

    # Claim expiration configuration
    claim_expiration_seconds: int = field(default=300)  # 5 minutes

    # Resync configuration
    resync: bool = field(default=False)
    resync_lock_timeout_seconds: int = field(default=30)

    # Enable/disable pubsub monitoring (useful for testing)
    enable_pubsub_monitor: bool = field(default=True)

    # Redis connection and pubsub
    _redis: Optional[redis.Redis] = field(default=None, init=False)
    _pubsub: Optional[redis.client.PubSub] = field(default=None, init=False)
    _pubsub_task: Optional[asyncio.Task] = field(default=None, init=False)
    _stop_pubsub: bool = field(default=False, init=False)

    def __post_init__(self):
        """Initialize Redis connection and call parent post_init"""
        super().__post_init__()
        # Initialize Redis-specific fields
        self._redis = None
        self._pubsub = None
        self._pubsub_task = None
        self._stop_pubsub = False

    async def __aenter__(self):
        """Start the Redis file event queue"""
        if self.running:
            return self

        # Initialize Redis connection
        try:
            self._redis = redis.from_url(
                self.redis_url,
                db=self.redis_db,
                password=self.redis_password,
                decode_responses=True,
            )
            await self._redis.ping()
            _LOGGER.info(f"Connected to Redis at {self.redis_url}")

            # Initialize next event ID from Redis or filesystem
            await self._initialize_redis_event_counter()

            # Set up pubsub
            self._pubsub = self._redis.pubsub()
            await self._pubsub.subscribe(self._get_pubsub_channel())

            # Start background pubsub monitoring (if enabled)
            self._stop_pubsub = False
            if self.enable_pubsub_monitor:
                self._pubsub_task = asyncio.create_task(self._pubsub_monitor_loop())

        except Exception as e:
            _LOGGER.warning(
                f"Failed to connect to Redis: {e}. Falling back to filesystem-only mode."
            )
            self._redis = None
            self._pubsub = None
            # Initialize event counter from filesystem only
            self._initialize_event_counter()

        # Handle resync if requested (only if Redis is available)
        if self.resync and self._redis:
            await self._handle_resync_request()

        self.running = True
        _LOGGER.info(f"Started Redis file event queue at {self.root_dir}")
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Stop the Redis file event queue"""
        if not self.running:
            return

        self.running = False
        self._stop_pubsub = True

        # Stop pubsub monitoring
        if self._pubsub_task and not self._pubsub_task.done():
            self._pubsub_task.cancel()
            try:
                await self._pubsub_task
            except asyncio.CancelledError:
                pass

        # Close pubsub and Redis connections
        if self._pubsub:
            await self._pubsub.unsubscribe()
            await self._pubsub.close()

        if self._redis:
            await self._redis.close()

        _LOGGER.info(f"Stopped Redis file event queue at {self.root_dir}")

    def _get_redis_key(self, key_type: str, key_id: str = "") -> str:
        """Generate Redis key with consistent naming"""
        base_key = f"{self.redis_prefix}:{self.root_dir.name}:{key_type}"
        return f"{base_key}:{key_id}" if key_id else base_key

    def _get_pubsub_channel(self) -> str:
        """Get the pubsub channel name"""
        return self._get_redis_key("events")

    async def _initialize_redis_event_counter(self):
        """Initialize event counter from Redis or regenerate from filesystem"""
        redis_key = self._get_redis_key("next_event_id")

        # Try to get from Redis first
        next_id = await self._redis.get(redis_key)
        if next_id is not None:
            self.next_event_id = int(next_id)
            _LOGGER.debug(f"Loaded next event ID from Redis: {self.next_event_id}")
        else:
            # Regenerate from filesystem
            self._initialize_event_counter()
            await self._redis.set(redis_key, self.next_event_id)
            _LOGGER.info(
                f"Regenerated next event ID from filesystem: {self.next_event_id}"
            )

    async def _update_redis_event_counter(self, event_id: int):
        """Update the Redis event counter (if Redis is available)"""
        if self._redis:
            redis_key = self._get_redis_key("next_event_id")
            await self._redis.set(redis_key, event_id + 1)

    async def publish(self, payload: T) -> QueueEvent[T]:
        """Publish an event and notify via Redis pubsub"""
        event = await super().publish(payload)

        # Update Redis counter
        await self._update_redis_event_counter(event.id)

        # Publish to Redis pubsub
        await self._publish_to_redis(
            "add_event", {"event_id": event.id, "worker_id": str(self.worker_id)}
        )

        # Notify subscribers
        await self._notify_subscribers_cached(event)

        return event

    async def create_claim(self, claim_id: str, data: str | None = None) -> bool:
        """Create a claim with Redis exclusive lock and expiration"""
        self._check_running()

        # Check local filesystem first
        claim_file = self.claims_dir / claim_id
        if claim_file.exists():
            return False

        # Try to create Redis exclusive lock (if Redis is available)
        if self._redis:
            redis_key = self._get_redis_key("claim", claim_id)
            lock_data = {
                "worker_id": str(self.worker_id),
                "created_at": datetime.now().isoformat(),
                "data": data,
            }

            # Use SET with NX (only if not exists) and EX (expiration)
            success = await self._redis.set(
                redis_key,
                json.dumps(lock_data),
                nx=True,
                ex=self.claim_expiration_seconds,
            )

            if not success:
                return False

        # Create local claim file
        try:
            claim = Claim(id=claim_id, worker_id=self.worker_id, data=data)
            claim_data = self.claim_serializer.serialize(claim)

            with open(claim_file, "xb") as f:
                f.write(claim_data)

            _LOGGER.info(f"Created claim {claim_id} with Redis lock")

            # Publish to Redis pubsub
            await self._publish_to_redis(
                "add_claim", {"claim_id": claim_id, "worker_id": str(self.worker_id)}
            )

            return True

        except FileExistsError:
            # Clean up Redis lock if file creation failed (and Redis is available)
            if self._redis:
                await self._redis.delete(redis_key)
            return False

    async def get_claim(self, claim_id: str) -> Claim:
        """Get a claim, checking local filesystem first, then Redis lock"""
        self._check_running()

        # Check local filesystem first
        claim_file = self.claims_dir / claim_id
        if claim_file.exists():
            with open(claim_file, "rb") as f:
                claim_data = f.read()
            return self.claim_serializer.deserialize(claim_data)

        # Check Redis lock (if Redis is available)
        if self._redis:
            redis_key = self._get_redis_key("claim", claim_id)
            lock_data = await self._redis.get(redis_key)

            if lock_data is not None:
                # Parse Redis lock data and create Claim object
                lock_info = json.loads(lock_data)
                return Claim(
                    id=claim_id,
                    worker_id=UUID(lock_info["worker_id"]),
                    created_at=datetime.fromisoformat(lock_info["created_at"]),
                    data=lock_info.get("data"),
                )

        # Claim not found in filesystem or Redis
        raise EventyError(f"Claim {claim_id} not found")

    async def subscribe(
        self,
        subscriber: Subscriber[T],
        check_subscriber_unique: bool = False,
        from_index: int | None = None,
    ) -> Subscription[T]:
        """Add a subscriber and publish to Redis pubsub"""
        subscription = await super().subscribe(
            subscriber, check_subscriber_unique, from_index
        )

        # Publish to Redis pubsub
        await self._publish_to_redis(
            "create_subscriber",
            {"subscriber_id": str(subscription.id), "worker_id": str(self.worker_id)},
        )

        return subscription

    async def unsubscribe(self, subscriber_id: UUID) -> bool:
        """Remove a subscriber and publish to Redis pubsub"""
        success = await super().unsubscribe(subscriber_id)

        if success:
            # Publish to Redis pubsub
            await self._publish_to_redis(
                "remove_subscriber",
                {"subscriber_id": str(subscriber_id), "worker_id": str(self.worker_id)},
            )

        return success

    async def _store_result(self, result: EventResult):
        """Store a result and publish to Redis pubsub"""
        await super()._store_result(result)

        # Publish to Redis pubsub
        await self._publish_to_redis(
            "add_result",
            {
                "result_id": str(result.id),
                "event_id": result.event_id,
                "worker_id": str(self.worker_id),
            },
        )

    async def _publish_to_redis(self, event_type: str, data: dict):
        """Publish an event to Redis pubsub"""
        if not self._redis:
            return

        message = {
            "type": event_type,
            "data": data,
            "timestamp": datetime.now().isoformat(),
        }

        try:
            await self._redis.publish(self._get_pubsub_channel(), json.dumps(message))
            _LOGGER.debug(f"Published {event_type} to Redis: {data}")
        except Exception as e:
            _LOGGER.error(f"Failed to publish to Redis: {e}")

    async def _pubsub_monitor_loop(self):
        """Background task to monitor Redis pubsub messages"""
        _LOGGER.debug("Starting Redis pubsub monitor loop")

        try:
            while not self._stop_pubsub:
                try:
                    message = await self._pubsub.get_message(timeout=1.0)
                    if message and message["type"] == "message":
                        await self._handle_pubsub_message(message["data"])
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    _LOGGER.error(f"Error in pubsub monitor loop: {e}", exc_info=True)
                    await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            _LOGGER.debug("Redis pubsub monitor loop cancelled")
            raise
        except Exception as e:
            _LOGGER.error(
                f"Unexpected error in pubsub monitor loop: {e}", exc_info=True
            )
        finally:
            _LOGGER.debug("Redis pubsub monitor loop ended")

    async def _handle_pubsub_message(self, message_data: str):
        """Handle incoming Redis pubsub messages"""
        try:
            message = json.loads(message_data)
            event_type = message.get("type")
            data = message.get("data", {})

            # Skip messages from this worker
            if data.get("worker_id") == str(self.worker_id):
                return

            _LOGGER.debug(f"Received pubsub message: {event_type} - {data}")

            if event_type == "add_event":
                await self._handle_remote_event(data)
            elif event_type == "add_claim":
                await self._handle_remote_claim(data)
            elif event_type == "add_result":
                await self._handle_remote_result(data)
            elif event_type == "create_subscriber":
                await self._handle_remote_subscriber_create(data)
            elif event_type == "remove_subscriber":
                await self._handle_remote_subscriber_remove(data)
            elif event_type == "resync":
                await self._handle_resync_message(data)

        except Exception as e:
            _LOGGER.error(f"Error handling pubsub message: {e}", exc_info=True)

    async def _handle_remote_event(self, data: dict):
        """Handle remote event notification"""
        event_id = data.get("event_id")
        if not event_id:
            return

        # Check if we already have this event locally
        event_file = self.events_dir / str(event_id)
        if event_file.exists():
            return

        # Event doesn't exist locally, but was published remotely
        # This could happen in distributed scenarios - we should process it
        try:
            event = await self.get_event(event_id)
            await self._notify_subscribers_cached(event)
            self._mark_event_processed(event_id)
            _LOGGER.info(f"Processed remote event {event_id}")
        except Exception as e:
            _LOGGER.warning(f"Could not process remote event {event_id}: {e}")

    async def _handle_remote_claim(self, data: dict):
        """Handle remote claim notification"""
        # For now, just log - claims are handled via Redis locks
        claim_id = data.get("claim_id")
        _LOGGER.debug(f"Remote claim created: {claim_id}")

    async def _handle_remote_result(self, data: dict):
        """Handle remote result notification"""
        # For now, just log - results are stored locally
        result_id = data.get("result_id")
        _LOGGER.debug(f"Remote result created: {result_id}")

    async def _handle_remote_subscriber_create(self, data: dict):
        """Handle remote subscriber creation"""
        # Mark subscription cache as dirty to reload
        self._mark_subscription_cache_dirty()
        _LOGGER.debug(f"Remote subscriber created: {data.get('subscriber_id')}")

    async def _handle_remote_subscriber_remove(self, data: dict):
        """Handle remote subscriber removal"""
        # Mark subscription cache as dirty to reload
        self._mark_subscription_cache_dirty()
        _LOGGER.debug(f"Remote subscriber removed: {data.get('subscriber_id')}")

    async def _handle_resync_request(self):
        """Handle resync request on startup"""
        resync_id = str(uuid4())

        # Publish resync request
        await self._publish_to_redis(
            "resync", {"resync_id": resync_id, "worker_id": str(self.worker_id)}
        )

        _LOGGER.info(f"Sent resync request: {resync_id}")

    async def _handle_resync_message(self, data: dict):
        """Handle resync message from another worker"""
        resync_id = data.get("resync_id")
        requesting_worker = data.get("worker_id")

        if not resync_id or requesting_worker == str(self.worker_id):
            return

        # Try to acquire exclusive resync lock
        lock_key = self._get_redis_key("resync_lock", resync_id)
        lock_acquired = await self._redis.set(
            lock_key, str(self.worker_id), nx=True, ex=self.resync_lock_timeout_seconds
        )

        if not lock_acquired:
            _LOGGER.debug(f"Could not acquire resync lock for {resync_id}")
            return

        _LOGGER.info(f"Acquired resync lock for {resync_id}, publishing full state")

        try:
            # Publish all subscribers
            async for subscription in self._iter_subscriptions():
                await self._publish_to_redis(
                    "create_subscriber",
                    {
                        "subscriber_id": str(subscription.id),
                        "worker_id": str(self.worker_id),
                    },
                )

            # Publish all claims
            async for claim in self._iter_claims():
                await self._publish_to_redis(
                    "add_claim",
                    {"claim_id": claim.id, "worker_id": str(self.worker_id)},
                )

            # Publish all results
            async for result in self._iter_results():
                await self._publish_to_redis(
                    "add_result",
                    {
                        "result_id": str(result.id),
                        "event_id": result.event_id,
                        "worker_id": str(self.worker_id),
                    },
                )

            # Publish all events
            async for event in self._iter_events():
                await self._publish_to_redis(
                    "add_event",
                    {"event_id": event.id, "worker_id": str(self.worker_id)},
                )

            _LOGGER.info(f"Completed resync broadcast for {resync_id}")

        except Exception as e:
            _LOGGER.error(f"Error during resync broadcast: {e}", exc_info=True)
        finally:
            # Release the lock
            await self._redis.delete(lock_key)

    async def reset(self):
        """Reset the queue by clearing all filesystem and Redis data"""
        import shutil

        # Clear filesystem data (same as parent class reset)
        if self.events_dir.exists():
            shutil.rmtree(self.events_dir)
        if self.results_dir.exists():
            shutil.rmtree(self.results_dir)
        if self.claims_dir.exists():
            shutil.rmtree(self.claims_dir)
        if self.subscriptions_dir.exists():
            shutil.rmtree(self.subscriptions_dir)

        # Reset counters
        self.processed_event_id = 0
        self.next_event_id = 1

        # Clear Redis data (if Redis is available)
        if self._redis:
            try:
                # Clear Redis keys for this queue
                pattern = f"{self.redis_prefix}:{self.root_dir.name}:*"
                keys = await self._redis.keys(pattern)
                if keys:
                    await self._redis.delete(*keys)
                    _LOGGER.info(f"Cleared {len(keys)} Redis keys for queue")
            except Exception as e:
                _LOGGER.warning(f"Error clearing Redis data: {e}")

        # Recreate directories
        self.events_dir.mkdir(parents=True, exist_ok=True)
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.claims_dir.mkdir(parents=True, exist_ok=True)
        self.subscriptions_dir.mkdir(parents=True, exist_ok=True)

        _LOGGER.info(f"Reset Redis file event queue at {self.root_dir}")
