"""SQL-based event queue implementation."""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import TypeVar, Optional, AsyncIterator
from uuid import UUID, uuid4

try:
    from sqlalchemy import select, delete, and_, or_, func
    from sqlalchemy.ext.asyncio import (
        create_async_engine,
        AsyncSession,
        async_sessionmaker,
    )
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.engine import Engine
except ImportError as e:
    raise ImportError(
        "SQLAlchemy is required for SQL event queue. Install with: pip install eventy[sql]"
    ) from e

from eventy.claim import Claim
from eventy.event_queue import EventQueue
from eventy.event_result import EventResult
from eventy.eventy_error import EventyError
from eventy.page import Page
from eventy.queue_event import QueueEvent
from eventy.serializers.serializer import Serializer, get_default_serializer
from eventy.subscribers.subscriber import Subscriber
from eventy.subscription import Subscription
from eventy.sql.models import Base, SqlEvent, SqlEventResult, SqlSubscriber, SqlClaim

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


@dataclass
class SqlEventQueue(EventQueue[T]):
    """
    SQL-based event queue implementation using SQLAlchemy.

    This class provides a database-backed storage system where:
    - Events are stored in the eventy_events table
    - Results are stored in the eventy_results table
    - Subscribers are stored in the eventy_subscribers table
    - Claims are stored in the eventy_claims table
    - Each type of data uses its own serializer for storage
    """

    database_url: str
    payload_type: type[T]

    # Serializers for different data types
    event_serializer: Serializer[QueueEvent[T]] = field(
        default_factory=get_default_serializer
    )
    result_serializer: Serializer[EventResult] = field(
        default_factory=get_default_serializer
    )
    subscriber_serializer: Serializer[Subscriber[T]] = field(
        default_factory=get_default_serializer
    )
    claim_serializer: Serializer[Claim] = field(default_factory=get_default_serializer)

    # Running state
    running: bool = field(default=False, init=False)

    # Worker ID for this instance
    worker_id: UUID = field(default_factory=uuid4)

    # Database connection
    _engine: Optional[Engine] = field(default=None, init=False)
    _session_factory: Optional[async_sessionmaker] = field(default=None, init=False)

    # Cache subscriptions to avoid loading from database for each event
    _subscription_cache: dict = field(default_factory=dict, init=False)
    _subscription_cache_dirty: bool = field(default=True, init=False)

    def __post_init__(self):
        """Initialize database connection"""
        self._engine = create_async_engine(self.database_url)
        self._session_factory = async_sessionmaker(bind=self._engine)

    def _check_running(self):
        """Check if the queue is running and raise error if not"""
        if not self.running:
            raise RuntimeError("EventQueue is not running. Call __aenter__ first.")

    def session_factory(self) -> async_sessionmaker:
        """Get the async session factory"""
        return self._session_factory

    def _get_payload_type_name(self) -> str:
        """Get the string name of the payload type"""
        return f"{self.payload_type.__module__}.{self.payload_type.__name__}"

    async def __aenter__(self):
        """Start this event queue"""
        # Create tables if they don't exist
        async with self._engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        # Initialize subscription cache
        await self._load_subscription_cache()

        self.running = True
        _LOGGER.info(f"Started SqlEventQueue for {self.payload_type}")
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Close this event queue"""
        self.running = False
        if self._engine:
            await self._engine.dispose()
        _LOGGER.info(f"Stopped SqlEventQueue for {self.payload_type}")

    def get_worker_id(self) -> UUID:
        """Get the id of the current worker"""
        return self.worker_id

    def get_payload_type(self) -> type[T]:
        """Get the type of payload handled by this queue"""
        return self.payload_type

    async def publish(self, payload: T) -> QueueEvent[T]:
        """Publish an event to this queue"""
        self._check_running()

        async with self.session_factory()() as session:
            # Create event with auto-incrementing ID
            sql_event = SqlEvent(
                payload_type=self._get_payload_type_name(),
                payload_data=b"",  # Temporary, will be updated after getting ID
            )

            session.add(sql_event)
            await session.commit()
            await session.refresh(sql_event)

            # Create the QueueEvent with the actual database ID
            event = QueueEvent(
                id=sql_event.id, payload=payload, created_at=sql_event.created_at
            )

            # Update the stored event data with the correct ID
            event_data = self.event_serializer.serialize(event)
            sql_event.payload_data = event_data
            await session.commit()

            _LOGGER.info(f"Published event {event.id}")
            return event

    async def get_event(self, event_id: int) -> QueueEvent[T]:
        """Get an event given its id."""
        self._check_running()

        async with self.session_factory()() as session:
            sql_event = await session.get(SqlEvent, event_id)
            if not sql_event or sql_event.payload_type != self._get_payload_type_name():
                raise EventyError(f"Event {event_id} not found")

            return self.event_serializer.deserialize(sql_event.payload_data)

    async def _iter_events(
        self,
        created_at__gte: Optional[datetime] = None,
        created_at__lte: Optional[datetime] = None,
    ) -> AsyncIterator[QueueEvent[T]]:
        """Iterate over events matching the given criteria"""
        async with self.session_factory()() as session:
            query = (
                select(SqlEvent)
                .where(SqlEvent.payload_type == self._get_payload_type_name())
                .order_by(SqlEvent.id)
            )

            if created_at__gte is not None:
                query = query.where(SqlEvent.created_at >= created_at__gte)
            if created_at__lte is not None:
                query = query.where(SqlEvent.created_at <= created_at__lte)

            result = await session.execute(query)
            for sql_event in result.scalars():
                try:
                    event = self.event_serializer.deserialize(sql_event.payload_data)
                    yield event
                except Exception as e:
                    _LOGGER.warning(f"Failed to deserialize event {sql_event.id}: {e}")

    async def search_events(
        self,
        page_id: Optional[str] = None,
        limit: int = 100,
        created_at__gte: Optional[datetime] = None,
        created_at__lte: Optional[datetime] = None,
    ) -> Page[QueueEvent[T]]:
        """Get existing events with optional paging parameters"""
        self._check_running()

        async with self.session_factory()() as session:
            query = (
                select(SqlEvent)
                .where(SqlEvent.payload_type == self._get_payload_type_name())
                .order_by(SqlEvent.id)
            )

            if created_at__gte is not None:
                query = query.where(SqlEvent.created_at >= created_at__gte)
            if created_at__lte is not None:
                query = query.where(SqlEvent.created_at <= created_at__lte)

            # Handle pagination
            offset = int(page_id) if page_id is not None else 0
            query = query.offset(offset).limit(
                limit + 1
            )  # Get one extra to check for next page

            result = await session.execute(query)
            sql_events = list(result.scalars())

            # Check if there's a next page
            has_next_page = len(sql_events) > limit
            if has_next_page:
                sql_events = sql_events[:limit]

            # Convert to QueueEvent objects
            events = []
            for sql_event in sql_events:
                try:
                    event = self.event_serializer.deserialize(sql_event.payload_data)
                    events.append(event)
                except Exception as e:
                    _LOGGER.warning(f"Failed to deserialize event {sql_event.id}: {e}")

            # Determine next page ID
            next_page_id = None
            if has_next_page:
                next_page_id = str(offset + limit)

            return Page(items=events, next_page_id=next_page_id)

    async def subscribe(
        self,
        subscriber: Subscriber[T],
        check_subscriber_unique: bool = False,
        from_index: int | None = None,
    ) -> Subscription[T]:
        """Add a subscriber to this queue"""
        self._check_running()

        # Check for existing subscriber if requested
        if check_subscriber_unique:
            for existing_subscription in await self._get_cached_subscriptions():
                if existing_subscription.subscriber == subscriber:
                    _LOGGER.info(
                        f"Found existing subscription {existing_subscription.id} for subscriber"
                    )
                    return existing_subscription

        # Create new subscription
        subscriber_id = uuid4()
        subscription = Subscription(id=subscriber_id, subscriber=subscriber)

        async with self.session_factory()() as session:
            subscriber_data = self.subscriber_serializer.serialize(subscriber)

            sql_subscriber = SqlSubscriber(
                id=subscriber_id,
                payload_type=self._get_payload_type_name(),
                subscriber_data=subscriber_data,
            )

            session.add(sql_subscriber)
            await session.commit()

        # Add to cache
        self._add_subscription_to_cache(subscription)

        _LOGGER.info(f"Added subscription {subscriber_id}")
        return subscription

    async def unsubscribe(self, subscriber_id: UUID) -> bool:
        """Remove a subscriber from this queue"""
        self._check_running()

        async with self.session_factory()() as session:
            result = await session.execute(
                delete(SqlSubscriber).where(
                    and_(
                        SqlSubscriber.id == subscriber_id,
                        SqlSubscriber.payload_type == self._get_payload_type_name(),
                    )
                )
            )
            await session.commit()

            if result.rowcount > 0:
                # Remove from cache
                self._remove_subscription_from_cache(subscriber_id)
                _LOGGER.info(f"Removed subscription {subscriber_id}")
                return True
            return False

    async def get_subscriber(self, subscriber_id: UUID) -> Subscriber[T]:
        """Get subscriber with id given"""
        self._check_running()

        async with self.session_factory()() as session:
            result = await session.execute(
                select(SqlSubscriber).where(
                    and_(
                        SqlSubscriber.id == subscriber_id,
                        SqlSubscriber.payload_type == self._get_payload_type_name(),
                    )
                )
            )
            sql_subscriber = result.scalar_one_or_none()

            if not sql_subscriber:
                raise KeyError(f"Subscriber {subscriber_id} not found")

            return self.subscriber_serializer.deserialize(
                sql_subscriber.subscriber_data
            )

    async def _iter_subscriptions(self) -> AsyncIterator[Subscription[T]]:
        """Iterate over all subscriptions"""
        async with self.session_factory()() as session:
            result = await session.execute(
                select(SqlSubscriber).where(
                    SqlSubscriber.payload_type == self._get_payload_type_name()
                )
            )

            for sql_subscriber in result.scalars():
                try:
                    subscriber = self.subscriber_serializer.deserialize(
                        sql_subscriber.subscriber_data
                    )
                    yield Subscription(id=sql_subscriber.id, subscriber=subscriber)
                except Exception as e:
                    _LOGGER.warning(
                        f"Failed to deserialize subscriber {sql_subscriber.id}: {e}"
                    )

    async def search_subscriptions(
        self, page_id: Optional[str] = None, limit: int = 100
    ) -> Page[Subscription[T]]:
        """Get all subscribers along with their IDs"""
        self._check_running()

        # Get all subscriptions from cache
        all_subscriptions = await self._get_cached_subscriptions()

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

    async def get_result(self, result_id: UUID) -> EventResult:
        """Get an event result given its id"""
        self._check_running()

        async with self.session_factory()() as session:
            sql_result = await session.get(SqlEventResult, result_id)
            if not sql_result:
                raise KeyError(f"Result {result_id} not found")

            return EventResult(
                id=sql_result.id,
                worker_id=sql_result.worker_id,
                event_id=sql_result.event_id,
                success=sql_result.success,
                details=sql_result.details,
                created_at=sql_result.created_at,
            )

    async def _iter_results(
        self,
        event_id__eq: Optional[int] = None,
        worker_id__eq: Optional[UUID] = None,
        created_at__gte: Optional[datetime] = None,
        created_at__lte: Optional[datetime] = None,
    ) -> AsyncIterator[EventResult]:
        """Iterate over results matching the given criteria"""
        async with self.session_factory()() as session:
            query = select(SqlEventResult)

            conditions = []
            if event_id__eq is not None:
                conditions.append(SqlEventResult.event_id == event_id__eq)
            if worker_id__eq is not None:
                conditions.append(SqlEventResult.worker_id == worker_id__eq)
            if created_at__gte is not None:
                conditions.append(SqlEventResult.created_at >= created_at__gte)
            if created_at__lte is not None:
                conditions.append(SqlEventResult.created_at <= created_at__lte)

            if conditions:
                query = query.where(and_(*conditions))

            query = query.order_by(SqlEventResult.created_at)

            result = await session.execute(query)
            for sql_result in result.scalars():
                yield EventResult(
                    id=sql_result.id,
                    worker_id=sql_result.worker_id,
                    event_id=sql_result.event_id,
                    success=sql_result.success,
                    details=sql_result.details,
                    created_at=sql_result.created_at,
                )

    async def search_results(
        self,
        page_id: Optional[str] = None,
        limit: int = 100,
        event_id__eq: Optional[int] = None,
        worker_id__eq: Optional[int] = None,
        created_at__gte: Optional[datetime] = None,
        created_at__lte: Optional[datetime] = None,
    ) -> Page[EventResult]:
        """Get existing results with optional paging parameters"""
        self._check_running()

        async with self.session_factory()() as session:
            query = select(SqlEventResult)

            conditions = []
            if event_id__eq is not None:
                conditions.append(SqlEventResult.event_id == event_id__eq)
            if worker_id__eq is not None:
                conditions.append(SqlEventResult.worker_id == worker_id__eq)
            if created_at__gte is not None:
                conditions.append(SqlEventResult.created_at >= created_at__gte)
            if created_at__lte is not None:
                conditions.append(SqlEventResult.created_at <= created_at__lte)

            if conditions:
                query = query.where(and_(*conditions))

            query = query.order_by(SqlEventResult.created_at)

            # Handle pagination
            offset = int(page_id) if page_id is not None else 0
            query = query.offset(offset).limit(
                limit + 1
            )  # Get one extra to check for next page

            result = await session.execute(query)
            sql_results = list(result.scalars())

            # Check if there's a next page
            has_next_page = len(sql_results) > limit
            if has_next_page:
                sql_results = sql_results[:limit]

            # Convert to EventResult objects
            results = []
            for sql_result in sql_results:
                results.append(
                    EventResult(
                        id=sql_result.id,
                        worker_id=sql_result.worker_id,
                        event_id=sql_result.event_id,
                        success=sql_result.success,
                        details=sql_result.details,
                        created_at=sql_result.created_at,
                    )
                )

            # Determine next page ID
            next_page_id = None
            if has_next_page:
                next_page_id = str(offset + limit)

            return Page(items=results, next_page_id=next_page_id)

    async def count_results(
        self,
        event_id__eq: Optional[int] = None,
        worker_id__eq: Optional[int] = None,
        created_at__gte: Optional[datetime] = None,
        created_at__lte: Optional[datetime] = None,
    ) -> int:
        """Get the number of results matching the criteria given"""
        self._check_running()

        async with self.session_factory()() as session:
            query = select(func.count(SqlEventResult.id))

            conditions = []
            if event_id__eq is not None:
                conditions.append(SqlEventResult.event_id == event_id__eq)
            if worker_id__eq is not None:
                conditions.append(SqlEventResult.worker_id == worker_id__eq)
            if created_at__gte is not None:
                conditions.append(SqlEventResult.created_at >= created_at__gte)
            if created_at__lte is not None:
                conditions.append(SqlEventResult.created_at <= created_at__lte)

            if conditions:
                query = query.where(and_(*conditions))

            result = await session.execute(query)
            return result.scalar()

    async def _store_result(self, result: EventResult):
        """Store a result to the database"""
        async with self.session_factory()() as session:
            sql_result = SqlEventResult(
                id=result.id,
                worker_id=result.worker_id,
                event_id=result.event_id,
                success=result.success,
                details=result.details,
                created_at=result.created_at,
            )

            session.add(sql_result)
            await session.commit()

        _LOGGER.info(f"Stored result {result.id}")

    async def create_claim(self, claim_id: str, data: str | None = None) -> bool:
        """Create a claim with the given ID."""
        self._check_running()

        async with self.session_factory()() as session:
            try:
                sql_claim = SqlClaim(
                    id=claim_id,
                    worker_id=self.worker_id,
                    payload_type=self._get_payload_type_name(),
                    data=data,
                )

                session.add(sql_claim)
                await session.commit()

                _LOGGER.info(f"Created claim {claim_id}")
                return True
            except IntegrityError:
                await session.rollback()
                return False

    async def get_claim(self, claim_id: str) -> Claim:
        """Get a claim by its ID."""
        self._check_running()

        async with self.session_factory()() as session:
            result = await session.execute(
                select(SqlClaim).where(
                    and_(
                        SqlClaim.id == claim_id,
                        SqlClaim.payload_type == self._get_payload_type_name(),
                    )
                )
            )
            sql_claim = result.scalar_one_or_none()

            if not sql_claim:
                raise EventyError(f"Claim {claim_id} not found")

            return Claim(
                id=sql_claim.id,
                worker_id=sql_claim.worker_id,
                created_at=sql_claim.created_at,
                data=sql_claim.data,
            )

    async def search_claims(
        self,
        page_id: Optional[str] = None,
        limit: int = 100,
        worker_id__eq: Optional[UUID] = None,
        created_at__gte: Optional[datetime] = None,
        created_at__lte: Optional[datetime] = None,
    ) -> Page[Claim]:
        """Search for claims with optional filtering and pagination."""
        self._check_running()

        async with self.session_factory()() as session:
            query = select(SqlClaim).where(
                SqlClaim.payload_type == self._get_payload_type_name()
            )

            conditions = []
            if worker_id__eq is not None:
                conditions.append(SqlClaim.worker_id == worker_id__eq)
            if created_at__gte is not None:
                conditions.append(SqlClaim.created_at >= created_at__gte)
            if created_at__lte is not None:
                conditions.append(SqlClaim.created_at <= created_at__lte)

            if conditions:
                query = query.where(and_(*conditions))

            query = query.order_by(SqlClaim.created_at)

            # Handle pagination
            offset = int(page_id) if page_id is not None else 0
            query = query.offset(offset).limit(
                limit + 1
            )  # Get one extra to check for next page

            result = await session.execute(query)
            sql_claims = list(result.scalars())

            # Check if there's a next page
            has_next_page = len(sql_claims) > limit
            if has_next_page:
                sql_claims = sql_claims[:limit]

            # Convert to Claim objects
            claims = []
            for sql_claim in sql_claims:
                claims.append(
                    Claim(
                        id=sql_claim.id,
                        worker_id=sql_claim.worker_id,
                        created_at=sql_claim.created_at,
                        data=sql_claim.data,
                    )
                )

            # Determine next page ID
            next_page_id = None
            if has_next_page:
                next_page_id = str(offset + limit)

            return Page(items=claims, next_page_id=next_page_id)

    async def count_claims(
        self,
        worker_id__eq: Optional[UUID] = None,
        created_at__gte: Optional[datetime] = None,
        created_at__lte: Optional[datetime] = None,
    ) -> int:
        """Count claims matching the given criteria."""
        async with self.session_factory()() as session:
            query = select(func.count(SqlClaim.id))

            conditions = []
            if worker_id__eq is not None:
                conditions.append(SqlClaim.worker_id == worker_id__eq)
            if created_at__gte is not None:
                conditions.append(SqlClaim.created_at >= created_at__gte)
            if created_at__lte is not None:
                conditions.append(SqlClaim.created_at <= created_at__lte)

            if conditions:
                query = query.where(and_(*conditions))

            result = await session.execute(query)
            return result.scalar() or 0

    async def _load_subscription_cache(self):
        """Load subscriptions into cache"""
        self._subscription_cache = {}
        if not self.running:
            return

        async with self.session_factory()() as session:
            result = await session.execute(
                select(SqlSubscriber).where(
                    SqlSubscriber.payload_type == self._get_payload_type_name()
                )
            )

            for sql_subscriber in result.scalars():
                try:
                    subscriber = self.subscriber_serializer.deserialize(
                        sql_subscriber.subscriber_data
                    )
                    subscription = Subscription(
                        id=sql_subscriber.id, subscriber=subscriber
                    )
                    self._subscription_cache[sql_subscriber.id] = subscription
                except Exception as e:
                    _LOGGER.warning(
                        f"Failed to load subscriber {sql_subscriber.id}: {e}"
                    )

        self._subscription_cache_dirty = False

    async def _get_cached_subscriptions(self) -> list[Subscription[T]]:
        """Get all subscriptions from cache"""
        if self._subscription_cache_dirty:
            await self._load_subscription_cache()
        return list(self._subscription_cache.values())

    def _add_subscription_to_cache(self, subscription: Subscription[T]):
        """Add a subscription to the cache"""
        self._subscription_cache[subscription.id] = subscription

    def _remove_subscription_from_cache(self, subscriber_id: UUID):
        """Remove a subscription from the cache"""
        self._subscription_cache.pop(subscriber_id, None)
