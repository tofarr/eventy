"""SQL-based queue manager implementation."""

import asyncio
import logging
from dataclasses import dataclass, field
from typing import TypeVar, Dict, Optional

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.engine import Engine
except ImportError as e:
    raise ImportError(
        "SQLAlchemy is required for SQL queue manager. Install with: pip install eventy[sql]"
    ) from e

from eventy.event_queue import EventQueue
from eventy.eventy_error import EventyError
from eventy.queue_manager import QueueManager
from eventy.serializers.serializer import Serializer, get_default_serializer
from eventy.sql.models import Base, SqlEvent, SqlEventResult, SqlSubscriber, SqlClaim
from eventy.sql.sql_event_queue import SqlEventQueue

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


@dataclass
class SqlQueueManager(QueueManager):
    """
    SQL-based implementation of QueueManager using SQLAlchemy.
    
    This manager creates and manages SQL-based event queues that store
    events, results, subscribers, and claims in database tables.
    """

    database_url: str
    serializer: Serializer = field(default_factory=get_default_serializer)

    # Internal storage
    _queues: Dict[type, EventQueue] = field(default_factory=dict, init=False)
    _entered: bool = field(default=False, init=False)
    _engine: Optional[Engine] = field(default=None, init=False)
    _session_factory: Optional[sessionmaker] = field(default=None, init=False)

    def __post_init__(self):
        """Initialize the database connection and create tables"""
        # Convert async database URL to sync for the manager
        sync_url = self.database_url.replace('+aiosqlite', '')
        self._engine = create_engine(sync_url)
        self._session_factory = sessionmaker(bind=self._engine)
        
        # Create all tables
        Base.metadata.create_all(self._engine)
        
        _LOGGER.info(f"Initialized SqlQueueManager with database: {sync_url}")

    def _check_entered(self) -> None:
        """Check if the manager has been entered, raise error if not"""
        if not self._entered:
            raise EventyError(
                "SqlQueueManager must be entered using async context manager before use"
            )

    async def __aenter__(self):
        """Begin using this queue manager"""
        self._entered = True

        # Start all existing queues
        await asyncio.gather(*[queue.__aenter__() for queue in self._queues.values()])

        _LOGGER.info(f"Started SqlQueueManager")
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        """Finish using this queue manager"""
        self._entered = False

        # Close all queues
        await asyncio.gather(
            *[
                queue.__aexit__(exc_type, exc_value, traceback)
                for queue in self._queues.values()
            ]
        )

        self._queues.clear()
        
        # Close database connection
        if self._engine:
            self._engine.dispose()
            
        _LOGGER.info(f"Stopped SqlQueueManager")

    def _create_queue(self, payload_type: type[T]) -> EventQueue[T]:
        """Create a new SQL event queue instance"""
        queue = SqlEventQueue(
            database_url=self.database_url,
            payload_type=payload_type,
            event_serializer=self.serializer,
            result_serializer=self.serializer,
            subscriber_serializer=self.serializer,
            claim_serializer=self.serializer,
        )
        _LOGGER.debug(f"Created SqlEventQueue for {payload_type}")
        return queue

    async def get_event_queue(self, payload_type: type[T]) -> EventQueue[T]:
        """Get an event queue for the event type given"""
        self._check_entered()

        if payload_type not in self._queues:
            raise EventyError(f"No queue registered for payload type: {payload_type}")

        return self._queues[payload_type]

    async def get_queue_types(self) -> list[type]:
        """List all available event queues"""
        self._check_entered()

        return list(self._queues.keys())

    async def register(self, payload_type: type[T]) -> None:
        """Register a payload type (Create an event queue)"""

        if payload_type in self._queues:
            _LOGGER.info(f"Queue for payload type {payload_type} already registered")
            return

        # Create new SQL event queue
        queue = self._create_queue(payload_type)

        if self._entered:
            await queue.__aenter__()

        self._queues[payload_type] = queue
        _LOGGER.info(f"Registered queue for payload type: {payload_type}")

    async def deregister(self, payload_type: type[T]) -> None:
        """Deregister a payload type (Shut down an event queue)"""
        self._check_entered()

        if payload_type not in self._queues:
            _LOGGER.warning(f"No queue found for payload type: {payload_type}")
            return

        # Close the queue
        queue = self._queues[payload_type]
        try:
            if self._entered:
                await queue.__aexit__(None, None, None)
        except Exception as e:
            _LOGGER.warning(
                f"Error closing queue for {payload_type}: {e}", exc_info=True
            )

        del self._queues[payload_type]
        _LOGGER.info(f"Deregistered queue for payload type: {payload_type}")

    async def reset(self, payload_type: type[T]):
        """Clear all Events, Results and claims for a specific payload type"""
        self._check_entered()
        
        if payload_type not in self._queues:
            _LOGGER.warning(f"No queue found for payload type: {payload_type}")
            return
            
        # Get the payload type name for filtering
        payload_type_name = f"{payload_type.__module__}.{payload_type.__name__}"
        
        with self._session_factory() as session:
            try:
                # Delete claims for this payload type
                session.execute(
                    text("DELETE FROM eventy_claims WHERE payload_type = :payload_type"),
                    {"payload_type": payload_type_name}
                )
                
                # Delete subscribers for this payload type
                session.execute(
                    text("DELETE FROM eventy_subscribers WHERE payload_type = :payload_type"),
                    {"payload_type": payload_type_name}
                )
                
                # Delete results for events of this payload type
                session.execute(
                    text("""
                        DELETE FROM eventy_results 
                        WHERE event_id IN (
                            SELECT id FROM eventy_events WHERE payload_type = :payload_type
                        )
                    """),
                    {"payload_type": payload_type_name}
                )
                
                # Delete events for this payload type
                session.execute(
                    text("DELETE FROM eventy_events WHERE payload_type = :payload_type"),
                    {"payload_type": payload_type_name}
                )
                
                session.commit()
                _LOGGER.info(f"Reset all data for payload type: {payload_type}")
                
                # Clear the subscription cache for the queue
                queue = self._queues[payload_type]
                if hasattr(queue, '_subscription_cache'):
                    queue._subscription_cache.clear()
                    queue._subscription_cache_dirty = True
                    
            except Exception as e:
                session.rollback()
                _LOGGER.error(f"Error resetting data for {payload_type}: {e}", exc_info=True)
                raise