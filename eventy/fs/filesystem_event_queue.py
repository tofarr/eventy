import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime
import os
from pathlib import Path
from typing import TypeVar, Optional
from uuid import UUID, uuid4

from eventy.event_queue import EventQueue
from eventy.fs.filesystem_event_coordinator import FilesystemEventCoordinator
from eventy.fs.filesystem_event_store import FilesystemEventStore
from eventy.fs.filesystem_watch import FilesystemWatch
from eventy.event_status import EventStatus
from eventy.fs.filesystem_worker_registry import FilesystemWorkerRegistry
from eventy.page import Page
from eventy.queue_event import QueueEvent
from eventy.serializers.serializer import Serializer, get_default_serializer
from eventy.subscriber.subscriber import Subscriber
from eventy.subscriber.subscription import Subscription

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


@dataclass
class FilesystemEventQueue(EventQueue[T]):
    """
    Filesystem-based event queue implementation.

    Events are stored as individual files in an 'events' directory.
    When a threshold is reached (by count or size), events are consolidated
    into numbered page files in a 'pages' directory.
    Subscribers are persisted to individual files in a 'subscribers' directory.
    """

    event_type: type[T]
    root_dir: Path

    worker_registry: FilesystemWorkerRegistry | None = None
    event_store: FilesystemEventStore | None = None
    event_coordinator: FilesystemEventCoordinator | None = None

    subscriber_serializer: Serializer[Subscriber[T]] = field(
        default_factory=get_default_serializer
    )
    subscribers: dict[UUID, Subscriber[T]] = field(default_factory=dict)
    subscriber_dir: Path | None = None
    sync_subscribers: bool = True

    # FilesystemWatch instances for monitoring
    subscriber_watch_sleep: int = 15
    worker_event_watch_sleep: int = 1
    _subscriber_watch: FilesystemWatch | None = field(default=None, init=False)
    _worker_event_watch: FilesystemWatch | None = field(default=None, init=False)

    def __post_init__(self) -> None:
        """Initialize directory structure"""
        self.root_dir.mkdir(parents=True, exist_ok=True)

        if self.worker_registry is None:
            self.worker_registry = FilesystemWorkerRegistry(
                worker_id=uuid4(),
                worker_dir=self.root_dir / "worker",
            )

        if self.event_store is None:
            self.event_store = FilesystemEventStore(
                payload_dir=self.root_dir / "payload",
                page_dir=self.root_dir / "page",
                meta_dir=self.root_dir / "meta",
            )

        if self.event_coordinator is None:
            self.event_coordinator = FilesystemEventCoordinator(
                worker_registry=self.worker_registry,
                event_store=self.event_store,
                process_dir=self.root_dir / "process",
                worker_event_dir=self.root_dir / "worker_event",
            )

        if self.subscriber_dir is None:
            self.subscriber_dir = self.root_dir / "subscriber"
        self.subscriber_dir.mkdir(parents=True, exist_ok=True)

        if self.sync_subscribers:
            self._load_subscribers_from_disk()
        self._setup_filesystem_watches()

    @property
    def worker_id(self):
        return self.worker_registry.worker_id

    def _load_subscribers_from_disk(self) -> None:
        """Load all subscribers from the subscribers directory"""
        subscribers = {}
        if self.subscriber_dir.exists():
            for subscriber_file in self.subscriber_dir.iterdir():
                try:
                    subscriber_id = UUID(subscriber_file.name)
                    with open(subscriber_file, "rb") as f:
                        subscriber_data = f.read()
                    subscriber = self.subscriber_serializer.deserialize(subscriber_data)
                    subscribers[subscriber_id] = subscriber
                except (ValueError, OSError) as e:
                    _LOGGER.warning(f"Failed to load subscriber {subscriber_file}: {e}")
        self.subscribers = subscribers

    def _setup_filesystem_watches(self) -> None:
        """Set up FilesystemWatch instances for monitoring directories"""
        # Watch subscriber events directory for changes and update subscribers
        if self.sync_subscribers:
            self._subscriber_watch = FilesystemWatch(
                path=self.subscriber_dir,
                callback=self._on_subscriber_directory_change,
                polling_frequency=self.subscriber_watch_sleep,
                monitor_deletes=True,
            )

        # Watch worker event directory for new events to process
        worker_event_dir = (
            self.event_coordinator.worker_event_dir / self.worker_registry.worker_id.hex
        )
        worker_event_dir.mkdir(parents=True, exist_ok=True)

        self._worker_event_watch = FilesystemWatch(
            path=worker_event_dir,
            callback=self._on_worker_event_directory_change,
            polling_frequency=self.worker_event_watch_sleep,
            monitor_deletes=False,  # Only monitor new events (file additions)
        )

    async def _on_subscriber_directory_change(self) -> None:
        """Callback triggered when subscriber directory changes"""
        _LOGGER.debug("Subscriber directory changed, reloading subscribers from disk")
        self._load_subscribers_from_disk()

    async def _on_worker_event_directory_change(self) -> None:
        """Callback triggered when new events are available for processing"""
        _LOGGER.debug("New events detected for processing")
        await self._process_worker_events()

    async def _process_worker_events(self) -> None:
        """Process events that are available for the current worker"""
        event_ids = self.event_coordinator.get_event_ids_to_process_for_current_worker()
        _LOGGER.debug(
            f"Processing {len(event_ids)} events for worker {self.worker_registry.worker_id}"
        )

        for event_id in event_ids:
            _LOGGER.info(f"Processing event {event_id}")
            event = self.event_store.get_event(event_id)
            if event.status == EventStatus.PENDING:
                self.event_store.update_event_status(event_id, EventStatus.PROCESSING)

            # Get the primary worker ID for this event
            process_meta = self.event_coordinator.get_process_meta(event_id)

            _LOGGER.info(
                f"Notifying {len(self.subscribers)} subscribers for event {event_id}"
            )
            for subscriber in list(self.subscribers.values()):
                try:
                    await subscriber.on_worker_event(
                        event,
                        self.worker_registry.worker_id,
                        process_meta.primary_worker_id,
                    )
                    _LOGGER.info(
                        f"Successfully notified subscriber for event {event_id}"
                    )
                except Exception:
                    self.event_store.update_event_status(event_id, EventStatus.ERROR)
                    _LOGGER.error("subscriber_error", exc_info=True, stack_info=True)

            self.event_coordinator.mark_event_processed_for_current_worker(event_id)
            status = self.event_coordinator.get_status(event_id, process_meta)
            if status == EventStatus.PROCESSING:
                continue
            self.event_coordinator.unscheduled_event(event_id)
            self.event_store.update_event_status(event_id, status)

    async def publish(self, payload: T) -> QueueEvent[T]:
        """Publish an event to the queue"""
        event_id, meta = self.event_store.add_event(payload)
        self.event_coordinator.schedule_event_for_processing(event_id)
        return QueueEvent(
            id=event_id,
            payload=payload,
            status=EventStatus.PENDING,
            created_at=meta.created_at,
        )

    async def subscribe(self, subscriber: Subscriber[T]) -> UUID:
        """Subscribe to events

        Args:
            subscriber: The subscriber to add. Its payload_type must be the same as or a superclass
                       of the queue's event_type.

        Returns:
            UUID: A unique identifier for the subscriber that can be used to unsubscribe

        Raises:
            TypeError: If the subscriber's payload_type is not compatible with the queue's event_type
        """
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
        self.subscribers[subscriber_id] = subscriber

        # Persist subscriber to disk
        subscriber_file = self.subscriber_dir / subscriber_id.hex
        subscriber_data = self.subscriber_serializer.serialize(subscriber)
        with open(subscriber_file, "wb") as f:
            f.write(subscriber_data)

        return subscriber_id

    async def unsubscribe(self, subscriber_id: UUID) -> bool:
        """Remove a subscriber from this queue

        Args:
            subscriber_id: The UUID returned by subscribe()

        Returns:
            bool: True if the subscriber was found and removed, False otherwise
        """
        if subscriber_id in self.subscribers:
            del self.subscribers[subscriber_id]

            # Remove subscriber file from disk
            subscriber_file = self.subscriber_dir / subscriber_id.hex
            try:
                subscriber_file.unlink()
            except FileNotFoundError:
                _LOGGER.warning(
                    f"Subscriber file {subscriber_file} not found during unsubscribe"
                )

            return True
        return False

    async def get_subscriber(self, subscriber_id: UUID) -> Subscriber[T]:
        """Get subscriber with id given"""
        if subscriber_id not in self.subscribers:
            raise KeyError(f"Subscriber {subscriber_id} not found")
        return self.subscribers[subscriber_id]

    async def search_subscriptions(
        self, page_id: Optional[str], limit: int = 100
    ) -> Page[Subscription[T]]:
        """Get all subscribers along with their IDs

        Returns:
            Page[Subscription[T]]: A page of subscriptions with subscriber IDs and their subscriber objects
        """
        # Convert subscribers dict to list of Subscription objects
        all_subscriptions = [
            Subscription(id=sub_id, subscriber=subscriber)
            for sub_id, subscriber in self.subscribers.items()
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

    async def list_subscribers(self) -> dict[UUID, Subscriber[T]]:
        """Get all subscribers along with their IDs (backward compatibility method)

        Returns:
            dict[UUID, Subscriber[T]]: A dictionary mapping subscriber IDs to their subscriber objects
        """
        return self.subscribers.copy()

    async def search_events(
        self,
        page_id: Optional[str] = None,
        limit: Optional[int] = 100,
        created_at__min: Optional[datetime] = None,
        created_at__max: Optional[datetime] = None,
        status__eq: Optional[EventStatus] = None,
    ) -> Page[QueueEvent[T]]:
        """Get events matching the criteria"""

        current_event_id = (
            self.event_store.next_event_id - 1
        )  # Start from the last created event
        if page_id:
            current_event_id = int(page_id)

        # If no events have been created yet, return empty page
        if current_event_id < 1:
            return Page(items=[], next_page_id=None)

        items = []
        next_page_id = None
        for event in self.event_store.iter_events_from(current_event_id):
            if created_at__min and event.created_at < created_at__min:
                continue
            if created_at__max and event.created_at > created_at__max:
                continue
            if status__eq and event.status != status__eq:
                continue
            if len(items) >= limit:
                next_page_id = str(event.id)
                break
            items.append(event)

        return Page(items=items, next_page_id=next_page_id)

    async def count_events(
        self,
        created_at__min: Optional[datetime] = None,
        created_at__max: Optional[datetime] = None,
        status__eq: Optional[EventStatus] = None,
    ) -> int:
        """Get the number of events matching the criteria"""
        if created_at__max is None and created_at__min is None and status__eq is None:
            result = self.event_store.count_events()
            return result

        count = 0
        async for _ in self.iter_events(created_at__min, created_at__max, status__eq):
            count += 1
        return count

    async def get_event(self, event_id: int) -> QueueEvent[T]:
        """Get an event by its ID"""
        return self.event_store.get_event(event_id)

    async def wait_for_processing(self, timeout: float = 1.0) -> None:
        """Wait for background tasks to process pending events

        This is primarily useful for testing to ensure events are processed
        before making assertions.

        Args:
            timeout: Maximum time to wait in seconds
        """
        import time

        start_time = time.time()

        while time.time() - start_time < timeout:
            # Check if there are any pending events to process
            event_ids = (
                self.event_coordinator.get_event_ids_to_process_for_current_worker()
            )
            if not event_ids:
                # No pending events, wait a bit more to ensure processing is complete
                await asyncio.sleep(0.05)
                break
            await asyncio.sleep(0.05)

        # Give a final small delay to ensure all async operations complete
        await asyncio.sleep(0.05)

    async def __aenter__(self):
        await self.event_store.__aenter__()
        await self.worker_registry.__aenter__()
        await self.event_coordinator.__aenter__()

        # Start filesystem watches
        if self._subscriber_watch:
            await self._subscriber_watch.__aenter__()
        await self._worker_event_watch.__aenter__()

        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        # Stop filesystem watches
        if self._subscriber_watch:
            await self._subscriber_watch.__aexit__(exc_type, exc_value, traceback)
        await self._worker_event_watch.__aexit__(exc_type, exc_value, traceback)

        # Clean up other components
        await self.event_store.__aexit__(exc_type, exc_value, traceback)
        await self.worker_registry.__aexit__(exc_type, exc_value, traceback)
        await self.event_coordinator.__aexit__(exc_type, exc_value, traceback)
