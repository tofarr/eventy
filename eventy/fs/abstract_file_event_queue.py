import os
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import TypeVar, Optional, AsyncIterator
from uuid import UUID, uuid4

from eventy.event_queue import EventQueue
from eventy.event_result import EventResult
from eventy.page import Page
from eventy.queue_event import QueueEvent
from eventy.serializers.serializer import Serializer, get_default_serializer
from eventy.subscriber.subscriber import Subscriber
from eventy.subscriber.subscription import Subscription

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


@dataclass
class AbstractFileEventQueue(EventQueue[T], ABC):
    """
    Abstract file-based event queue implementation using dataclasses.
    
    This class provides a file-based storage system where:
    - Events are stored in an 'events' directory, one per file named by event ID
    - Results are stored in a 'results' directory, one per file named by result ID  
    - Subscribers are stored in a 'subscriptions' directory, one per file named by subscriber ID
    - Each type of data uses its own serializer for storage
    
    Concrete subclasses must implement __aenter__ and __aexit__ methods.
    """
    
    root_dir: Path
    payload_type: type[T]
    
    # Serializers for different data types
    event_serializer: Serializer[QueueEvent[T]] = field(default_factory=get_default_serializer)
    result_serializer: Serializer[EventResult] = field(default_factory=get_default_serializer)
    subscriber_serializer: Serializer[Subscriber[T]] = field(default_factory=get_default_serializer)
    
    # Running state
    running: bool = field(default=False, init=False)
    
    # Worker ID for this instance
    worker_id: UUID = field(default_factory=uuid4)
    
    # Event counter for generating sequential IDs
    _next_event_id: int = field(default=1, init=False)
    
    # Track processed events to avoid duplicate processing
    _processed_events: set = field(default_factory=set, init=False)
    
    def __post_init__(self):
        """Initialize directory structure"""
        self.root_dir = Path(self.root_dir)
        self.root_dir.mkdir(parents=True, exist_ok=True)
        
        # Create subdirectories
        self.events_dir = self.root_dir / "events"
        self.results_dir = self.root_dir / "results" 
        self.subscriptions_dir = self.root_dir / "subscriptions"
        
        self.events_dir.mkdir(exist_ok=True)
        self.results_dir.mkdir(exist_ok=True)
        self.subscriptions_dir.mkdir(exist_ok=True)
        
        # Initialize next event ID by checking existing events
        self._initialize_event_counter()
    
    def _initialize_event_counter(self):
        """Initialize the event counter based on existing events"""
        max_id = 0
        if self.events_dir.exists():
            for event_file in self.events_dir.iterdir():
                try:
                    event_id = int(event_file.name)
                    max_id = max(max_id, event_id)
                except ValueError:
                    # Skip files that aren't numeric
                    continue
        self._next_event_id = max_id + 1
    
    def _check_running(self):
        """Check if the queue is running and raise error if not"""
        if not self.running:
            raise RuntimeError("EventQueue is not running. Call __aenter__ first.")
    
    def get_worker_id(self) -> UUID:
        """Get the id of the current worker"""
        return self.worker_id
    
    def get_payload_type(self) -> type[T]:
        """Get the type of payload handled by this queue"""
        return self.payload_type
    
    async def publish(self, payload: T) -> QueueEvent[T]:
        """Publish an event to this queue"""
        self._check_running()
        
        # Create event with sequential ID
        event_id = self._next_event_id
        self._next_event_id += 1
        
        event = QueueEvent(id=event_id, payload=payload)
        
        # Serialize and write event to file
        event_file = self.events_dir / str(event_id)
        event_data = self.event_serializer.serialize(event)
        
        with open(event_file, 'wb') as f:
            f.write(event_data)
        
        _LOGGER.info(f"Published event {event_id} to {event_file}")
        return event
    
    async def subscribe(self, subscriber: Subscriber[T], check_subscriber_unique: bool = True, from_index: int | None = None) -> Subscription[T]:
        """Add a subscriber to this queue"""
        self._check_running()
        
        # Validate subscriber payload type compatibility
        if not hasattr(subscriber, 'payload_type') or subscriber.payload_type is None:
            raise TypeError(f"Subscriber {subscriber} must have a payload_type attribute")
        
        if not issubclass(self.payload_type, subscriber.payload_type):
            raise TypeError(
                f"Subscriber payload_type {subscriber.payload_type.__name__} is not compatible "
                f"with queue payload_type {self.payload_type.__name__}. The queue's payload_type must "
                f"be the same as or a subclass of the subscriber's payload_type."
            )
        
        # Check for existing subscriber if requested
        if check_subscriber_unique:
            async for existing_subscription in self._iter_subscriptions():
                if existing_subscription.subscriber == subscriber:
                    _LOGGER.info(f"Found existing subscription {existing_subscription.id} for subscriber")
                    return existing_subscription
        
        # Create new subscription
        subscriber_id = uuid4()
        subscription = Subscription(id=subscriber_id, subscriber=subscriber)
        
        # Serialize and write subscriber to file
        subscriber_file = self.subscriptions_dir / str(subscriber_id)
        subscriber_data = self.subscriber_serializer.serialize(subscriber)
        
        with open(subscriber_file, 'wb') as f:
            f.write(subscriber_data)
        
        _LOGGER.info(f"Added subscription {subscriber_id} to {subscriber_file}")
        return subscription
    
    async def unsubscribe(self, subscriber_id: UUID) -> bool:
        """Remove a subscriber from this queue"""
        self._check_running()
        
        subscriber_file = self.subscriptions_dir / str(subscriber_id)
        if subscriber_file.exists():
            subscriber_file.unlink()
            _LOGGER.info(f"Removed subscription {subscriber_id}")
            return True
        return False
    
    async def get_subscriber(self, subscriber_id: UUID) -> Subscriber[T]:
        """Get subscriber with id given"""
        self._check_running()
        
        subscriber_file = self.subscriptions_dir / str(subscriber_id)
        if not subscriber_file.exists():
            raise KeyError(f"Subscriber {subscriber_id} not found")
        
        with open(subscriber_file, 'rb') as f:
            subscriber_data = f.read()
        
        return self.subscriber_serializer.deserialize(subscriber_data)
    
    async def _iter_subscriptions(self) -> AsyncIterator[Subscription[T]]:
        """Iterate over all subscriptions"""
        if not self.subscriptions_dir.exists():
            return
        
        for subscriber_file in self.subscriptions_dir.iterdir():
            try:
                subscriber_id = UUID(subscriber_file.name)
                subscriber = await self.get_subscriber(subscriber_id)
                yield Subscription(id=subscriber_id, subscriber=subscriber)
            except (ValueError, OSError) as e:
                _LOGGER.warning(f"Failed to load subscription {subscriber_file}: {e}")
    
    async def search_subscriptions(self, page_id: Optional[str], limit: int = 100) -> Page[Subscription[T]]:
        """Get all subscribers along with their IDs"""
        self._check_running()
        
        # Collect all subscriptions
        all_subscriptions = []
        async for subscription in self._iter_subscriptions():
            all_subscriptions.append(subscription)
        
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
        
        result_file = self.results_dir / str(result_id)
        if not result_file.exists():
            raise KeyError(f"Result {result_id} not found")
        
        with open(result_file, 'rb') as f:
            result_data = f.read()
        
        return self.result_serializer.deserialize(result_data)
    
    async def _iter_results(self, 
                           event_id__eq: Optional[int] = None,
                           worker_id__eq: Optional[UUID] = None,
                           created_at__gte: Optional[datetime] = None,
                           created_at__lte: Optional[datetime] = None) -> AsyncIterator[EventResult]:
        """Iterate over results matching the given criteria"""
        if not self.results_dir.exists():
            return
        
        for result_file in self.results_dir.iterdir():
            try:
                result_id = UUID(result_file.name)
                result = await self.get_result(result_id)
                
                # Apply filters
                if event_id__eq is not None and result.event_id != event_id__eq:
                    continue
                if worker_id__eq is not None and result.worker_id != worker_id__eq:
                    continue
                if created_at__gte is not None and result.created_at < created_at__gte:
                    continue
                if created_at__lte is not None and result.created_at > created_at__lte:
                    continue
                
                yield result
            except (ValueError, OSError) as e:
                _LOGGER.warning(f"Failed to load result {result_file}: {e}")
    
    async def search_results(self,
                           page_id: Optional[int] = None,
                           limit: int = 100,
                           event_id__eq: Optional[int] = None,
                           worker_id__eq: Optional[int] = None,
                           created_at__gte: Optional[datetime] = None,
                           created_at__lte: Optional[datetime] = None) -> Page[EventResult]:
        """Get existing results with optional paging parameters"""
        self._check_running()
        
        # Collect all matching results
        all_results = []
        async for result in self._iter_results(event_id__eq, worker_id__eq, created_at__gte, created_at__lte):
            all_results.append(result)
        
        # Sort by created_at for consistent pagination
        all_results.sort(key=lambda r: r.created_at)
        
        # Handle pagination
        start_index = page_id if page_id is not None else 0
        end_index = start_index + limit
        page_results = all_results[start_index:end_index]
        
        # Determine next page ID
        next_page_id = None
        if end_index < len(all_results):
            next_page_id = end_index
        
        return Page(items=page_results, next_page_id=next_page_id)
    
    async def count_results(self,
                          event_id__eq: Optional[int] = None,
                          worker_id__eq: Optional[int] = None,
                          created_at__gte: Optional[datetime] = None,
                          created_at__lte: Optional[datetime] = None) -> int:
        """Get the number of results matching the criteria given"""
        self._check_running()
        
        count = 0
        async for _ in self._iter_results(event_id__eq, worker_id__eq, created_at__gte, created_at__lte):
            count += 1
        return count
    
    async def _store_result(self, result: EventResult):
        """Store a result to the results directory"""
        result_file = self.results_dir / str(result.id)
        result_data = self.result_serializer.serialize(result)
        
        with open(result_file, 'wb') as f:
            f.write(result_data)
        
        _LOGGER.info(f"Stored result {result.id} to {result_file}")
    
    def _mark_event_processed(self, event_id: int):
        """Mark an event as processed to avoid duplicate processing"""
        self._processed_events.add(event_id)
    
    def _is_event_processed(self, event_id: int) -> bool:
        """Check if an event has already been processed"""
        return event_id in self._processed_events
    
    # Abstract methods that must be implemented by concrete subclasses
    @abstractmethod
    async def __aenter__(self):
        """Start this event queue - must be implemented by subclasses"""
        pass
    
    @abstractmethod
    async def __aexit__(self, exc_type, exc_value, traceback):
        """Close this event queue - must be implemented by subclasses"""
        pass