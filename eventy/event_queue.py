from abc import ABC, abstractmethod
from datetime import datetime
import logging
from typing import Callable, Generic, TypeVar, Optional, AsyncIterator
from uuid import UUID
from eventy.claim import Claim
from eventy.event_result import EventResult
from eventy.page import Page
from eventy.queue_event import QueueEvent
from eventy.subscriber import Subscriber
from eventy.subscription import Subscription

T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


class EventQueue(Generic[T], ABC):
    """Event queue for distributed processin. Within the context of an event queue
       results are only ever added - never updated or deleted """

    @abstractmethod
    async def __aenter__(self):
        """ Start this event queue"""

    @abstractmethod
    async def __aexit__(self, exc_type, exc_value, traceback):
        """ Close this event queue """

    @abstractmethod
    def get_worker_id(self) -> UUID:
        """
        Get the id of the current worker - this is a unique id which differentiates the current object
        from any others which may be subscribed - whether in the current process or anywhere outside it.
        """



    @abstractmethod
    def get_payload_type(self) -> type[T]:
        """ Get the type of payload handled by this queue """

    @abstractmethod
    async def subscribe(self, subscriber: Subscriber[T], check_subscriber_unique: bool = True, from_index: int | None = None) -> Subscription[T]:
        """
        Add a subscriber to this queue

        Args:
            subscriber: The subscriber to add
            check_subscriber_unique: Check if the subscriber is equal to an existing one and return that if possible rather than creating a new one
            from_index: The index of the event from which to subscribe (Events from this index will be immediately passed to the subscriber)

        Returns:
            UUID: A unique identifier for the subscriber that can be used to unsubscribe
        """

    @abstractmethod
    async def unsubscribe(self, subscriber_id: UUID) -> bool:
        """Remove a subscriber from this queue

        Args:
            subscriber_id: The UUID returned by subscribe()

        Returns:
            bool: True if the subscriber was found and removed, False otherwise
        """

    @abstractmethod
    async def get_subscriber(self, subscriber_id: UUID) -> Subscriber[T]:
        """Get subscriber with id given"""

    async def batch_get_subscriptions(
        self, subscriber_ids: list[UUID]
    ) -> list[Subscription[T] | None]:
        subscribers = []
        for subscriber_id in subscriber_ids:
            try:
                subscriber = await self.get_subscriber(subscriber_id)
                subscribers.append(
                    Subscription(id=subscriber_id, subscriber=subscriber)
                )
            except Exception:
                subscribers.append(None)
        return subscribers

    @abstractmethod
    async def search_subscriptions(
        self, page_id: Optional[str], limit: int = 100
    ) -> Page[Subscription[T]]:
        """Get all subscribers along with their IDs

        Returns:
            dict[UUID, Subscriber[T]]: A dictionary mapping subscriber IDs to their subscriber objects
        """

    async def count_subscriptions(self) -> int:
        count = sum(1 for subscription in self.iter_subscriptions())
        return count

    @abstractmethod
    async def publish(self, payload: T) -> QueueEvent[T]:
        """Publish an event to this queue"""

    @abstractmethod
    async def get_result(self, result_id: UUID) -> EventResult:
        """Get an event given its id."""

    async def batch_get_results(self, result_ids: list[UUID]) -> list[EventResult | None]:
        results = []
        for result_id in result_ids:
            try:
                result = await self.get_result(result_id)
                results.append(result)
            except Exception:
                _LOGGER.warning("error_getting_result", exc_info=True, stack_info=True)
                results.append(None)

        return results

    @abstractmethod
    async def search_results(
        self,
        page_id: Optional[int] = None,
        limit: int = 100,
        event_id__eq: Optional[int] = None,
        worker_id__eq: Optional[int] = None,
        created_at__gte: Optional[datetime] = None,
        created_at__lte: Optional[datetime] = None,
    ) -> Page[EventResult]:
        """Get existing results with optional paging parameters
        """

    @abstractmethod
    async def count_results(
        self,
        event_id__eq: Optional[int] = None,
        worker_id__eq: Optional[int] = None,
        created_at__gte: Optional[datetime] = None,
        created_at__lte: Optional[datetime] = None,
    ) -> int:
        """Get the number of events matching the criteria given"""

    @abstractmethod
    async def create_claim(self, claim_id: str, data: str | None = None) -> bool:
        """Create a claim with the given ID.
        
        Args:
            claim_id: The string ID for the claim
            data: Optional string data to store with the claim (e.g., worker info)
            
        Returns:
            bool: True if the claim was created successfully, False if it already exists
        """

    @abstractmethod
    async def get_claim(self, claim_id: str) -> Claim:
        """Get a claim by its ID.
        
        Args:
            claim_id: The string ID of the claim
            
        Returns:
            Claim: The claim object
            
        Raises:
            EventyError: If the claim is not found
        """

    async def batch_get_claims(self, claim_ids: list[str]) -> list[Claim | None]:
        """Get multiple claims by their IDs.
        
        Args:
            claim_ids: List of claim IDs to retrieve
            
        Returns:
            list[Claim | None]: List of claims, None for claims that don't exist
        """
        claims = []
        for claim_id in claim_ids:
            try:
                claim = await self.get_claim(claim_id)
                claims.append(claim)
            except Exception:
                _LOGGER.warning("error_getting_claim", exc_info=True, stack_info=True)
                claims.append(None)
        return claims

    @abstractmethod
    async def search_claims(
        self,
        page_id: Optional[str] = None,
        limit: int = 100,
        worker_id__eq: Optional[UUID] = None,
        created_at__gte: Optional[datetime] = None,
        created_at__lte: Optional[datetime] = None,
    ) -> Page[Claim]:
        """Search for claims with optional filtering and pagination.
        
        Args:
            page_id: Optional page ID for pagination
            limit: Maximum number of claims to return
            worker_id__eq: Filter by worker ID
            created_at__gte: Filter by minimum creation time
            created_at__lte: Filter by maximum creation time
            
        Returns:
            Page[Claim]: Paginated results
        """

    @abstractmethod
    async def count_claims(
        self,
        worker_id__eq: Optional[UUID] = None,
        created_at__gte: Optional[datetime] = None,
        created_at__lte: Optional[datetime] = None,
    ) -> int:
        """Count claims matching the given criteria.
        
        Args:
            worker_id__eq: Filter by worker ID
            created_at__gte: Filter by minimum creation time
            created_at__lte: Filter by maximum creation time
            
        Returns:
            int: Number of matching claims
        """