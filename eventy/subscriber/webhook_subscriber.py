from http.client import HTTPResponse
import logging
from typing import TypeVar, Dict, Optional, Any
from urllib.parse import urlparse
from uuid import UUID

import httpx

from eventy.subscriber.subscriber import Subscriber
from eventy.queue_event import QueueEvent
from eventy.serializers.serializer import Serializer
from eventy.serializers.json_serializer import JsonSerializer

T = TypeVar("T")

logger = logging.getLogger(__name__)


class WebhookSubscriber(Subscriber[T]):
    """
    A subscriber that sends HTTP webhook requests when events are received.

    This subscriber uses httpx to send HTTP requests to a specified URL with
    the event payload serialized using a configurable serializer.
    """

    def __init__(
        self,
        url: str,
        payload_type: type[T],
        serializer: Optional[Serializer[T]] = None,
        headers: Optional[Dict[str, str]] = None,
        request_method: str = "POST",
        timeout: float = 30.0,
        retry_attempts: int = 3,
        retry_delay: float = 1.0,
    ):
        """
        Initialize the webhook subscriber.

        Args:
            url: The webhook URL to send requests to
            payload_type: The type of payload this subscriber handles
            serializer: Serializer to convert payload to request content (defaults to JsonSerializer)
            headers: Additional HTTP headers to include in requests
            request_method: HTTP method to use (GET, POST, PUT, PATCH, DELETE)
            timeout: Request timeout in seconds
            retry_attempts: Number of retry attempts on failure
            retry_delay: Delay between retry attempts in seconds

        Raises:
            ValueError: If URL is invalid or request_method is not supported
        """
        # Validate URL
        parsed_url = urlparse(url)
        if not parsed_url.scheme or not parsed_url.netloc:
            raise ValueError(f"Invalid URL: {url}")

        # Validate request method
        valid_methods = {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
        request_method = request_method.upper()
        if request_method not in valid_methods:
            raise ValueError(f"Unsupported request method: {request_method}")

        self.url = url
        self.payload_type = payload_type
        self.serializer = serializer or JsonSerializer[T]()
        self.headers = headers or {}
        self.request_method = request_method
        self.timeout = timeout
        self.retry_attempts = max(0, retry_attempts)
        self.retry_delay = max(0.0, retry_delay)

        # Set default content type based on serializer
        if "content-type" not in (k.lower() for k in self.headers.keys()):
            if self.serializer.is_json:
                self.headers["Content-Type"] = "application/json"
            else:
                self.headers["Content-Type"] = "application/octet-stream"

    async def on_event(self, event: QueueEvent[T], current_worker_id: UUID, primary_worker_id: UUID) -> None:
        """
        Handle an event by sending a webhook request.

        Args:
            event: The event to process
        """
        try:
            # Serialize the payload
            content = self.serializer.serialize(event.payload)

            # Prepare request data
            request_data = {
                "method": self.request_method,
                "url": self.url,
                "headers": self.headers,
                "timeout": self.timeout,
            }

            # Add content for methods that support it
            if self.request_method not in {"GET", "HEAD", "OPTIONS"}:
                request_data["content"] = content

            # Send request with retries
            await self._send_with_retries(request_data, event)

        except Exception as e:
            logger.error(
                f"Failed to send webhook for event {event.id}: {e}", exc_info=True
            )
            # Re-raise to allow the event queue to handle the error
            raise

    async def _send_with_retries(
        self, request_data: Dict[str, Any], event: QueueEvent[T]
    ) -> None:
        """
        Send HTTP request with retry logic.

        Args:
            request_data: Request parameters for httpx
            event: The original event (for logging)

        Raises:
            httpx.HTTPError: If all retry attempts fail
        """
        last_exception = None

        for attempt in range(self.retry_attempts + 1):
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.request(**request_data)
                    response.raise_for_status()

                    logger.debug(
                        f"Webhook sent successfully for event {event.id} "
                        f"(attempt {attempt + 1}): {response.status_code}"
                    )
                    return

            except httpx.HTTPError as e:
                last_exception = e
                logger.warning(
                    f"Webhook attempt {attempt + 1} failed for event {event.id}: {e}"
                )

                # Don't retry on client errors (4xx), only on server errors (5xx) and network issues
                response: HTTPResponse | None = getattr(e, "response", None)
                if response:
                    if 400 <= response.status_code < 500:
                        logger.error(
                            f"Client error {response.status_code} for webhook, not retrying"
                        )
                        raise

                # Wait before retry (except on last attempt)
                if attempt < self.retry_attempts:
                    import asyncio

                    await asyncio.sleep(
                        self.retry_delay * (attempt + 1)
                    )  # Exponential backoff

            except Exception as e:
                last_exception = e
                logger.warning(
                    f"Webhook attempt {attempt + 1} failed for event {event.id}: {e}"
                )

                # Wait before retry (except on last attempt)
                if attempt < self.retry_attempts:
                    import asyncio

                    await asyncio.sleep(self.retry_delay * (attempt + 1))

        # All attempts failed
        logger.error(
            f"All {self.retry_attempts + 1} webhook attempts failed for event {event.id}"
        )
        if last_exception:
            raise last_exception

    def __repr__(self) -> str:
        """String representation of the webhook subscriber."""
        return (
            f"WebhookSubscriber(url='{self.url}', method='{self.request_method}', "
            f"payload_type={self.payload_type.__name__})"
        )
