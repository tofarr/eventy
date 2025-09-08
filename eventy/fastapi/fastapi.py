from datetime import datetime
import logging
from typing import TypeVar, Union
from uuid import UUID, uuid4
from eventy.event_queue import EventQueue
from eventy.event_status import EventStatus
from eventy.eventy_config import EventyConfig
from eventy.fastapi.websocket_subscriber import WEBSOCKETS, WebsocketSubscriber
from eventy.queue_manager import QueueManager
from fastapi import (
    APIRouter,
    FastAPI,
    HTTPException,
    status,
    WebSocket,
    WebSocketDisconnect,
    WebSocketState,
)
from pydantic import BaseModel

from eventy.subscriber.subscriber import Subscriber


T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


def add_endpoints(fastapi: FastAPI, queue_manager: QueueManager, config: EventyConfig):
    for payload_type in config.get_payload_types():
        router = APIRouter(prefix=f"/{payload_type.__name__}")
        add_queue_endpoints(router, payload_type, queue_manager, config)


def add_queue_endpoints(
    fastapi: FastAPI,
    payload_type: type[T],
    queue_manager: QueueManager,
    config: EventyConfig,
):

    class ResponseEvent(BaseModel):
        id: int
        payload: payload_type  # type: ignore
        status: EventStatus
        created_at: datetime

    class ResponsePage(BaseModel):
        items: list[payload_type]  # type: ignore
        next_page_id: str | None

    subscriber_type = Union[
        tuple(
            s
            for s in config.get_subscriber_types()
            if s.payload_type == payload_type
            or issubclass(s.payload_type, payload_type)
        )
    ]

    class SubscriptionResponse(BaseModel):
        id: UUID
        subscriber: subscriber_type  # type: ignore

    class SubscriptionPage(BaseModel):
        items: list[SubscriptionResponse]  # type: ignore
        next_page_id: str | None

    @fastapi.post("/event")
    async def publish(payload: payload_type) -> ResponseEvent:  # type: ignore
        event_queue: EventQueue[T] = await queue_manager.get_event_queue(payload_type)
        event = await event_queue.publish(payload)
        return event

    @fastapi.get("/event/search")
    async def search_events(
        page_id: str | None = None,
        limit: int = 100,
        created_at__min: datetime | None = None,
        created_at__max: datetime | None = None,
        status__eq: EventStatus | None = None,
    ) -> ResponsePage:
        assert limit <= 100
        event_queue: EventQueue[T] = await queue_manager.get_event_queue(payload_type)
        page = await event_queue.search_events(
            page_id, limit, created_at__min, created_at__max, status__eq
        )
        return page

    @fastapi.get("/event/{id}")
    async def get_event(id: int) -> ResponseEvent:
        event_queue: EventQueue[T] = await queue_manager.get_event_queue(payload_type)
        try:
            event = await event_queue.get_event(id)
            return event
        except Exception:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    @fastapi.get("/event")
    async def get_all_events(event_ids: list[int]) -> list[ResponseEvent | None]:
        assert len(event_ids) <= 100
        event_queue: EventQueue[T] = await queue_manager.get_event_queue(payload_type)
        events = await event_queue.get_all_events(event_ids)
        return events

    # TODO: Add an add_subscriber endpoint...
    @fastapi.get("/subscriber/search")
    async def search_subscriptions(
        page_id: str | None = None, limit: int = 100
    ) -> SubscriptionPage:
        event_queue: EventQueue[T] = await queue_manager.get_event_queue(payload_type)
        try:
            return await event_queue.search_subscriptions(page_id, limit)
        except Exception:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    @fastapi.post("/subscriber")
    async def add_subscriber(subscriber: subscriber_type) -> UUID:  # type: ignore
        event_queue: EventQueue[T] = await queue_manager.get_event_queue(payload_type)
        subscription_id = await event_queue.subscribe(subscriber)
        return subscription_id

    @fastapi.get("/subscriber/{id}")
    async def get_subscriber(subscriber: subscriber_type) -> UUID:  # type: ignore
        event_queue: EventQueue[T] = await queue_manager.get_event_queue(payload_type)
        try:
            subscriber = await event_queue.get_subscriber(id)
            return SubscriptionResponse(id, subscriber)
        except Exception:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    @fastapi.delete("/subscriber/{id}")
    async def remove_subscriber(id: UUID) -> bool:
        event_queue: EventQueue[T] = await queue_manager.get_event_queue(payload_type)
        return event_queue.unsubscribe(id)

    @fastapi.websocket("/socket")
    async def socket(
        websocket: WebSocket,
    ):
        await websocket.accept()
        event_queue: EventQueue[T] = queue_manager.get_event_queue(payload_type)
        websocket_id = uuid4()
        listener_id = event_queue.subscribe(
            WebsocketSubscriber(websocket_id=websocket_id)
        )
        try:
            while websocket.application_state == WebSocketState.CONNECTED:
                data = await websocket.receive_json()
                payload = payload_type.model_validate(data)
                await event_queue.publish(payload)
        except WebSocketDisconnect as e:
            _LOGGER.debug("websocket_closed")
        finally:
            await event_queue.unsubscribe(listener_id)
            WEBSOCKETS.pop(websocket_id)
