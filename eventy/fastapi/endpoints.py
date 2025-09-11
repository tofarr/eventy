from datetime import datetime
import logging
from typing import Annotated, TypeVar, Union
from uuid import UUID, uuid4

from eventy.event_queue import EventQueue
from eventy.event_result import EventResult
from eventy.config.eventy_config import EventyConfig
from eventy.fastapi.websocket_subscriber import (
    SERIALIZERS,
    WEBSOCKETS,
    WebsocketSubscriber,
)
from eventy.queue_manager import QueueManager
from eventy.queue_event import QueueEvent

from fastapi import (
    APIRouter,
    FastAPI,
    HTTPException,
    status,
    WebSocketDisconnect,
)
from fastapi.websockets import WebSocket, WebSocketState
from pydantic import BaseModel, Field, TypeAdapter

from eventy.serializers.pydantic_serializer import PydanticSerializer
from eventy.subscribers.subscriber import Subscriber, get_payload_type


T = TypeVar("T")
_LOGGER = logging.getLogger(__name__)


async def add_endpoints(
    fastapi: FastAPI, queue_manager: QueueManager, config: EventyConfig
):
    for payload_type in config.get_payload_types():
        await queue_manager.register(payload_type)
        router = APIRouter(prefix=f"/{payload_type.__name__}")
        add_queue_endpoints(router, payload_type, queue_manager, config)
        fastapi.include_router(router)


def add_queue_endpoints(
    fastapi: FastAPI,
    payload_type: type[T],
    queue_manager: QueueManager,
    config: EventyConfig,
):

    class EventResponse(BaseModel):
        id: int
        payload: payload_type  # type: ignore
        created_at: datetime

    class EventPage(BaseModel):
        items: list[QueueEvent[payload_type]]  # type: ignore
        next_page_id: str | None

    subscriber_types = []
    for subscriber_type in config.get_subscriber_types():
        subscriber_payload_type = get_payload_type(subscriber_type)
        if issubclass(subscriber_payload_type, payload_type):
            subscriber_types.append(subscriber_type)

    websocket_subscriber_type = WebsocketSubscriber[payload_type]
    subscriber_types.append(websocket_subscriber_type)

    if len(subscriber_types) > 1:
        subscriber_type = Annotated[
            Union[tuple(subscriber_types)], Field(discriminator="type_name")
        ]
    else:
        subscriber_type = subscriber_types[0]

    class SubscriptionResponse(BaseModel):
        id: UUID
        subscriber: subscriber_type  # type: ignore

    class SubscriptionPage(BaseModel):
        items: list[SubscriptionResponse]  # type: ignore
        next_page_id: str | None

    class ResultPage(BaseModel):
        items: list[EventResult]
        next_page_id: str | None

    payload_type_adapter = TypeAdapter(payload_type)
    SERIALIZERS[payload_type.__name__] = PydanticSerializer(
        TypeAdapter(QueueEvent[payload_type])
    )

    @fastapi.post("/event")
    async def publish(payload: payload_type) -> EventResponse:  # type: ignore
        event_queue: EventQueue[T] = await queue_manager.get_event_queue(payload_type)
        event = await event_queue.publish(payload)
        return event

    @fastapi.get("/event/search")
    async def search_events(
        page_id: str | None = None,
        limit: int = 100,
        created_at__gte: datetime | None = None,
        created_at__lte: datetime | None = None,
    ) -> EventPage:
        assert limit <= 100
        event_queue: EventQueue[T] = await queue_manager.get_event_queue(payload_type)
        page = await event_queue.search_events(
            page_id, limit, created_at__gte, created_at__lte
        )
        return page

    @fastapi.get("/event/count")
    async def count_events(
        created_at__gte: datetime | None = None,
        created_at__lte: datetime | None = None,
    ) -> int:
        event_queue: EventQueue[T] = await queue_manager.get_event_queue(payload_type)
        count = await event_queue.count_events(created_at__gte, created_at__lte)
        return count

    @fastapi.get("/event/{id}")
    async def get_event(id: int) -> EventResponse:
        event_queue: EventQueue[T] = await queue_manager.get_event_queue(payload_type)
        try:
            event = await event_queue.get_event(id)
            return event
        except Exception:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    @fastapi.get("/event")
    async def batch_get_events(ids: list[int]) -> list[EventResponse | None]:
        assert len(ids) <= 100
        event_queue: EventQueue[T] = await queue_manager.get_event_queue(payload_type)
        events = await event_queue.batch_get_events(ids)
        return events

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
    async def add_subscriber(subscriber: subscriber_type) -> SubscriptionResponse:  # type: ignore
        event_queue: EventQueue[T] = await queue_manager.get_event_queue(payload_type)
        subscription_id = await event_queue.subscribe(subscriber)
        return subscription_id

    @fastapi.get("/subscriber/{id}")
    async def get_subscriber(id: UUID) -> subscriber_type:  # type: ignore
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

    @fastapi.get("/result/search")
    async def search_results(
        page_id: str | None = None,
        limit: int = 100,
        event_id__eq: int | None = None,
        worker_id__eq: UUID | None = None,
        created_at__gte: datetime | None = None,
        created_at__lte: datetime | None = None,
    ) -> ResultPage:
        event_queue: EventQueue[T] = await queue_manager.get_event_queue(payload_type)
        try:
            return await event_queue.search_results(
                page_id=page_id,
                limit=limit,
                event_id__eq=event_id__eq,
                worker_id__eq=worker_id__eq,
                created_at__gte=created_at__gte,
                created_at__lte=created_at__lte,
            )
        except Exception:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    @fastapi.get("/result/{id}")
    async def get_result(id: UUID) -> EventResult:  # type: ignore
        event_queue: EventQueue[T] = await queue_manager.get_event_queue(payload_type)
        try:
            result = await event_queue.get_result(id)
            return SubscriptionResponse(id, result)
        except Exception:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)

    @fastapi.get("/result")
    async def batch_get_results(result_ids: list[int]) -> list[EventResult | None]:
        assert len(result_ids) <= 100
        event_queue: EventQueue[T] = await queue_manager.get_event_queue(payload_type)
        results = await event_queue.batch_get_results(result_ids)
        return results

    @fastapi.websocket("/socket")
    async def socket(
        websocket: WebSocket,
    ):
        await websocket.accept()
        event_queue: EventQueue[T] = await queue_manager.get_event_queue(payload_type)
        websocket_id = uuid4()
        subscriber = WebsocketSubscriber(
            type_name="WebsocketSubscriber",
            websocket_id=websocket_id,
            payload_type_name=payload_type.__name__,
        )
        WEBSOCKETS[websocket_id] = websocket
        subscription = await event_queue.subscribe(subscriber)
        try:
            while websocket.application_state == WebSocketState.CONNECTED:
                data = await websocket.receive_json()
                payload = payload_type_adapter.validate_python(data)
                await event_queue.publish(payload)
        except WebSocketDisconnect as e:
            _LOGGER.debug("websocket_closed")
        finally:
            await event_queue.unsubscribe(subscription.id)
            WEBSOCKETS.pop(websocket_id, None)
