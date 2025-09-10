from dataclasses import dataclass, field
from fastapi.websockets import WebSocket, WebSocketState
from typing import Literal, TypeVar
from uuid import UUID

from pydantic import BaseModel, TypeAdapter, ConfigDict
from eventy.event_queue import EventQueue
from eventy.queue_event import QueueEvent
from eventy.serializers.pydantic_serializer import PydanticSerializer
from eventy.serializers.serializer import Serializer
from eventy.subscribers.subscriber import Subscriber


WEBSOCKETS: dict[UUID, WebSocket] = {}
SERIALIZERS: dict[UUID, Serializer] = {}
"""Global collection of websockets - managed """
T = TypeVar("T")


class WebsocketSubscriber(BaseModel):
    """Subscriber sending data to a websocket"""
    model_config = ConfigDict(arbitrary_types_allowed=True)
    
    type_name: Literal["WebsocketSubscriber"]
    websocket_id: UUID
    payload_type_name: str

    async def on_event(
        self, event: QueueEvent, event_queue: EventQueue
    ) -> None:
        """Send event to websocket if connected and matches worker ID"""
        # Only send to websocket if it matches the current worker
        if event_queue.get_worker_id() != self.websocket_id:
            return

        websocket = WEBSOCKETS.get(self.websocket_id)
        if not websocket:
            return
        if websocket.application_state != WebSocketState.CONNECTED:
            return
        serializer = SERIALIZERS.get(self.payload_type_name)
        data = serializer.serialize(event)
        await websocket.send_text(data)


class GenericWebsocketSubscriber(Subscriber[T]):
    """Generic wrapper for WebsocketSubscriber that implements Subscriber[T]"""
    
    def __init__(self, websocket_subscriber: WebsocketSubscriber):
        self.websocket_subscriber = websocket_subscriber
    
    async def on_event(self, event: QueueEvent[T], event_queue: EventQueue[T]) -> None:
        await self.websocket_subscriber.on_event(event, event_queue)
