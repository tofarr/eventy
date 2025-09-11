import json
from typing import Literal, TypeVar
from uuid import UUID

from fastapi.websockets import WebSocket, WebSocketState
from pydantic import BaseModel
from eventy.event_queue import EventQueue
from eventy.queue_event import QueueEvent
from eventy.serializers.serializer import Serializer
from eventy.subscribers.subscriber import Subscriber


WEBSOCKETS: dict[UUID, WebSocket] = {}
SERIALIZERS: dict[UUID, Serializer] = {}
"""Global collection of websockets - managed """
T = TypeVar("T")


class WebsocketSubscriber(BaseModel, Subscriber[T]):
    """Subscriber sending data to a websocket"""

    type_name: Literal["WebsocketSubscriber"]
    websocket_id: UUID
    payload_type_name: str

    async def on_event(self, event: QueueEvent[T], event_queue: EventQueue[T]) -> None:
        websocket = WEBSOCKETS.get(self.websocket_id)
        if not websocket:
            return
        if websocket.application_state != WebSocketState.CONNECTED:
            return
        serializer = SERIALIZERS.get(self.payload_type_name)
        if serializer.is_json:
            data = json.loads(serializer.serialize(event))
            await websocket.send_json(data)
        else:
            data = serializer.serialize(event)
            await websocket.send_bytes(data)
