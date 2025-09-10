from dataclasses import dataclass, field
from fastapi.websockets import WebSocket, WebSocketState
from typing import TypeVar
from uuid import UUID

from pydantic import TypeAdapter
from eventy.event_queue import EventQueue
from eventy.queue_event import QueueEvent
from eventy.serializers.pydantic_serializer import PydanticSerializer
from eventy.serializers.serializer import Serializer
from eventy.subscribers.subscriber import Subscriber


WEBSOCKETS: dict[UUID, WebSocket] = {}
SERIALIZERS: dict[UUID, Serializer] = {}
"""Global collection of websockets - managed """
T = TypeVar("T")


@dataclass
class WebsocketSubscriber(Subscriber[T]):
    """Subscriber sending data to a websocket"""

    websocket_id: UUID

    async def on_event(
        self, event: QueueEvent[T], event_queue: EventQueue[T]
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
        serializer = SERIALIZERS.get(self.websocket_id)
        data = serializer.serialize(event)
        await websocket.send_text(data)
