from dataclasses import dataclass, field
from fastapi.websockets import WebSocket, WebSocketState
from typing import TypeVar
from uuid import UUID

from pydantic import TypeAdapter
from eventy.event_queue import EventQueue
from eventy.queue_event import QueueEvent
from eventy.serializers.pydantic_serializer import PydanticSerializer
from eventy.subscribers.subscriber import Subscriber


WEBSOCKETS: dict[UUID, WebSocket] = {}
"""Global collection of websockets - managed """
T = TypeVar("T")

def websocket_subscriber(payload_type: type[T]):

    serializer = PydanticSerializer(TypeAdapter(payload_type))

    @dataclass
    class WebsocketSubscriber(Subscriber[T]):
        """Subscriber sending data to a websocket"""

        websocket_id: UUID

        @staticmethod
        def get_payload_type():
            return payload_type

        async def on_event(
            self, event: QueueEvent[T], event_queue: EventQueue[T]
        ) -> None:
            """Send event to websocket if connected and matches worker ID"""
            # Only send to websocket if it matches the current worker
            if event_queue.worker_id != self.websocket_id:
                return

            websocket = WEBSOCKETS.get(self.websocket_id)
            if not websocket:
                return
            if websocket.application_state != WebSocketState.CONNECTED:
                return
            data = self.serializer.serialize(event)
            await websocket.send_text(data)

    WebsocketSubscriber.__name__ = f"{payload_type.__name__}WebsocketSubscriber"
    return WebsocketSubscriber
