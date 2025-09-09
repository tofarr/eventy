from dataclasses import dataclass, field
from fastapi import WebSocket, WebSocketState
from typing import TypeVar, TYPE_CHECKING
from uuid import UUID
from eventy.queue_event import QueueEvent
from eventy.serializers.serializer import Serializer, get_default_serializer
from eventy.subscribers.subscriber import Subscriber

if TYPE_CHECKING:
    from eventy.event_queue import EventQueue


WEBSOCKETS: dict[UUID, WebSocket] = {}
"""Global collection of websockets - managed """
T = TypeVar("T")


@dataclass
class WebsocketSubscriber(Subscriber[T]):

    payload_type: type[T]
    websocket_id: UUID
    serializer: Serializer[QueueEvent[T]] = field(
        default_factory=get_default_serializer
    )

    async def on_event(
        self, event: QueueEvent[T], event_queue: "EventQueue[T]"
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
