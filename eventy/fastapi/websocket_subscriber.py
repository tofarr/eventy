from dataclasses import dataclass, field
from fastapi import WebSocket, WebSocketState
from typing import TypeVar
from uuid import UUID
from eventy.queue_event import QueueEvent
from eventy.serializers.serializer import Serializer, get_default_serializer
from eventy.subscriber.subscriber import Subscriber


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

    async def on_event(self, event: QueueEvent[T]) -> None:
        websocket = WEBSOCKETS.get(self.websocket_id)
        if not websocket:
            return
        if websocket.application_state != WebSocketState.CONNECTED:
            return
        data = self.serializer.serialize(event)
        await self.websocket.send_text(data)

    async def on_worker_event(
        self, event: QueueEvent[T], current_worker_id: UUID, primary_worker_id: UUID
    ) -> None:
        if current_worker_id == self.websocket_id:
            await self.on_event(event)
