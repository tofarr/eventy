from typing import Literal
from pydantic import BaseModel

from eventy.config.default_eventy_config import DefaultEventyConfig
from eventy.config.eventy_config import set_config
from eventy.event_queue import EventQueue
from eventy.fastapi.__main__ import main as fastapi_main
from eventy.queue_event import QueueEvent
from eventy.subscribers.subscriber import Subscriber


class MyMsg(BaseModel):
    msg: str | None = None


class PrintSubscriber(Subscriber[MyMsg], BaseModel):
    type_name: Literal["PrintSubscriber"]

    async def on_event(
        self, event: QueueEvent[MyMsg], event_queue: EventQueue[MyMsg]
    ) -> None:
       print(f"📤 Received message (Event ID: {event.id}): {event.payload.msg}")


def main():
    set_config(DefaultEventyConfig(
        payload_types=[MyMsg],
        subscriber_types=[PrintSubscriber],
    ))

    # Run main
    fastapi_main()


if __name__ == "__main__":
    main()
