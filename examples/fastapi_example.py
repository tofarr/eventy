from typing import Literal
from pydantic import BaseModel
import os

from eventy.config.default_eventy_config import DefaultEventyConfig
from eventy.config.eventy_config import set_config
from eventy.event_queue import EventQueue
from eventy.fastapi.__main__ import main as fastapi_main
from eventy.fastapi.app import app
from eventy.queue_event import QueueEvent
from eventy.subscribers.subscriber import Subscriber
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse


class MyMsg(BaseModel):
    msg: str | None = None


class PrintSubscriber(BaseModel, Subscriber[MyMsg]):
    type_name: Literal["PrintSubscriber"]

    async def on_event(
        self, event: QueueEvent[MyMsg], event_queue: EventQueue[MyMsg]
    ) -> None:
        print(f"📤 Received message (Event ID: {event.id}): {event.payload.msg}")


@app.get("/")
async def serve_index():
    """
    Serve the main HTML page for the WebSocket demo.

    Returns:
        FileResponse: The HTML page
    """
    # Get the directory where this file is located
    current_dir = os.path.dirname(os.path.abspath(__file__))
    # Navigate to the static directory
    static_dir = os.path.join(current_dir, "static")
    html_file = os.path.join(static_dir, "index.html")

    if os.path.exists(html_file):
        return FileResponse(html_file)
    else:
        return {"error": "HTML file not found", "path": html_file}


# Mount static files directory if it exists
current_dir = os.path.dirname(os.path.abspath(__file__))
static_dir = os.path.join(current_dir, "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")


def main():
    set_config(
        DefaultEventyConfig(
            payload_types=[MyMsg],
            subscriber_types=[PrintSubscriber],
        )
    )

    # Run main with this module's app
    fastapi_main("fastapi_example:app")


if __name__ == "__main__":
    main()
