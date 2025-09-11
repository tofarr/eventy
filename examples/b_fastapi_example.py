"""
FastAPI WebSocket Example for Eventy Queue System

This example demonstrates how to integrate the eventy queue system with FastAPI
to create a real-time WebSocket-based messaging application. The example:

1. Sets up a FastAPI application with WebSocket support for real-time communication
2. Creates a custom MyMsg dataclass for message payloads
3. Implements a PrintSubscriber that logs received messages to the console
4. Serves a static HTML page with WebSocket client functionality
5. Allows users to send messages through a web interface and see them processed in real-time
6. Uses a random port between 8000-9000 to avoid conflicts

The web interface provides a simple form where users can type messages that are
sent via WebSocket to the eventy queue system, processed by subscribers, and
logged to the server console.
"""

from typing import Literal
from pydantic import BaseModel
import os
import random
import sys

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

    # Generate a random port between 8000 and 9000
    random_port = random.randint(8000, 9000)

    # Add the port argument to sys.argv if not already specified
    if "--port" not in sys.argv:
        sys.argv.extend(["--port", str(random_port)])

    print(f"🎲 Using random port: {random_port}")

    # Run main with this module's app
    fastapi_main("b_fastapi_example:app")


if __name__ == "__main__":
    main()
