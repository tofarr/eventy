# Setup Python path for eventy module when running directly
import sys
import os
if __name__ == "__main__":
    eventy_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    sys.path.insert(0, eventy_root)

from typing import Dict, Any
from fastapi import FastAPI
from pydantic import BaseModel

from eventy.eventy_config import EventyConfig
from eventy.fastapi.endpoints import add_endpoints
from eventy.fastapi.websocket_subscriber import WebsocketSubscriber
from eventy.mem.memory_queue_manager import MemoryQueueManager
from eventy.subscriber.webhook_subscriber import WebhookSubscriber


class ExamplePayload(BaseModel):
    """Example payload type for demonstration"""
    message: str
    user_id: str
    timestamp: str


class SimpleEventyConfig(EventyConfig):
    """Simple configuration for eventy with example payload types"""
    
    def get_subscriber_types(self) -> list[type]:
        """Get available subscriber types"""
        # Return empty list for now to avoid the payload_type attribute issue
        return []
    
    def get_payload_types(self) -> list[type]:
        """Get available payload types"""
        return [ExamplePayload, Dict[str, Any]]


def create_app() -> FastAPI:
    """Create and configure the FastAPI application with eventy endpoints"""
    app = FastAPI(
        title="Eventy API",
        description="Event queue management system with FastAPI",
        version="0.1.0"
    )
    
    # Configure CORS and other settings for the runtime environment
    from fastapi.middleware.cors import CORSMiddleware
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Create the queue manager and configuration
    config = SimpleEventyConfig()
    queue_manager = MemoryQueueManager()
    
    # Store these for use in startup event
    app.state.queue_manager = queue_manager
    app.state.config = config
    
    @app.on_event("startup")
    async def startup_event():
        """Register payload types with the queue manager on startup"""
        for payload_type in config.get_payload_types():
            await queue_manager.register(payload_type)
    
    # Add eventy endpoints
    add_endpoints(app, queue_manager, config)
    
    @app.get("/")
    async def root():
        """Root endpoint with API information"""
        return {
            "message": "Eventy FastAPI Server",
            "version": "0.1.0",
            "endpoints": {
                "events": "/{payload_type}/event",
                "subscribers": "/{payload_type}/subscriber",
                "websocket": "/{payload_type}/socket"
            },
            "payload_types": [pt.__name__ for pt in config.get_payload_types()],
            "subscriber_types": [st.__name__ for st in config.get_subscriber_types()]
        }
    
    @app.get("/health")
    async def health_check():
        """Health check endpoint"""
        return {"status": "healthy"}
    
    return app


# Create the app instance
app = create_app()


if __name__ == "__main__":
    import uvicorn
    
    # Run the server with configuration suitable for the runtime environment
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=12000,
        reload=False,  # Disable reload to avoid path issues
        access_log=True
    )