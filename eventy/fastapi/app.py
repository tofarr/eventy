"""
FastAPI application with eventy queue manager integration.

This module provides a FastAPI application that:
1. Uses the default queue manager from eventy
2. Properly manages the queue manager lifecycle (enter/exit)
3. Provides a hello world endpoint
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
from eventy.config.eventy_config import get_config
from eventy.fastapi.endpoints import add_endpoints
from eventy.queue_manager import get_default_queue_manager


# Global queue manager instance
queue_manager = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage the application lifecycle.
    
    This context manager handles the startup and shutdown of the queue manager,
    ensuring proper resource management.
    """
    global queue_manager
    
    # Startup: Initialize and enter the queue manager
    queue_manager = await get_default_queue_manager()
    async with queue_manager:
        config = get_config()
        await add_endpoints(app, queue_manager, config)
        yield


# Create FastAPI app with lifecycle management
app = FastAPI(
    title="Eventy FastAPI App",
    description="A FastAPI application integrated with eventy queue manager",
    version="0.1.0",
    lifespan=lifespan
)


@app.get("/health")
async def health_check():
    """
    Health check endpoint.
    
    Returns:
        dict: Application health status
    """
    return {
        "status": "healthy",
        "queue_manager_active": queue_manager is not None
    }


