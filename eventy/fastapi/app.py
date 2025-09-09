"""
FastAPI application with eventy queue manager integration.

This module provides a FastAPI application that:
1. Uses the default queue manager from eventy
2. Properly manages the queue manager lifecycle (enter/exit)
3. Provides a hello world endpoint
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI
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
    queue_manager = get_default_queue_manager()
    await queue_manager.__aenter__()
    
    try:
        yield
    finally:
        # Shutdown: Exit the queue manager
        if queue_manager:
            await queue_manager.__aexit__(None, None, None)


# Create FastAPI app with lifecycle management
app = FastAPI(
    title="Eventy FastAPI App",
    description="A FastAPI application integrated with eventy queue manager",
    version="0.1.0",
    lifespan=lifespan
)


@app.get("/")
async def hello_world():
    """
    Hello world endpoint.
    
    Returns:
        dict: A simple greeting message with queue manager status
    """
    return {
        "message": "Hello, World!",
        "status": "Queue manager is running",
        "queue_manager_type": queue_manager.__class__.__name__ if queue_manager else "None"
    }


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