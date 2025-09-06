"""
Eventy - Event queue system with pluggable implementations.

This package provides abstract interfaces for event queues and concrete
implementations including in-memory queues with serialization support.
"""

# Core interfaces
from eventy.event_queue import EventQueue
from eventy.queue_event import QueueEvent
from eventy.subscriber import Subscriber
from eventy.page import Page

# Serializers
from eventy.serializers import Serializer, PickleSerializer, get_default_serializer

# Memory implementation
from eventy.mem import MemoryEventQueue, MemoryEventQueueManager

__all__ = [
    # Core interfaces
    'EventQueue',
    'QueueEvent', 
    'Subscriber',
    'Page',
    
    # Serializers
    'Serializer',
    'PickleSerializer',
    'get_default_serializer',
    
    # Memory implementation
    'MemoryEventQueue',
    'MemoryEventQueueManager',
]