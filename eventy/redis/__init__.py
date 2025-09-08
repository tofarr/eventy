"""Redis-based implementations for eventy queue management.

This module provides Redis-backed implementations of EventQueue and QueueManager
that support distributed processing with pub/sub notifications and automatic
cleanup using expiring keys.

Classes:
    RedisEventQueue: Redis-based event queue implementation
    RedisQueueManager: Redis-based queue manager implementation

Requirements:
    - redis>=5.0.0 (install with: pip install eventy[redis])
"""

try:
    from .redis_event_queue import RedisEventQueue
    from .redis_queue_manager import RedisQueueManager
    
    __all__ = ['RedisEventQueue', 'RedisQueueManager']
except ImportError:
    # Redis not installed
    __all__ = []