"""
Serializers package for eventy.

This package contains serializer implementations for converting events
to and from serialized formats for storage and transmission.
"""

from .serializer import Serializer, get_default_serializer
from .pickle_serializer import PickleSerializer

__all__ = [
    'Serializer',
    'get_default_serializer', 
    'PickleSerializer',
]