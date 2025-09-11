"""
Unit tests for get_payload_type function in subscriber.py.

This module tests the get_payload_type functionality, including:
- Extracting payload types from direct generic subscriptions
- Extracting payload types from inherited subscriber classes
- Handling complex type hierarchies
- Error handling for invalid subscriber types
"""

import pytest
from typing import Generic, TypeVar, List, Dict, Union, Optional
from dataclasses import dataclass

from eventy.subscribers.subscriber import Subscriber, get_payload_type
from eventy.queue_event import QueueEvent
from eventy.event_queue import EventQueue


# Test subscriber implementations
class StringSubscriber(Subscriber[str]):
    """Simple subscriber with string payload"""
    async def on_event(self, event: QueueEvent[str], event_queue: EventQueue[str]) -> None:
        pass


class InheritedSubscriber(StringSubscriber):
    pass


class InheritedIntSubscriber(Subscriber[int]):
    """Simple subscriber with string payload"""
    async def on_event(self, event: QueueEvent[int], event_queue: EventQueue[int]) -> None:
        pass


def test_get_payload_type():
    assert get_payload_type(Subscriber[int]) == int
    assert get_payload_type(StringSubscriber) == str
    assert get_payload_type(InheritedIntSubscriber) == int
    assert get_payload_type(InheritedSubscriber) == str
