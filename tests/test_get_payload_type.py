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

    async def on_event(
        self, event: QueueEvent[str], event_queue: EventQueue[str]
    ) -> None:
        pass


class InheritedSubscriber(StringSubscriber):
    pass


class InheritedIntSubscriber(Subscriber[int]):
    """Simple subscriber with int payload"""

    async def on_event(
        self, event: QueueEvent[int], event_queue: EventQueue[int]
    ) -> None:
        pass


T = TypeVar("T")


class GenericSubscriber(Subscriber[T]):
    """Simple subscriber with generic payload"""

    async def on_event(self, event: QueueEvent[T], event_queue: EventQueue[T]) -> None:
        pass


class InheritedGenericSubscriber(GenericSubscriber[float]):
    async def on_event(self, event: QueueEvent[T], event_queue: EventQueue[T]) -> None:
        pass


class SecondLevelSubscriber(InheritedGenericSubscriber):
    pass


def test_get_payload_type():
    assert get_payload_type(Subscriber[int]) == int
    assert get_payload_type(StringSubscriber) == str
    assert get_payload_type(InheritedIntSubscriber) == int
    assert get_payload_type(InheritedSubscriber) == str
    assert get_payload_type(GenericSubscriber[bool]) == bool
    assert get_payload_type(InheritedGenericSubscriber) == float
    assert get_payload_type(SecondLevelSubscriber) == float
    with pytest.raises(TypeError):
        get_payload_type(int)
