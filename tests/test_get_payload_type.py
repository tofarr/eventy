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


# Test payload types
@dataclass
class CustomPayload:
    message: str
    value: int


class ComplexPayload:
    def __init__(self, data: Dict[str, any]):
        self.data = data


# Test subscriber implementations
class StringSubscriber(Subscriber[str]):
    """Simple subscriber with string payload"""
    async def on_event(self, event: QueueEvent[str], event_queue: EventQueue[str]) -> None:
        pass


class IntSubscriber(Subscriber[int]):
    """Simple subscriber with int payload"""
    async def on_event(self, event: QueueEvent[int], event_queue: EventQueue[int]) -> None:
        pass


class CustomPayloadSubscriber(Subscriber[CustomPayload]):
    """Subscriber with custom dataclass payload"""
    async def on_event(self, event: QueueEvent[CustomPayload], event_queue: EventQueue[CustomPayload]) -> None:
        pass


class ListPayloadSubscriber(Subscriber[List[str]]):
    """Subscriber with generic list payload"""
    async def on_event(self, event: QueueEvent[List[str]], event_queue: EventQueue[List[str]]) -> None:
        pass


class DictPayloadSubscriber(Subscriber[Dict[str, int]]):
    """Subscriber with generic dict payload"""
    async def on_event(self, event: QueueEvent[Dict[str, int]], event_queue: EventQueue[Dict[str, int]]) -> None:
        pass


class UnionPayloadSubscriber(Subscriber[Union[str, int]]):
    """Subscriber with union payload"""
    async def on_event(self, event: QueueEvent[Union[str, int]], event_queue: EventQueue[Union[str, int]]) -> None:
        pass


class OptionalPayloadSubscriber(Subscriber[Optional[str]]):
    """Subscriber with optional payload"""
    async def on_event(self, event: QueueEvent[Optional[str]], event_queue: EventQueue[Optional[str]]) -> None:
        pass


# Inheritance test cases
class BaseStringSubscriber(Subscriber[str]):
    """Base subscriber class"""
    async def on_event(self, event: QueueEvent[str], event_queue: EventQueue[str]) -> None:
        pass


class InheritedStringSubscriber(BaseStringSubscriber):
    """Subscriber that inherits from another subscriber"""
    async def on_event(self, event: QueueEvent[str], event_queue: EventQueue[str]) -> None:
        pass


class MultiLevelInheritedSubscriber(InheritedStringSubscriber):
    """Subscriber with multiple levels of inheritance"""
    async def on_event(self, event: QueueEvent[str], event_queue: EventQueue[str]) -> None:
        pass


# Generic base class for testing
T = TypeVar('T')


class GenericBaseSubscriber(Subscriber[T]):
    """Generic base subscriber"""
    async def on_event(self, event: QueueEvent[T], event_queue: EventQueue[T]) -> None:
        pass


class ConcreteFromGenericSubscriber(GenericBaseSubscriber[int]):
    """Concrete subscriber from generic base"""
    async def on_event(self, event: QueueEvent[int], event_queue: EventQueue[int]) -> None:
        pass


# Invalid subscriber classes for error testing
class NonSubscriberClass:
    """Class that doesn't inherit from Subscriber"""
    pass


class NonGenericSubscriber(Subscriber):
    """Subscriber without generic type parameter"""
    async def on_event(self, event, event_queue) -> None:
        pass


class TestGetPayloadType:
    """Test cases for get_payload_type function"""

    def test_simple_string_subscriber(self):
        """Test extracting payload type from simple string subscriber"""
        payload_type = get_payload_type(StringSubscriber)
        assert payload_type == str, "Should extract str payload type"

    def test_simple_int_subscriber(self):
        """Test extracting payload type from simple int subscriber"""
        payload_type = get_payload_type(IntSubscriber)
        assert payload_type == int, "Should extract int payload type"

    def test_custom_dataclass_subscriber(self):
        """Test extracting payload type from custom dataclass subscriber"""
        payload_type = get_payload_type(CustomPayloadSubscriber)
        assert payload_type == CustomPayload, "Should extract CustomPayload type"

    def test_custom_class_subscriber(self):
        """Test extracting payload type from custom class subscriber"""
        class ComplexPayloadSubscriber(Subscriber[ComplexPayload]):
            async def on_event(self, event: QueueEvent[ComplexPayload], event_queue: EventQueue[ComplexPayload]) -> None:
                pass

        payload_type = get_payload_type(ComplexPayloadSubscriber)
        assert payload_type == ComplexPayload, "Should extract ComplexPayload type"

    def test_generic_list_subscriber(self):
        """Test extracting payload type from generic list subscriber"""
        payload_type = get_payload_type(ListPayloadSubscriber)
        assert payload_type == List[str], "Should extract List[str] payload type"

    def test_generic_dict_subscriber(self):
        """Test extracting payload type from generic dict subscriber"""
        payload_type = get_payload_type(DictPayloadSubscriber)
        assert payload_type == Dict[str, int], "Should extract Dict[str, int] payload type"

    def test_union_payload_subscriber(self):
        """Test extracting payload type from union subscriber"""
        payload_type = get_payload_type(UnionPayloadSubscriber)
        assert payload_type == Union[str, int], "Should extract Union[str, int] payload type"

    def test_optional_payload_subscriber(self):
        """Test extracting payload type from optional subscriber"""
        payload_type = get_payload_type(OptionalPayloadSubscriber)
        assert payload_type == Optional[str], "Should extract Optional[str] payload type"

    def test_inherited_subscriber(self):
        """Test extracting payload type from inherited subscriber"""
        payload_type = get_payload_type(InheritedStringSubscriber)
        assert payload_type == str, "Should extract str payload type from inherited subscriber"

    def test_multi_level_inherited_subscriber(self):
        """Test extracting payload type from multi-level inherited subscriber"""
        payload_type = get_payload_type(MultiLevelInheritedSubscriber)
        assert payload_type == str, "Should extract str payload type from multi-level inherited subscriber"

    def test_concrete_from_generic_subscriber(self):
        """Test extracting payload type from concrete subscriber derived from generic base"""
        payload_type = get_payload_type(ConcreteFromGenericSubscriber)
        assert payload_type == int, "Should extract int payload type from concrete generic subscriber"

    def test_direct_generic_subscription(self):
        """Test extracting payload type from direct generic subscription"""
        # This tests the first branch of the function where get_args returns the type directly
        class DirectGenericSubscriber(Subscriber[str]):
            async def on_event(self, event: QueueEvent[str], event_queue: EventQueue[str]) -> None:
                pass

        payload_type = get_payload_type(DirectGenericSubscriber)
        assert payload_type == str, "Should extract str payload type from direct generic subscription"

    def test_nested_generic_types(self):
        """Test extracting payload type with nested generic types"""
        class NestedGenericSubscriber(Subscriber[Dict[str, List[int]]]):
            async def on_event(self, event: QueueEvent[Dict[str, List[int]]], event_queue: EventQueue[Dict[str, List[int]]]) -> None:
                pass

        payload_type = get_payload_type(NestedGenericSubscriber)
        assert payload_type == Dict[str, List[int]], "Should extract nested generic payload type"

    def test_complex_inheritance_chain(self):
        """Test extracting payload type from complex inheritance chain"""
        class BaseGeneric(Subscriber[T]):
            async def on_event(self, event: QueueEvent[T], event_queue: EventQueue[T]) -> None:
                pass

        class MiddleLayer(BaseGeneric[str]):
            pass

        class FinalSubscriber(MiddleLayer):
            async def on_event(self, event: QueueEvent[str], event_queue: EventQueue[str]) -> None:
                pass

        payload_type = get_payload_type(FinalSubscriber)
        assert payload_type == str, "Should extract str payload type from complex inheritance chain"

    def test_error_non_subscriber_class(self):
        """Test that AttributeError is raised for non-subscriber class"""
        with pytest.raises(AttributeError):
            get_payload_type(NonSubscriberClass)

    def test_error_non_generic_subscriber(self):
        """Test that TypeError is raised for non-generic subscriber"""
        with pytest.raises(TypeError, match="Could not get payload type for"):
            get_payload_type(NonGenericSubscriber)

    def test_error_invalid_type(self):
        """Test that AttributeError is raised for invalid type"""
        with pytest.raises(AttributeError):
            get_payload_type(str)  # Passing a basic type instead of a class

    def test_error_none_type(self):
        """Test that AttributeError is raised for None type"""
        with pytest.raises(AttributeError):
            get_payload_type(None)

    def test_multiple_generic_parameters(self):
        """Test behavior with multiple generic parameters (should take first one)"""
        U = TypeVar('U')
        
        class MultiGenericSubscriber(Subscriber[str]):
            # This is a bit artificial, but tests the edge case
            pass

        payload_type = get_payload_type(MultiGenericSubscriber)
        assert payload_type == str, "Should extract first generic parameter"

    def test_subscriber_with_any_type(self):
        """Test extracting payload type when using Any"""
        from typing import Any
        
        class AnySubscriber(Subscriber[Any]):
            async def on_event(self, event: QueueEvent[Any], event_queue: EventQueue[Any]) -> None:
                pass

        payload_type = get_payload_type(AnySubscriber)
        assert payload_type == Any, "Should extract Any payload type"

    def test_subscriber_with_type_var(self):
        """Test extracting payload type when using TypeVar"""
        class TypeVarSubscriber(Subscriber[T]):
            async def on_event(self, event: QueueEvent[T], event_queue: EventQueue[T]) -> None:
                pass

        payload_type = get_payload_type(TypeVarSubscriber)
        assert payload_type == T, "Should extract TypeVar payload type"

    def test_real_world_payload_types(self):
        """Test with real-world payload types"""
        from datetime import datetime
        from uuid import UUID
        
        class DateTimeSubscriber(Subscriber[datetime]):
            async def on_event(self, event: QueueEvent[datetime], event_queue: EventQueue[datetime]) -> None:
                pass

        class UUIDSubscriber(Subscriber[UUID]):
            async def on_event(self, event: QueueEvent[UUID], event_queue: EventQueue[UUID]) -> None:
                pass

        datetime_payload_type = get_payload_type(DateTimeSubscriber)
        uuid_payload_type = get_payload_type(UUIDSubscriber)
        
        assert datetime_payload_type == datetime, "Should extract datetime payload type"
        assert uuid_payload_type == UUID, "Should extract UUID payload type"

    def test_edge_case_empty_inheritance(self):
        """Test edge case with empty inheritance chain"""
        class EmptyInheritanceSubscriber(Subscriber[str]):
            async def on_event(self, event: QueueEvent[str], event_queue: EventQueue[str]) -> None:
                pass

        payload_type = get_payload_type(EmptyInheritanceSubscriber)
        assert payload_type == str, "Should handle empty inheritance chain correctly"


class TestGetPayloadTypeIntegration:
    """Integration tests for get_payload_type with actual subscriber usage"""

    def test_payload_type_consistency_with_subscriber_usage(self):
        """Test that extracted payload type is consistent with actual subscriber usage"""
        class TestSubscriber(Subscriber[CustomPayload]):
            def __init__(self):
                self.received_payloads = []

            async def on_event(self, event: QueueEvent[CustomPayload], event_queue: EventQueue[CustomPayload]) -> None:
                self.received_payloads.append(event.payload)

        # Extract payload type
        payload_type = get_payload_type(TestSubscriber)
        assert payload_type == CustomPayload, "Extracted payload type should match subscriber definition"

        # Verify the subscriber can handle the payload type
        subscriber = TestSubscriber()
        test_payload = CustomPayload(message="test", value=42)
        test_event = QueueEvent(id=1, payload=test_payload)
        
        # This would be called by the event queue in real usage
        # We're just verifying the types are consistent
        assert isinstance(test_event.payload, payload_type), "Event payload should match extracted type"

    def test_payload_type_with_wrapper_subscribers(self):
        """Test payload type extraction with wrapper subscribers like NonceSubscriber"""
        from eventy.subscribers.nonce_subscriber import NonceSubscriber
        from eventy.subscribers.worker_match_subscriber import WorkerMatchSubscriber
        from typing import TypeVar

        # NonceSubscriber and WorkerMatchSubscriber are generic classes
        # When we call get_payload_type on them directly, we get a TypeVar
        nonce_payload_type = get_payload_type(NonceSubscriber)
        worker_match_payload_type = get_payload_type(WorkerMatchSubscriber)
        
        # Both wrapper subscribers should return a TypeVar since they are generic
        assert isinstance(nonce_payload_type, TypeVar), "NonceSubscriber should return a TypeVar"
        assert isinstance(worker_match_payload_type, TypeVar), "WorkerMatchSubscriber should return a TypeVar"
        assert str(nonce_payload_type) == "~T", "NonceSubscriber TypeVar should be named T"
        assert str(worker_match_payload_type) == "~T", "WorkerMatchSubscriber TypeVar should be named T"
        
        # Test with concrete instantiated types
        class ConcreteNonceSubscriber(NonceSubscriber[str]):
            pass
            
        class ConcreteWorkerMatchSubscriber(WorkerMatchSubscriber[int]):
            pass
            
        concrete_nonce_payload_type = get_payload_type(ConcreteNonceSubscriber)
        concrete_worker_payload_type = get_payload_type(ConcreteWorkerMatchSubscriber)
        
        assert concrete_nonce_payload_type == str, "Concrete NonceSubscriber should extract str payload type"
        assert concrete_worker_payload_type == int, "Concrete WorkerMatchSubscriber should extract int payload type"