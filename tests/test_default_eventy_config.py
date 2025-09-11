"""
Unit tests for DefaultEventyConfig.

This module tests the DefaultEventyConfig functionality, including:
- Initialization with payload types and subscriber types
- Default factory behavior for subscriber_types
- Getter methods for payload and subscriber types
- Inheritance from EventyConfig abstract base class
"""

import pytest
from dataclasses import FrozenInstanceError

from eventy.config.default_eventy_config import DefaultEventyConfig
from eventy.config.eventy_config import EventyConfig
from eventy.subscribers.subscriber import Subscriber
from eventy.queue_event import QueueEvent
from eventy.event_queue import EventQueue


class MockSubscriber(Subscriber[str]):
    """Mock subscriber for testing purposes"""

    async def on_event(self, event: QueueEvent[str], event_queue: EventQueue[str]) -> None:
        """Mock implementation of on_event"""
        pass


class AnotherMockSubscriber(Subscriber[int]):
    """Another mock subscriber for testing purposes"""

    async def on_event(self, event: QueueEvent[int], event_queue: EventQueue[int]) -> None:
        """Mock implementation of on_event"""
        pass


class TestDefaultEventyConfig:
    """Test cases for DefaultEventyConfig"""

    def test_initialization_with_payload_types_only(self):
        """Test initialization with only payload types"""
        payload_types = [str, int, dict]
        config = DefaultEventyConfig(payload_types=payload_types)
        
        assert config.payload_types == payload_types
        assert config.subscriber_types == []  # Should use default factory

    def test_initialization_with_payload_and_subscriber_types(self):
        """Test initialization with both payload and subscriber types"""
        payload_types = [str, int]
        subscriber_types = [MockSubscriber, AnotherMockSubscriber]
        
        config = DefaultEventyConfig(
            payload_types=payload_types,
            subscriber_types=subscriber_types
        )
        
        assert config.payload_types == payload_types
        assert config.subscriber_types == subscriber_types

    def test_get_payload_types(self):
        """Test get_payload_types method"""
        payload_types = [str, int, float, dict, list]
        config = DefaultEventyConfig(payload_types=payload_types)
        
        result = config.get_payload_types()
        assert result == payload_types
        assert result is config.payload_types  # Should return the same reference

    def test_get_subscriber_types_empty(self):
        """Test get_subscriber_types method with empty list"""
        config = DefaultEventyConfig(payload_types=[str])
        
        result = config.get_subscriber_types()
        assert result == []
        assert result is config.subscriber_types  # Should return the same reference

    def test_get_subscriber_types_with_subscribers(self):
        """Test get_subscriber_types method with subscriber types"""
        subscriber_types = [MockSubscriber, AnotherMockSubscriber]
        config = DefaultEventyConfig(
            payload_types=[str, int],
            subscriber_types=subscriber_types
        )
        
        result = config.get_subscriber_types()
        assert result == subscriber_types
        assert result is config.subscriber_types  # Should return the same reference

    def test_inheritance_from_eventy_config(self):
        """Test that DefaultEventyConfig properly inherits from EventyConfig"""
        config = DefaultEventyConfig(payload_types=[str])
        
        assert isinstance(config, EventyConfig)
        assert hasattr(config, 'get_payload_types')
        assert hasattr(config, 'get_subscriber_types')

    def test_default_factory_creates_new_list_instances(self):
        """Test that default factory creates separate list instances"""
        config1 = DefaultEventyConfig(payload_types=[str])
        config2 = DefaultEventyConfig(payload_types=[int])
        
        # Both should have empty lists but different instances
        assert config1.subscriber_types == []
        assert config2.subscriber_types == []
        assert config1.subscriber_types is not config2.subscriber_types

    def test_payload_types_required(self):
        """Test that payload_types is required"""
        with pytest.raises(TypeError):
            DefaultEventyConfig()  # Should fail without payload_types

    def test_subscriber_types_modification(self):
        """Test that subscriber_types can be modified after creation"""
        config = DefaultEventyConfig(payload_types=[str])
        assert config.subscriber_types == []
        
        # Modify the list
        config.subscriber_types.append(MockSubscriber)
        assert config.subscriber_types == [MockSubscriber]
        
        # Verify getter returns the modified list
        assert config.get_subscriber_types() == [MockSubscriber]

    def test_payload_types_modification(self):
        """Test that payload_types can be modified after creation"""
        config = DefaultEventyConfig(payload_types=[str])
        assert config.payload_types == [str]
        
        # Modify the list
        config.payload_types.append(int)
        assert config.payload_types == [str, int]
        
        # Verify getter returns the modified list
        assert config.get_payload_types() == [str, int]

    def test_empty_payload_types_list(self):
        """Test initialization with empty payload types list"""
        config = DefaultEventyConfig(payload_types=[])
        
        assert config.payload_types == []
        assert config.subscriber_types == []
        assert config.get_payload_types() == []
        assert config.get_subscriber_types() == []

    def test_complex_payload_types(self):
        """Test with complex payload types including custom classes"""
        class CustomPayload:
            pass
        
        payload_types = [str, int, dict, list, tuple, CustomPayload]
        config = DefaultEventyConfig(payload_types=payload_types)
        
        assert config.get_payload_types() == payload_types
        assert CustomPayload in config.get_payload_types()

    def test_dataclass_behavior(self):
        """Test that DefaultEventyConfig behaves as a proper dataclass"""
        config1 = DefaultEventyConfig(payload_types=[str], subscriber_types=[MockSubscriber])
        config2 = DefaultEventyConfig(payload_types=[str], subscriber_types=[MockSubscriber])
        
        # Test equality
        assert config1 == config2
        
        # Test with different values
        config3 = DefaultEventyConfig(payload_types=[int], subscriber_types=[MockSubscriber])
        assert config1 != config3

    def test_repr_and_str(self):
        """Test string representation of DefaultEventyConfig"""
        config = DefaultEventyConfig(
            payload_types=[str, int],
            subscriber_types=[MockSubscriber]
        )
        
        repr_str = repr(config)
        assert "DefaultEventyConfig" in repr_str
        assert "payload_types" in repr_str
        assert "subscriber_types" in repr_str