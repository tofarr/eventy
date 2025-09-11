"""
Unit tests for EventyConfig get_config and set_config functions.

This module tests the global configuration management functionality.
"""

from unittest.mock import patch

from eventy.config.eventy_config import get_config, set_config, EventyConfig, _config
from eventy.config.default_eventy_config import DefaultEventyConfig


class MockEventyConfig(EventyConfig):
    """Mock EventyConfig for testing that doesn't require arguments"""
    
    def get_subscriber_types(self):
        return []
    
    def get_payload_types(self):
        return [str]


class TestEventyConfigFunctions:
    """Test cases for get_config and set_config functions"""

    def setup_method(self):
        """Reset global config before each test"""
        global _config
        _config = None

    def teardown_method(self):
        """Clean up global config after each test"""
        global _config
        _config = None

    @patch.dict('os.environ', {'EVENTY_CONFIG': 'tests.test_eventy_config.MockEventyConfig'})
    def test_get_config_returns_mock_config(self):
        """Test that get_config returns MockEventyConfig when environment variable is set"""
        config = get_config()
        
        assert isinstance(config, MockEventyConfig)
        assert isinstance(config, EventyConfig)
        assert config.get_payload_types() == [str]
        
        # Test that subsequent calls return the same instance (singleton behavior)
        config2 = get_config()
        assert config is config2

    def test_set_config_and_get_config(self):
        """Test setting and getting custom config"""
        custom_config = DefaultEventyConfig(payload_types=[str, int])
        
        set_config(custom_config)
        retrieved_config = get_config()
        
        assert retrieved_config is custom_config
        assert len(retrieved_config.get_payload_types()) == 2
        assert str in retrieved_config.get_payload_types()
        assert int in retrieved_config.get_payload_types()
        assert retrieved_config.get_subscriber_types() == []