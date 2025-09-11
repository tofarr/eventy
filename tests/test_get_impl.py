"""
Comprehensive unit tests for the get_impl method in eventy.util.

This test suite covers all aspects of the get_impl method including:
- Basic functionality with environment variables and defaults
- Type validation and inheritance checking
- Error conditions and edge cases
- Integration with actual eventy classes
"""

import unittest
import os
from unittest.mock import patch, MagicMock
from abc import ABC, abstractmethod
from typing import Any

from eventy.util import get_impl
from eventy.event_queue import EventQueue
from eventy.mem.memory_event_queue import MemoryEventQueue
from eventy.serializers.serializer import Serializer
from eventy.serializers.pickle_serializer import PickleSerializer


# Test classes for inheritance testing
class BaseTestClass:
    """Base class for testing inheritance"""

    pass


class ValidSubclass(BaseTestClass):
    """Valid subclass for testing"""

    pass


class InvalidClass:
    """Class that doesn't inherit from BaseTestClass"""

    pass


class AbstractTestClass(ABC):
    """Abstract base class for testing"""

    @abstractmethod
    def test_method(self):
        pass


class ConcreteTestClass(AbstractTestClass):
    """Concrete implementation of abstract class"""

    def test_method(self):
        return "implemented"


class TestGetImpl(unittest.TestCase):
    """Test cases for the get_impl method"""

    def setUp(self):
        """Set up test environment"""
        # Store original environment variables to restore later
        self.original_env = {}
        self.test_env_var = "TEST_GET_IMPL_VAR"

        # Clean up any existing test environment variable
        if self.test_env_var in os.environ:
            self.original_env[self.test_env_var] = os.environ[self.test_env_var]
            del os.environ[self.test_env_var]

    def tearDown(self):
        """Clean up test environment"""
        # Remove test environment variable if it exists
        if self.test_env_var in os.environ:
            del os.environ[self.test_env_var]

        # Restore original environment variables
        for key, value in self.original_env.items():
            os.environ[key] = value

    def test_get_impl_with_valid_env_var(self):
        """Test get_impl with valid environment variable"""
        # Set environment variable to a valid class
        os.environ[self.test_env_var] = "tests.test_get_impl.ValidSubclass"

        result = get_impl(self.test_env_var, BaseTestClass)

        self.assertEqual(result, ValidSubclass)
        self.assertTrue(issubclass(result, BaseTestClass))

    def test_get_impl_with_builtin_class(self):
        """Test get_impl with built-in Python class"""
        os.environ[self.test_env_var] = "builtins.str"

        result = get_impl(self.test_env_var, object)

        self.assertEqual(result, str)
        self.assertTrue(issubclass(result, object))

    def test_get_impl_with_standard_library_class(self):
        """Test get_impl with standard library class"""
        os.environ[self.test_env_var] = "collections.OrderedDict"

        result = get_impl(self.test_env_var, dict)

        from collections import OrderedDict

        self.assertEqual(result, OrderedDict)
        self.assertTrue(issubclass(result, dict))

    def test_get_impl_fallback_to_default(self):
        """Test get_impl falls back to default when no env var is set"""
        # Ensure environment variable is not set
        if self.test_env_var in os.environ:
            del os.environ[self.test_env_var]

        result = get_impl(self.test_env_var, BaseTestClass, ValidSubclass)

        self.assertEqual(result, ValidSubclass)

    def test_get_impl_no_env_var_no_default_raises_error(self):
        """Test get_impl raises ValueError when no env var and no default"""
        # Ensure environment variable is not set
        if self.test_env_var in os.environ:
            del os.environ[self.test_env_var]

        with self.assertRaises(ValueError) as context:
            get_impl(self.test_env_var, BaseTestClass)

        self.assertEqual(str(context.exception), "no_default_type")

    def test_get_impl_empty_env_var_uses_default(self):
        """Test get_impl uses default when env var is empty string"""
        os.environ[self.test_env_var] = ""

        result = get_impl(self.test_env_var, BaseTestClass, ValidSubclass)

        self.assertEqual(result, ValidSubclass)

    def test_get_impl_whitespace_env_var_raises_error(self):
        """Test get_impl raises error when env var is whitespace (not treated as empty)"""
        os.environ[self.test_env_var] = "   "

        # Whitespace is not treated as empty, so it tries to import "   " which fails
        with self.assertRaises(ValueError):
            get_impl(self.test_env_var, BaseTestClass, ValidSubclass)

    def test_get_impl_invalid_module_raises_error(self):
        """Test get_impl raises error for invalid module name"""
        os.environ[self.test_env_var] = "nonexistent.module.Class"

        with self.assertRaises(ModuleNotFoundError):
            get_impl(self.test_env_var, BaseTestClass)

    def test_get_impl_invalid_class_raises_error(self):
        """Test get_impl raises error for invalid class name"""
        os.environ[self.test_env_var] = "tests.test_get_impl.NonexistentClass"

        with self.assertRaises(AttributeError):
            get_impl(self.test_env_var, BaseTestClass)

    def test_get_impl_wrong_base_type_raises_error(self):
        """Test get_impl raises AssertionError for wrong base type"""
        os.environ[self.test_env_var] = "tests.test_get_impl.InvalidClass"

        with self.assertRaises(AssertionError):
            get_impl(self.test_env_var, BaseTestClass)

    def test_get_impl_default_wrong_base_type_raises_error(self):
        """Test get_impl raises AssertionError when default is wrong type"""
        with self.assertRaises(AssertionError):
            get_impl(self.test_env_var, BaseTestClass, InvalidClass)

    def test_get_impl_with_abstract_base_class(self):
        """Test get_impl with abstract base class"""
        os.environ[self.test_env_var] = "tests.test_get_impl.ConcreteTestClass"

        result = get_impl(self.test_env_var, AbstractTestClass)

        self.assertEqual(result, ConcreteTestClass)
        self.assertTrue(issubclass(result, AbstractTestClass))

    def test_get_impl_with_collections_module(self):
        """Test get_impl with collections module classes"""
        os.environ[self.test_env_var] = "collections.defaultdict"

        result = get_impl(self.test_env_var, dict)

        from collections import defaultdict

        self.assertEqual(result, defaultdict)
        self.assertTrue(issubclass(result, dict))

    def test_get_impl_malformed_class_path(self):
        """Test get_impl with malformed class path"""
        os.environ[self.test_env_var] = "just.a.module.without.class."

        # This should raise ModuleNotFoundError because "just" module doesn't exist
        with self.assertRaises(ModuleNotFoundError):
            get_impl(self.test_env_var, BaseTestClass)

    def test_get_impl_single_name_raises_error(self):
        """Test get_impl with single name (no module) raises error"""
        os.environ[self.test_env_var] = "ValidSubclass"

        with self.assertRaises(ValueError):
            get_impl(self.test_env_var, BaseTestClass)

    def test_get_impl_consistency_across_calls(self):
        """Test get_impl returns same class across multiple calls"""
        os.environ[self.test_env_var] = "tests.test_get_impl.ValidSubclass"

        result1 = get_impl(self.test_env_var, BaseTestClass)
        result2 = get_impl(self.test_env_var, BaseTestClass)

        self.assertEqual(result1, result2)
        self.assertIs(result1, result2)  # Should be the exact same class object


class TestGetImplIntegration(unittest.TestCase):
    """Integration tests using actual eventy classes"""

    def setUp(self):
        """Set up test environment"""
        self.original_env = {}
        self.test_vars = [
            "TEST_EVENTY_QUEUE_MANAGER",
            "TEST_EVENTY_SERIALIZER",
            "TEST_EVENTY_CONFIG",
        ]

        # Clean up any existing test environment variables
        for var in self.test_vars:
            if var in os.environ:
                self.original_env[var] = os.environ[var]
                del os.environ[var]

    def tearDown(self):
        """Clean up test environment"""
        # Remove test environment variables
        for var in self.test_vars:
            if var in os.environ:
                del os.environ[var]

        # Restore original environment variables
        for key, value in self.original_env.items():
            os.environ[key] = value

    def test_get_impl_with_memory_event_queue(self):
        """Test get_impl with MemoryEventQueue"""
        os.environ["TEST_EVENTY_QUEUE"] = (
            "eventy.mem.memory_event_queue.MemoryEventQueue"
        )

        result = get_impl("TEST_EVENTY_QUEUE", EventQueue)

        # Note: MemoryEventQueue is generic, so we need to check the origin
        from typing import get_origin

        self.assertEqual(get_origin(result) or result, MemoryEventQueue)

    def test_get_impl_with_pickle_serializer(self):
        """Test get_impl with PickleSerializer"""
        os.environ["TEST_EVENTY_SERIALIZER"] = (
            "eventy.serializers.pickle_serializer.PickleSerializer"
        )

        result = get_impl("TEST_EVENTY_SERIALIZER", Serializer)

        # PickleSerializer is also generic
        from typing import get_origin

        self.assertEqual(get_origin(result) or result, PickleSerializer)

    def test_get_impl_serializer_with_default(self):
        """Test get_impl with serializer using default"""
        result = get_impl("NONEXISTENT_SERIALIZER", Serializer, PickleSerializer)

        from typing import get_origin

        self.assertEqual(get_origin(result) or result, PickleSerializer)

    def test_get_impl_with_actual_eventy_constants(self):
        """Test get_impl with actual eventy environment variable names"""
        from eventy.constants import EVENTY_SERIALIZER

        # Test that we can use the actual constant
        os.environ[EVENTY_SERIALIZER] = (
            "eventy.serializers.pickle_serializer.PickleSerializer"
        )

        result = get_impl(EVENTY_SERIALIZER, Serializer)

        from typing import get_origin

        self.assertEqual(get_origin(result) or result, PickleSerializer)

    def test_get_impl_type_validation_with_eventy_classes(self):
        """Test type validation works with eventy class hierarchy"""
        # This should work - MemoryEventQueue is a subclass of EventQueue
        os.environ["TEST_QUEUE"] = "eventy.mem.memory_event_queue.MemoryEventQueue"
        result = get_impl("TEST_QUEUE", EventQueue)
        from typing import get_origin

        self.assertEqual(get_origin(result) or result, MemoryEventQueue)

        # This should fail - PickleSerializer is not a subclass of EventQueue
        os.environ["TEST_QUEUE_INVALID"] = (
            "eventy.serializers.pickle_serializer.PickleSerializer"
        )
        with self.assertRaises(AssertionError):
            get_impl("TEST_QUEUE_INVALID", EventQueue)


class TestGetImplEdgeCases(unittest.TestCase):
    """Edge case tests for get_impl method"""

    def setUp(self):
        """Set up test environment"""
        self.test_env_var = "TEST_EDGE_CASE_VAR"
        if self.test_env_var in os.environ:
            del os.environ[self.test_env_var]

    def tearDown(self):
        """Clean up test environment"""
        if self.test_env_var in os.environ:
            del os.environ[self.test_env_var]

    def test_get_impl_with_none_default(self):
        """Test get_impl with explicit None default"""
        with self.assertRaises(ValueError) as context:
            get_impl(self.test_env_var, BaseTestClass, None)

        self.assertEqual(str(context.exception), "no_default_type")

    def test_get_impl_with_object_base_type(self):
        """Test get_impl with object as base type (everything inherits from object)"""
        os.environ[self.test_env_var] = "tests.test_get_impl.ValidSubclass"

        result = get_impl(self.test_env_var, object)

        self.assertEqual(result, ValidSubclass)
        self.assertTrue(issubclass(result, object))

    def test_get_impl_with_same_class_as_base_and_default(self):
        """Test get_impl when default class is same as base class"""
        result = get_impl(self.test_env_var, BaseTestClass, BaseTestClass)

        self.assertEqual(result, BaseTestClass)

    def test_get_impl_multiple_inheritance(self):
        """Test get_impl with multiple inheritance using collections.OrderedDict"""
        # OrderedDict inherits from both dict and MutableMapping
        os.environ[self.test_env_var] = "collections.OrderedDict"

        result = get_impl(self.test_env_var, dict)

        from collections import OrderedDict
        from collections.abc import MutableMapping

        self.assertEqual(result, OrderedDict)
        self.assertTrue(issubclass(result, dict))
        self.assertTrue(issubclass(result, MutableMapping))

    @patch("eventy.util.import_from")
    def test_get_impl_import_from_called_correctly(self, mock_import_from):
        """Test that import_from is called with correct parameters"""
        mock_import_from.return_value = ValidSubclass

        os.environ[self.test_env_var] = "some.module.SomeClass"

        result = get_impl(self.test_env_var, BaseTestClass)

        mock_import_from.assert_called_once_with("some.module.SomeClass")
        self.assertEqual(result, ValidSubclass)

    @patch("eventy.util.import_from")
    def test_get_impl_import_from_exception_propagated(self, mock_import_from):
        """Test that exceptions from import_from are propagated"""
        mock_import_from.side_effect = ImportError("Test import error")

        os.environ[self.test_env_var] = "some.module.SomeClass"

        with self.assertRaises(ImportError) as context:
            get_impl(self.test_env_var, BaseTestClass)

        self.assertEqual(str(context.exception), "Test import error")


if __name__ == "__main__":
    unittest.main()
