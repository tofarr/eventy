"""
Comprehensive unit tests for the get_payload_type method across different EventQueue implementations.

This module provides extensive test coverage for the get_payload_type method, testing various
payload types, edge cases, and ensuring consistency across different EventQueue implementations.
"""

import unittest
from dataclasses import dataclass
from typing import Dict, List, Optional, Union, Any
from uuid import UUID
import tempfile
import os
from pathlib import Path

from eventy.mem.memory_event_queue import MemoryEventQueue
from eventy.fs.polling_file_event_queue import PollingFileEventQueue


# Test payload classes
@dataclass
class SimplePayload:
    """Simple dataclass for testing"""

    message: str
    value: int


@dataclass
class ComplexPayload:
    """Complex dataclass with nested types"""

    id: UUID
    data: Dict[str, Any]
    items: List[str]
    metadata: Optional[Dict[str, str]] = None


class CustomClass:
    """Custom class for testing"""

    def __init__(self, name: str):
        self.name = name

    def __eq__(self, other):
        return isinstance(other, CustomClass) and self.name == other.name


class TestGetPayloadType(unittest.IsolatedAsyncioTestCase):
    """Comprehensive tests for get_payload_type method"""

    async def test_get_payload_type_string(self):
        """Test get_payload_type with string payload type"""
        queue = MemoryEventQueue(payload_type=str)
        async with queue:
            payload_type = queue.get_payload_type()
            self.assertEqual(payload_type, str)
            self.assertIs(payload_type, str)

    async def test_get_payload_type_integer(self):
        """Test get_payload_type with integer payload type"""
        queue = MemoryEventQueue(payload_type=int)
        async with queue:
            payload_type = queue.get_payload_type()
            self.assertEqual(payload_type, int)
            self.assertIs(payload_type, int)

    async def test_get_payload_type_float(self):
        """Test get_payload_type with float payload type"""
        queue = MemoryEventQueue(payload_type=float)
        async with queue:
            payload_type = queue.get_payload_type()
            self.assertEqual(payload_type, float)
            self.assertIs(payload_type, float)

    async def test_get_payload_type_boolean(self):
        """Test get_payload_type with boolean payload type"""
        queue = MemoryEventQueue(payload_type=bool)
        async with queue:
            payload_type = queue.get_payload_type()
            self.assertEqual(payload_type, bool)
            self.assertIs(payload_type, bool)

    async def test_get_payload_type_dict(self):
        """Test get_payload_type with dict payload type"""
        queue = MemoryEventQueue(payload_type=dict)
        async with queue:
            payload_type = queue.get_payload_type()
            self.assertEqual(payload_type, dict)
            self.assertIs(payload_type, dict)

    async def test_get_payload_type_list(self):
        """Test get_payload_type with list payload type"""
        queue = MemoryEventQueue(payload_type=list)
        async with queue:
            payload_type = queue.get_payload_type()
            self.assertEqual(payload_type, list)
            self.assertIs(payload_type, list)

    async def test_get_payload_type_simple_dataclass(self):
        """Test get_payload_type with simple dataclass payload type"""
        queue = MemoryEventQueue(payload_type=SimplePayload)
        async with queue:
            payload_type = queue.get_payload_type()
            self.assertEqual(payload_type, SimplePayload)
            self.assertIs(payload_type, SimplePayload)

    async def test_get_payload_type_complex_dataclass(self):
        """Test get_payload_type with complex dataclass payload type"""
        queue = MemoryEventQueue(payload_type=ComplexPayload)
        async with queue:
            payload_type = queue.get_payload_type()
            self.assertEqual(payload_type, ComplexPayload)
            self.assertIs(payload_type, ComplexPayload)

    async def test_get_payload_type_custom_class(self):
        """Test get_payload_type with custom class payload type"""
        queue = MemoryEventQueue(payload_type=CustomClass)
        async with queue:
            payload_type = queue.get_payload_type()
            self.assertEqual(payload_type, CustomClass)
            self.assertIs(payload_type, CustomClass)

    async def test_get_payload_type_generic_dict(self):
        """Test get_payload_type with generic Dict type"""
        queue = MemoryEventQueue(payload_type=Dict[str, int])
        async with queue:
            payload_type = queue.get_payload_type()
            self.assertEqual(payload_type, Dict[str, int])

    async def test_get_payload_type_generic_list(self):
        """Test get_payload_type with generic List type"""
        queue = MemoryEventQueue(payload_type=List[str])
        async with queue:
            payload_type = queue.get_payload_type()
            self.assertEqual(payload_type, List[str])

    async def test_get_payload_type_optional_type(self):
        """Test get_payload_type with Optional type"""
        queue = MemoryEventQueue(payload_type=Optional[str])
        async with queue:
            payload_type = queue.get_payload_type()
            self.assertEqual(payload_type, Optional[str])

    async def test_get_payload_type_union_type(self):
        """Test get_payload_type with Union type"""
        queue = MemoryEventQueue(payload_type=Union[str, int])
        async with queue:
            payload_type = queue.get_payload_type()
            self.assertEqual(payload_type, Union[str, int])

    async def test_get_payload_type_any_type(self):
        """Test get_payload_type with Any type"""
        queue = MemoryEventQueue(payload_type=Any)
        async with queue:
            payload_type = queue.get_payload_type()
            self.assertEqual(payload_type, Any)
            self.assertIs(payload_type, Any)

    async def test_get_payload_type_consistency_across_calls(self):
        """Test that get_payload_type returns consistent results across multiple calls"""
        queue = MemoryEventQueue(payload_type=SimplePayload)
        async with queue:
            payload_type1 = queue.get_payload_type()
            payload_type2 = queue.get_payload_type()
            payload_type3 = queue.get_payload_type()

            self.assertEqual(payload_type1, payload_type2)
            self.assertEqual(payload_type2, payload_type3)
            self.assertIs(payload_type1, payload_type2)
            self.assertIs(payload_type2, payload_type3)

    async def test_get_payload_type_before_context_manager(self):
        """Test that get_payload_type works before entering context manager"""
        queue = MemoryEventQueue(payload_type=str)
        # Should work even before entering context manager
        payload_type = queue.get_payload_type()
        self.assertEqual(payload_type, str)
        self.assertIs(payload_type, str)

    async def test_get_payload_type_after_context_manager_exit(self):
        """Test that get_payload_type works after exiting context manager"""
        queue = MemoryEventQueue(payload_type=int)
        async with queue:
            pass  # Enter and exit context manager

        # Should still work after exiting
        payload_type = queue.get_payload_type()
        self.assertEqual(payload_type, int)
        self.assertIs(payload_type, int)

    async def test_get_payload_type_multiple_queue_instances(self):
        """Test that different queue instances return their respective payload types"""
        queue1 = MemoryEventQueue(payload_type=str)
        queue2 = MemoryEventQueue(payload_type=int)
        queue3 = MemoryEventQueue(payload_type=SimplePayload)

        async with queue1, queue2, queue3:
            payload_type1 = queue1.get_payload_type()
            payload_type2 = queue2.get_payload_type()
            payload_type3 = queue3.get_payload_type()

            self.assertEqual(payload_type1, str)
            self.assertEqual(payload_type2, int)
            self.assertEqual(payload_type3, SimplePayload)

            # Ensure they're different
            self.assertNotEqual(payload_type1, payload_type2)
            self.assertNotEqual(payload_type2, payload_type3)
            self.assertNotEqual(payload_type1, payload_type3)

    async def test_get_payload_type_with_nested_generic_types(self):
        """Test get_payload_type with nested generic types"""
        nested_type = Dict[str, List[int]]
        queue = MemoryEventQueue(payload_type=nested_type)
        async with queue:
            payload_type = queue.get_payload_type()
            self.assertEqual(payload_type, nested_type)

    async def test_get_payload_type_with_complex_nested_types(self):
        """Test get_payload_type with very complex nested types"""
        complex_type = Dict[str, Union[List[int], Optional[Dict[str, Any]]]]
        queue = MemoryEventQueue(payload_type=complex_type)
        async with queue:
            payload_type = queue.get_payload_type()
            self.assertEqual(payload_type, complex_type)

    async def test_get_payload_type_immutability(self):
        """Test that the returned payload type reference is immutable"""
        queue = MemoryEventQueue(payload_type=str)
        async with queue:
            payload_type1 = queue.get_payload_type()
            payload_type2 = queue.get_payload_type()

            # Should be the exact same object reference
            self.assertIs(payload_type1, payload_type2)

            # Verify it's the actual str type, not a copy
            self.assertIs(payload_type1, str)

    async def test_get_payload_type_with_inheritance(self):
        """Test get_payload_type with class inheritance"""

        class BasePayload:
            def __init__(self, base_field: str):
                self.base_field = base_field

        class DerivedPayload(BasePayload):
            def __init__(self, base_field: str, derived_field: int):
                super().__init__(base_field)
                self.derived_field = derived_field

        queue = MemoryEventQueue(payload_type=DerivedPayload)
        async with queue:
            payload_type = queue.get_payload_type()
            self.assertEqual(payload_type, DerivedPayload)
            self.assertIs(payload_type, DerivedPayload)

            # Verify it's the derived class, not the base class
            self.assertNotEqual(payload_type, BasePayload)


class TestGetPayloadTypeMultipleImplementations(unittest.IsolatedAsyncioTestCase):
    """Test get_payload_type method across different EventQueue implementations"""

    def setUp(self):
        """Set up temporary directory for file-based tests"""
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Clean up temporary directory"""
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    async def test_get_payload_type_memory_vs_file_queue_consistency(self):
        """Test that MemoryEventQueue and PollingFileEventQueue return consistent payload types"""
        payload_type = str

        memory_queue = MemoryEventQueue(payload_type=payload_type)
        file_queue = PollingFileEventQueue(
            payload_type=payload_type,
            root_dir=Path(self.temp_dir),
            polling_interval=0.1,
        )

        async with memory_queue, file_queue:
            memory_payload_type = memory_queue.get_payload_type()
            file_payload_type = file_queue.get_payload_type()

            self.assertEqual(memory_payload_type, file_payload_type)
            self.assertEqual(memory_payload_type, payload_type)
            self.assertEqual(file_payload_type, payload_type)
            self.assertIs(memory_payload_type, str)
            self.assertIs(file_payload_type, str)

    async def test_get_payload_type_different_implementations_different_types(self):
        """Test different implementations with different payload types"""
        memory_queue = MemoryEventQueue(payload_type=int)
        file_queue = PollingFileEventQueue(
            payload_type=SimplePayload,
            root_dir=Path(self.temp_dir),
            polling_interval=0.1,
        )

        async with memory_queue, file_queue:
            memory_payload_type = memory_queue.get_payload_type()
            file_payload_type = file_queue.get_payload_type()

            self.assertEqual(memory_payload_type, int)
            self.assertEqual(file_payload_type, SimplePayload)
            self.assertNotEqual(memory_payload_type, file_payload_type)

    async def test_get_payload_type_complex_types_across_implementations(self):
        """Test complex payload types work consistently across implementations"""
        complex_type = Dict[str, List[int]]

        memory_queue = MemoryEventQueue(payload_type=complex_type)
        file_queue = PollingFileEventQueue(
            payload_type=complex_type,
            root_dir=Path(self.temp_dir),
            polling_interval=0.1,
        )

        async with memory_queue, file_queue:
            memory_payload_type = memory_queue.get_payload_type()
            file_payload_type = file_queue.get_payload_type()

            self.assertEqual(memory_payload_type, complex_type)
            self.assertEqual(file_payload_type, complex_type)
            self.assertEqual(memory_payload_type, file_payload_type)

    async def test_get_payload_type_custom_class_across_implementations(self):
        """Test custom class payload types work consistently across implementations"""
        memory_queue = MemoryEventQueue(payload_type=CustomClass)
        file_queue = PollingFileEventQueue(
            payload_type=CustomClass, root_dir=Path(self.temp_dir), polling_interval=0.1
        )

        async with memory_queue, file_queue:
            memory_payload_type = memory_queue.get_payload_type()
            file_payload_type = file_queue.get_payload_type()

            self.assertEqual(memory_payload_type, CustomClass)
            self.assertEqual(file_payload_type, CustomClass)
            self.assertEqual(memory_payload_type, file_payload_type)
            self.assertIs(memory_payload_type, CustomClass)
            self.assertIs(file_payload_type, CustomClass)

    async def test_get_payload_type_multiple_instances_same_implementation(self):
        """Test multiple instances of the same implementation with different payload types"""
        queue1 = MemoryEventQueue(payload_type=str)
        queue2 = MemoryEventQueue(payload_type=int)
        queue3 = MemoryEventQueue(payload_type=SimplePayload)

        # Test before entering context managers
        self.assertEqual(queue1.get_payload_type(), str)
        self.assertEqual(queue2.get_payload_type(), int)
        self.assertEqual(queue3.get_payload_type(), SimplePayload)

        async with queue1, queue2, queue3:
            # Test within context managers
            self.assertEqual(queue1.get_payload_type(), str)
            self.assertEqual(queue2.get_payload_type(), int)
            self.assertEqual(queue3.get_payload_type(), SimplePayload)

            # Ensure they're all different
            payload_types = [
                queue1.get_payload_type(),
                queue2.get_payload_type(),
                queue3.get_payload_type(),
            ]

            # All should be different
            self.assertEqual(len(set(payload_types)), 3)

    async def test_get_payload_type_implementation_independence(self):
        """Test that payload type is independent of the implementation used"""
        test_cases = [
            str,
            int,
            dict,
            SimplePayload,
            Dict[str, int],
            List[str],
            Optional[int],
        ]

        for payload_type in test_cases:
            with self.subTest(payload_type=payload_type):
                memory_queue = MemoryEventQueue(payload_type=payload_type)
                file_queue = PollingFileEventQueue(
                    payload_type=payload_type,
                    root_dir=Path(self.temp_dir),
                    polling_interval=0.1,
                )

                async with memory_queue, file_queue:
                    memory_result = memory_queue.get_payload_type()
                    file_result = file_queue.get_payload_type()

                    # Both should return the same type
                    self.assertEqual(memory_result, payload_type)
                    self.assertEqual(file_result, payload_type)
                    self.assertEqual(memory_result, file_result)


if __name__ == "__main__":
    unittest.main()
