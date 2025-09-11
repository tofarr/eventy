"""
Unit tests for PydanticSerializer.

This module tests the PydanticSerializer functionality, including:
- Serialization of objects to JSON bytes
- Deserialization of JSON bytes back to objects
- Type validation and error handling
- Integration with Pydantic TypeAdapter
- Inheritance from Serializer abstract base class
"""

import pytest
from dataclasses import dataclass
from typing import List, Dict, Optional
from pydantic import BaseModel, TypeAdapter, ValidationError

from eventy.serializers.pydantic_serializer import PydanticSerializer
from eventy.serializers.serializer import Serializer


class SimpleModel(BaseModel):
    """Simple Pydantic model for testing"""
    name: str
    age: int
    active: bool = True


@dataclass
class SimpleDataclass:
    """Simple dataclass for testing"""
    name: str
    value: int


class ComplexModel(BaseModel):
    """Complex Pydantic model for testing"""
    id: int
    title: str
    tags: List[str]
    metadata: Dict[str, str]
    optional_field: Optional[str] = None


class TestPydanticSerializer:
    """Test cases for PydanticSerializer"""

    def test_initialization_with_type_adapter(self):
        """Test initialization with TypeAdapter"""
        type_adapter = TypeAdapter(SimpleModel)
        serializer = PydanticSerializer(type_adapter=type_adapter)
        
        assert serializer.type_adapter is type_adapter
        assert serializer.is_json is True  # Default value

    def test_initialization_with_is_json_false(self):
        """Test initialization with is_json set to False"""
        type_adapter = TypeAdapter(SimpleModel)
        serializer = PydanticSerializer(type_adapter=type_adapter, is_json=False)
        
        assert serializer.type_adapter is type_adapter
        assert serializer.is_json is False

    def test_inheritance_from_serializer(self):
        """Test that PydanticSerializer properly inherits from Serializer"""
        type_adapter = TypeAdapter(SimpleModel)
        serializer = PydanticSerializer(type_adapter=type_adapter)
        
        assert isinstance(serializer, Serializer)
        assert hasattr(serializer, 'serialize')
        assert hasattr(serializer, 'deserialize')
        assert hasattr(serializer, 'is_json')

    def test_serialize_simple_model(self):
        """Test serialization of a simple Pydantic model"""
        type_adapter = TypeAdapter(SimpleModel)
        serializer = PydanticSerializer(type_adapter=type_adapter)
        
        model = SimpleModel(name="John", age=30, active=True)
        result = serializer.serialize(model)
        
        assert isinstance(result, bytes)
        # Verify it's valid JSON by checking it contains expected fields
        result_str = result.decode('utf-8')
        assert '"name":"John"' in result_str
        assert '"age":30' in result_str
        assert '"active":true' in result_str

    def test_deserialize_simple_model(self):
        """Test deserialization of a simple Pydantic model"""
        type_adapter = TypeAdapter(SimpleModel)
        serializer = PydanticSerializer(type_adapter=type_adapter)
        
        # Create JSON bytes
        json_data = b'{"name":"Jane","age":25,"active":false}'
        result = serializer.deserialize(json_data)
        
        assert isinstance(result, SimpleModel)
        assert result.name == "Jane"
        assert result.age == 25
        assert result.active is False

    def test_serialize_deserialize_roundtrip(self):
        """Test that serialize/deserialize is a proper roundtrip"""
        type_adapter = TypeAdapter(SimpleModel)
        serializer = PydanticSerializer(type_adapter=type_adapter)
        
        original = SimpleModel(name="Alice", age=35, active=True)
        serialized = serializer.serialize(original)
        deserialized = serializer.deserialize(serialized)
        
        assert deserialized == original
        assert deserialized.name == original.name
        assert deserialized.age == original.age
        assert deserialized.active == original.active

    def test_serialize_complex_model(self):
        """Test serialization of a complex Pydantic model"""
        type_adapter = TypeAdapter(ComplexModel)
        serializer = PydanticSerializer(type_adapter=type_adapter)
        
        model = ComplexModel(
            id=123,
            title="Test Title",
            tags=["tag1", "tag2", "tag3"],
            metadata={"key1": "value1", "key2": "value2"},
            optional_field="optional_value"
        )
        result = serializer.serialize(model)
        
        assert isinstance(result, bytes)
        result_str = result.decode('utf-8')
        assert '"id":123' in result_str
        assert '"title":"Test Title"' in result_str
        assert '"tags":["tag1","tag2","tag3"]' in result_str
        assert '"optional_field":"optional_value"' in result_str

    def test_deserialize_complex_model(self):
        """Test deserialization of a complex Pydantic model"""
        type_adapter = TypeAdapter(ComplexModel)
        serializer = PydanticSerializer(type_adapter=type_adapter)
        
        json_data = b'{"id":456,"title":"Another Title","tags":["a","b"],"metadata":{"x":"y"}}'
        result = serializer.deserialize(json_data)
        
        assert isinstance(result, ComplexModel)
        assert result.id == 456
        assert result.title == "Another Title"
        assert result.tags == ["a", "b"]
        assert result.metadata == {"x": "y"}
        assert result.optional_field is None  # Default value

    def test_serialize_with_dataclass(self):
        """Test serialization with a dataclass using TypeAdapter"""
        type_adapter = TypeAdapter(SimpleDataclass)
        serializer = PydanticSerializer(type_adapter=type_adapter)
        
        obj = SimpleDataclass(name="Test", value=42)
        result = serializer.serialize(obj)
        
        assert isinstance(result, bytes)
        result_str = result.decode('utf-8')
        assert '"name":"Test"' in result_str
        assert '"value":42' in result_str

    def test_deserialize_with_dataclass(self):
        """Test deserialization with a dataclass using TypeAdapter"""
        type_adapter = TypeAdapter(SimpleDataclass)
        serializer = PydanticSerializer(type_adapter=type_adapter)
        
        json_data = b'{"name":"Deserialized","value":99}'
        result = serializer.deserialize(json_data)
        
        assert isinstance(result, SimpleDataclass)
        assert result.name == "Deserialized"
        assert result.value == 99

    def test_serialize_primitive_types(self):
        """Test serialization of primitive types"""
        # Test with string
        type_adapter = TypeAdapter(str)
        serializer = PydanticSerializer(type_adapter=type_adapter)
        
        result = serializer.serialize("hello world")
        assert isinstance(result, bytes)
        assert result == b'"hello world"'
        
        # Test with integer
        type_adapter = TypeAdapter(int)
        serializer = PydanticSerializer(type_adapter=type_adapter)
        
        result = serializer.serialize(42)
        assert isinstance(result, bytes)
        assert result == b'42'

    def test_deserialize_primitive_types(self):
        """Test deserialization of primitive types"""
        # Test with string
        type_adapter = TypeAdapter(str)
        serializer = PydanticSerializer(type_adapter=type_adapter)
        
        result = serializer.deserialize(b'"test string"')
        assert result == "test string"
        
        # Test with integer
        type_adapter = TypeAdapter(int)
        serializer = PydanticSerializer(type_adapter=type_adapter)
        
        result = serializer.deserialize(b'123')
        assert result == 123

    def test_deserialize_invalid_json_raises_error(self):
        """Test that invalid JSON raises appropriate error"""
        type_adapter = TypeAdapter(SimpleModel)
        serializer = PydanticSerializer(type_adapter=type_adapter)
        
        with pytest.raises(ValidationError):
            serializer.deserialize(b'invalid json')

    def test_deserialize_wrong_structure_raises_error(self):
        """Test that JSON with wrong structure raises ValidationError"""
        type_adapter = TypeAdapter(SimpleModel)
        serializer = PydanticSerializer(type_adapter=type_adapter)
        
        # Missing required fields
        with pytest.raises(ValidationError):
            serializer.deserialize(b'{"name":"John"}')  # Missing age
        
        # Wrong types
        with pytest.raises(ValidationError):
            serializer.deserialize(b'{"name":"John","age":"not_a_number","active":true}')

    def test_serialize_list_type(self):
        """Test serialization of list types"""
        type_adapter = TypeAdapter(List[str])
        serializer = PydanticSerializer(type_adapter=type_adapter)
        
        data = ["item1", "item2", "item3"]
        result = serializer.serialize(data)
        
        assert isinstance(result, bytes)
        assert result == b'["item1","item2","item3"]'

    def test_deserialize_list_type(self):
        """Test deserialization of list types"""
        type_adapter = TypeAdapter(List[int])
        serializer = PydanticSerializer(type_adapter=type_adapter)
        
        result = serializer.deserialize(b'[1,2,3,4,5]')
        assert result == [1, 2, 3, 4, 5]

    def test_dataclass_behavior(self):
        """Test that PydanticSerializer behaves as a proper dataclass"""
        type_adapter1 = TypeAdapter(str)
        type_adapter2 = TypeAdapter(str)
        
        serializer1 = PydanticSerializer(type_adapter=type_adapter1, is_json=True)
        serializer2 = PydanticSerializer(type_adapter=type_adapter2, is_json=True)
        
        # Note: TypeAdapter instances are not equal even with same type,
        # so we test the structure rather than equality
        assert serializer1.is_json == serializer2.is_json
        assert type(serializer1.type_adapter) == type(serializer2.type_adapter)