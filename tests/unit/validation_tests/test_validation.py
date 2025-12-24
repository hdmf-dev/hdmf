"""Unit tests for the hdmf.validation module."""

import unittest
from typing import Optional
import numpy as np

from hdmf.validation import ArrayData, ScalarData, AnyData, validated, get_validator_info
from hdmf.validation.types import register_type, get_registered_types, get_all_macros
from hdmf.validation.decorators import has_validation


class TestTypeAliases(unittest.TestCase):
    """Tests for type alias definitions."""

    def test_array_data_includes_numpy(self):
        """ArrayData should include numpy.ndarray."""
        # This tests that the type alias is defined correctly
        # We can't directly test Union types, but we can verify the registry
        types = get_registered_types("array_data")
        self.assertIn(np.ndarray, types)

    def test_array_data_includes_list(self):
        """ArrayData should include list."""
        types = get_registered_types("array_data")
        self.assertIn(list, types)

    def test_array_data_includes_tuple(self):
        """ArrayData should include tuple."""
        types = get_registered_types("array_data")
        self.assertIn(tuple, types)

    def test_scalar_data_types(self):
        """ScalarData should include str, int, float, bytes, bool."""
        types = get_registered_types("scalar_data")
        self.assertIn(str, types)
        self.assertIn(int, types)
        self.assertIn(float, types)
        self.assertIn(bytes, types)
        self.assertIn(bool, types)


class TestTypeRegistry(unittest.TestCase):
    """Tests for the type registry system."""

    def test_register_type(self):
        """Test registering a new type with a macro."""
        class CustomArrayType:
            pass
        
        register_type("array_data", CustomArrayType)
        types = get_registered_types("array_data")
        self.assertIn(CustomArrayType, types)

    def test_register_new_macro(self):
        """Test registering a type with a new macro name."""
        class MyType:
            pass
        
        register_type("my_custom_macro", MyType)
        types = get_registered_types("my_custom_macro")
        self.assertIn(MyType, types)

    def test_get_all_macros(self):
        """Test getting all registered macros."""
        macros = get_all_macros()
        self.assertIn("array_data", macros)
        self.assertIn("scalar_data", macros)

    def test_register_duplicate_type(self):
        """Registering the same type twice should not create duplicates."""
        initial_types = get_registered_types("array_data")
        initial_count = len(initial_types)
        
        # Register np.ndarray again (it's already registered)
        register_type("array_data", np.ndarray)
        
        types = get_registered_types("array_data")
        self.assertEqual(len(types), initial_count)


class TestValidatedDecorator(unittest.TestCase):
    """Tests for the @validated decorator."""

    def test_validated_basic(self):
        """Test basic @validated decorator usage."""
        @validated
        def my_func(name: str, value: int) -> str:
            return f"{name}: {value}"
        
        result = my_func(name="test", value=42)
        self.assertEqual(result, "test: 42")

    def test_validated_with_defaults(self):
        """Test @validated with default values."""
        @validated
        def my_func(name: str, value: int = 10) -> str:
            return f"{name}: {value}"
        
        result = my_func(name="test")
        self.assertEqual(result, "test: 10")

    def test_validated_with_options(self):
        """Test @validated with options."""
        @validated(enforce_type=True, enforce_shape=True)
        def my_func(name: str) -> str:
            return name
        
        result = my_func(name="test")
        self.assertEqual(result, "test")

    def test_keyword_only_with_star_syntax(self):
        """Test keyword-only using Python's native * syntax."""
        @validated
        def my_func(*, name: str) -> str:  # Use * for keyword-only
            return name
        
        # Keyword should work
        result = my_func(name="test")
        self.assertEqual(result, "test")
        
        # Positional should raise TypeError (Python's native behavior)
        with self.assertRaises(TypeError):
            my_func("test")

    def test_extra_kwargs_rejected_by_python(self):
        """Test that extra kwargs are rejected by Python naturally (no **kwargs)."""
        @validated
        def my_func(name: str) -> str:
            return name
        
        # Python naturally raises TypeError for unexpected kwargs
        with self.assertRaises(TypeError):
            my_func(name="test", extra="arg")

    def test_extra_kwargs_allowed_with_kwargs_signature(self):
        """Test that extra kwargs work when function has **kwargs in signature."""
        @validated
        def my_func(name: str, **kwargs) -> str:
            return f"{name}: {kwargs}"
        
        # Should not raise - **kwargs naturally accepts extra args
        result = my_func(name="test", extra="arg")
        self.assertEqual(result, "test: {'extra': 'arg'}")

    def test_validated_method(self):
        """Test @validated on an instance method."""
        class MyClass:
            @validated
            def my_method(self, name: str) -> str:
                return name
        
        obj = MyClass()
        result = obj.my_method(name="test")
        self.assertEqual(result, "test")

    def test_validated_optional_type(self):
        """Test @validated with Optional type."""
        @validated
        def my_func(name: str, value: Optional[int] = None) -> str:
            return f"{name}: {value}"
        
        result = my_func(name="test")
        self.assertEqual(result, "test: None")
        
        result = my_func(name="test", value=42)
        self.assertEqual(result, "test: 42")


class TestGetValidatorInfo(unittest.TestCase):
    """Tests for get_validator_info function."""

    def test_get_all_args(self):
        """Test getting all validation args."""
        @validated
        def my_func(name: str, value: int = 10) -> str:
            return f"{name}: {value}"
        
        info = get_validator_info(my_func)
        self.assertEqual(len(info), 2)
        
        names = [arg['name'] for arg in info]
        self.assertIn('name', names)
        self.assertIn('value', names)

    def test_get_specific_args(self):
        """Test getting specific validation args."""
        @validated
        def my_func(name: str, value: int = 10) -> str:
            return f"{name}: {value}"
        
        info = get_validator_info(my_func, 'name')
        self.assertEqual(len(info), 1)
        self.assertEqual(info[0]['name'], 'name')

    def test_get_multiple_specific_args(self):
        """Test getting multiple specific validation args."""
        @validated
        def my_func(name: str, value: int, extra: float = 1.0) -> str:
            return f"{name}: {value}"
        
        info = get_validator_info(my_func, 'name', 'extra')
        self.assertEqual(len(info), 2)
        names = [arg['name'] for arg in info]
        self.assertEqual(names, ['name', 'extra'])

    def test_get_nonexistent_arg(self):
        """Test getting a nonexistent arg raises ValueError."""
        @validated
        def my_func(name: str) -> str:
            return name
        
        with self.assertRaises(ValueError):
            get_validator_info(my_func, 'nonexistent')

    def test_get_info_no_validation(self):
        """Test getting info from function without validation."""
        def my_func(name: str) -> str:
            return name
        
        info = get_validator_info(my_func)
        self.assertEqual(info, tuple())


class TestHasValidation(unittest.TestCase):
    """Tests for has_validation function."""

    def test_has_validation_true(self):
        """Test has_validation returns True for @validated functions."""
        @validated
        def my_func(name: str) -> str:
            return name
        
        self.assertTrue(has_validation(my_func))

    def test_has_validation_false(self):
        """Test has_validation returns False for regular functions."""
        def my_func(name: str) -> str:
            return name
        
        self.assertFalse(has_validation(my_func))


class TestTypeEnforcement(unittest.TestCase):
    """Tests for beartype type enforcement when installed."""

    def test_type_error_on_wrong_type(self):
        """Test that wrong types raise TypeError when beartype is installed."""
        from hdmf.validation.decorators import BEARTYPE_INSTALLED
        
        @validated
        def my_func(name: str, value: int) -> str:
            return f"{name}: {value}"
        
        if BEARTYPE_INSTALLED:
            # With beartype, passing wrong type should raise TypeError
            with self.assertRaises(TypeError):
                my_func(name="test", value="not_an_int")
        else:
            # Without beartype, no type checking happens
            result = my_func(name="test", value="not_an_int")
            self.assertEqual(result, "test: not_an_int")

    def test_enforce_type_false_skips_validation(self):
        """Test that enforce_type=False skips type validation."""
        @validated(enforce_type=False)
        def my_func(name: str, value: int) -> str:
            return f"{name}: {value}"
        
        # Should not raise even with wrong type
        result = my_func(name="test", value="not_an_int")
        self.assertEqual(result, "test: not_an_int")


class TestMutableDefaults(unittest.TestCase):
    """Tests for mutable default handling using standard Python pattern."""

    def test_list_default_standard_pattern(self):
        """Test the standard Python pattern for mutable list defaults."""
        @validated
        def my_func(items: Optional[list] = None) -> list:
            if items is None:
                items = []
            items.append(1)
            return items
        
        # Each call should get a fresh list
        result1 = my_func()
        result2 = my_func()
        
        self.assertEqual(result1, [1])
        self.assertEqual(result2, [1])  # Fresh list each time

    def test_dict_default_standard_pattern(self):
        """Test the standard Python pattern for mutable dict defaults."""
        @validated
        def my_func(data: Optional[dict] = None) -> dict:
            if data is None:
                data = {}
            data['key'] = 'value'
            return data
        
        result1 = my_func()
        result2 = my_func()
        
        self.assertEqual(result1, {'key': 'value'})
        self.assertEqual(result2, {'key': 'value'})  # Fresh dict each time


if __name__ == '__main__':
    unittest.main()
