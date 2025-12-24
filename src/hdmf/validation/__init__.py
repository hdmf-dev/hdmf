"""
HDMF Validation Module

This module provides modern type validation using beartype and numpydantic,
replacing the legacy docval system with Python type hints.

Type Aliases:
    ArrayData: Union type for array-like data (np.ndarray, list, tuple, h5py.Dataset, etc.)
    ScalarData: Union type for scalar data (str, int, float, bytes, bool)
    AnyData: General data type alias

Decorators:
    @validated: Runtime type validation decorator using beartype

Functions:
    get_validator_info: Get validation metadata from decorated functions
    has_validation: Check if a function has validation applied
    register_type: Register a new type with a macro name

Testing:
    compare_validation_behavior: Compare behavior between old/new validation
    compare_validation_metadata: Compare metadata between old/new validation
"""

from .types import ArrayData, ScalarData, AnyData, register_type, get_registered_types
from .decorators import validated, get_validator_info, has_validation
from .migrate import DocvalMigrator, DocvalParser

__all__ = [
    # Type aliases
    "ArrayData",
    "ScalarData", 
    "AnyData",
    # Decorators
    "validated",
    # Introspection
    "get_validator_info",
    "has_validation",
    # Type registry
    "register_type",
    "get_registered_types",
]
