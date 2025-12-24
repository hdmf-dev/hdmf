"""
Type aliases for HDMF validation.

This module defines type aliases that replace the docval macro system,
providing IDE-friendly type hints that work with modern Python tooling.
"""

from typing import TypeAlias, Union, Any

import numpy as np
import h5py

# Try to import optional dependencies
try:
    from zarr import Array as ZarrArray
    ZARR_INSTALLED = True
except ImportError:
    ZarrArray = None  # type: ignore
    ZARR_INSTALLED = False


# -----------------------------------------------------------------------------
# Core Type Aliases
# -----------------------------------------------------------------------------

# ArrayData: Matches the docval 'array_data' macro
# This includes all types that can represent array-like data
if ZARR_INSTALLED:
    ArrayData: TypeAlias = Union[
        np.ndarray,
        list,
        tuple,
        h5py.Dataset,
        ZarrArray,
    ]
else:
    ArrayData: TypeAlias = Union[
        np.ndarray,
        list,
        tuple,
        h5py.Dataset,
    ]

# ScalarData: Matches the docval 'scalar_data' macro
ScalarData: TypeAlias = Union[str, int, float, bytes, bool]

# AnyData: General data type (combines array and scalar)
# This matches the docval 'data' macro behavior
AnyData: TypeAlias = Union[ArrayData, ScalarData]


# -----------------------------------------------------------------------------
# Numeric Type Aliases
# -----------------------------------------------------------------------------

# Integer types
Int8: TypeAlias = np.int8
Int16: TypeAlias = Union[np.int16, np.int32, np.int64, int]
Int32: TypeAlias = Union[int, np.int32, np.int64]
Int64: TypeAlias = np.int64

# Unsigned integer types
UInt8: TypeAlias = Union[np.uint8, np.uint16, np.uint32, np.uint64]
UInt16: TypeAlias = Union[np.uint16, np.uint32, np.uint64]
UInt32: TypeAlias = Union[np.uint32, np.uint64]
UInt64: TypeAlias = np.uint64

# Float types
Float32: TypeAlias = Union[float, np.float32, np.float64]
Float64: TypeAlias = Union[float, np.float64]

# General numeric type
Numeric: TypeAlias = Union[
    float, np.float32, np.float64,
    np.int8, np.int16, np.int32, np.int64, int,
    np.uint8, np.uint16, np.uint32, np.uint64,
]

# Boolean type
Bool: TypeAlias = Union[bool, np.bool_]

# Text types
Text: TypeAlias = str
Bytes: TypeAlias = bytes


# -----------------------------------------------------------------------------
# Registry for dynamic type extension (replaces docval_macro decorator)
# -----------------------------------------------------------------------------

_type_registry: dict[str, list[type]] = {
    "array_data": [np.ndarray, list, tuple, h5py.Dataset],
    "scalar_data": [str, int, float, bytes, bool],
    "data": [],
}

if ZARR_INSTALLED:
    _type_registry["array_data"].append(ZarrArray)


def register_type(macro_name: str, type_cls: type) -> None:
    """Register a type with a macro name for dynamic type extension.
    
    This replaces the @docval_macro decorator functionality.
    
    Args:
        macro_name: The name of the macro (e.g., 'array_data')
        type_cls: The type class to add to the macro
    
    Example:
        >>> from hdmf.validation.types import register_type
        >>> register_type('array_data', MyCustomArrayType)
    """
    if macro_name not in _type_registry:
        _type_registry[macro_name] = []
    if type_cls not in _type_registry[macro_name]:
        _type_registry[macro_name].append(type_cls)


def get_registered_types(macro_name: str) -> tuple[type, ...]:
    """Get the types registered with a macro name.
    
    Args:
        macro_name: The name of the macro
        
    Returns:
        Tuple of registered types
    """
    return tuple(_type_registry.get(macro_name, []))


def get_all_macros() -> dict[str, tuple[type, ...]]:
    """Get all registered macros and their types.
    
    Returns:
        Dictionary mapping macro names to tuples of types
    """
    return {k: tuple(v) for k, v in _type_registry.items()}
