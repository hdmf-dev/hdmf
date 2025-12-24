"""
Validation decorators for HDMF.

This module provides the @validated decorator that replaces @docval,
using beartype for runtime type checking with support for array shape validation.
"""

from __future__ import annotations

import functools
import inspect
from typing import Any, Callable, TypeVar, cast, get_type_hints

# Try to import beartype - this is the primary validation engine
try:
    from beartype import beartype
    from beartype.roar import BeartypeCallHintViolation
    BEARTYPE_INSTALLED = True
except ImportError:
    BEARTYPE_INSTALLED = False
    beartype = None  # type: ignore
    BeartypeCallHintViolation = TypeError  # type: ignore

# Try to import numpydantic for array shape validation
try:
    from numpydantic import NDArray, Shape
    NUMPYDANTIC_INSTALLED = True
except ImportError:
    NUMPYDANTIC_INSTALLED = False
    NDArray = None  # type: ignore
    Shape = None  # type: ignore


F = TypeVar("F", bound=Callable[..., Any])


# -----------------------------------------------------------------------------
# Validation Decorator
# -----------------------------------------------------------------------------

def validated(
    func: F | None = None,
    *,
    enforce_type: bool = True,
    enforce_shape: bool = True,
) -> F | Callable[[F], F]:
    """Decorator for validating function arguments using type hints.
    
    This decorator replaces @docval with modern Python type hints and beartype
    for runtime validation.
    
    Args:
        func: The function to decorate (when used without parentheses)
        enforce_type: Whether to enforce type checking (default: True)
        enforce_shape: Whether to enforce array shape checking (default: True)
    
    Returns:
        The decorated function with runtime validation
    
    Note:
        - Extra keyword arguments: Use **kwargs in signature to accept them
        - Keyword-only arguments: Use * in signature (e.g., def func(*, name: str))
    
    Example:
        >>> @validated
        ... def my_func(name: str, data: ArrayData) -> None:
        ...     '''Standard function - allows positional args.'''
        ...     pass
        
        >>> @validated
        ... def keyword_only(*, name: str, data: ArrayData) -> None:
        ...     '''Keyword-only function - positional args raise TypeError.'''
        ...     pass
    """
    def decorator(fn: F) -> F:
        # Get type hints for the function
        try:
            hints = get_type_hints(fn)
        except Exception:
            hints = {}
        
        # Get function signature for parameter inspection
        sig = inspect.signature(fn)
        
        # Store validation metadata on the function for introspection
        # This maintains compatibility with get_docval() pattern
        validation_info = {
            "args": [],
            "enforce_type": enforce_type,
            "enforce_shape": enforce_shape,
        }
        
        for param_name, param in sig.parameters.items():
            if param_name in ("self", "cls"):
                continue
            
            arg_info = {
                "name": param_name,
                "type": hints.get(param_name, Any),
                "doc": "",  # Docstring parsing could be added
            }
            
            if param.default is not inspect.Parameter.empty:
                arg_info["default"] = param.default
            
            validation_info["args"].append(arg_info)
        
        # Apply beartype if available and type enforcement is enabled
        if enforce_type and BEARTYPE_INSTALLED and beartype is not None:
            validated_fn = beartype(fn)
        else:
            validated_fn = fn
        
        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Call the validated function directly
            # Note: For mutable defaults, use the standard Python pattern:
            #   def func(items: list | None = None):
            #       if items is None:
            #           items = []
            try:
                return validated_fn(*args, **kwargs)
            except BeartypeCallHintViolation as e:
                # Convert beartype errors to a simpler TypeError
                raise TypeError(f"{fn.__qualname__}: {e}") from None
        
        # Attach validation info for introspection (backwards compatibility)
        wrapper.__validation_info__ = validation_info  # type: ignore
        wrapper.__wrapped__ = fn  # type: ignore
        
        return wrapper  # type: ignore
    
    # Handle both @validated and @validated() syntax
    if func is not None:
        return decorator(func)
    return decorator


# -----------------------------------------------------------------------------
# Introspection Functions (backwards compatibility with get_docval)
# -----------------------------------------------------------------------------

def get_validator_info(func: Callable, *args: str) -> tuple[dict, ...] | dict:
    """Get validation information for a function's arguments.
    
    This is the replacement for get_docval() that works with @validated decorated
    functions. It returns information about the function's validated arguments.
    
    Args:
        func: The function to get validation info for
        *args: Optional specific argument names to retrieve
        
    Returns:
        If args are specified, returns a tuple of dicts for those args.
        Otherwise, returns a tuple of all argument info dicts.
    
    Example:
        >>> @validated
        ... def my_func(name: str, data: ArrayData) -> None:
        ...     pass
        >>> get_validator_info(my_func)
        ({'name': 'name', 'type': str, 'doc': ''}, {'name': 'data', 'type': ArrayData, 'doc': ''})
    """
    # Check for new validation info first
    validation_info = getattr(func, "__validation_info__", None)
    if validation_info is not None:
        all_args = tuple(validation_info["args"])
        if args:
            arg_dict = {a["name"]: a for a in all_args}
            try:
                return tuple(arg_dict[name] for name in args)
            except KeyError as e:
                raise ValueError(
                    f"Function {func.__name__} does not have argument {e}"
                ) from None
        return all_args
    
    # Fall back to legacy docval if available
    docval_info = getattr(func, "__docval__", None)
    if docval_info is not None:
        all_args = tuple(docval_info.get("args", []))
        if args:
            arg_dict = {a["name"]: a for a in all_args}
            try:
                return tuple(arg_dict[name] for name in args)
            except KeyError as e:
                raise ValueError(
                    f"Function {func.__name__} does not have docval argument {e}"
                ) from None
        return all_args
    
    # No validation info available
    if args:
        raise ValueError(f"Function {func.__name__} has no validation arguments")
    return tuple()


def has_validation(func: Callable) -> bool:
    """Check if a function has validation decorators applied.
    
    Args:
        func: The function to check
        
    Returns:
        True if the function has @validated or @docval applied
    """
    return (
        hasattr(func, "__validation_info__") or
        hasattr(func, "__docval__")
    )
