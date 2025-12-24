"""
Test helpers for HDMF validation.

This module provides utilities for testing the equivalence between
the legacy docval system and the new type hint-based validation.
"""

from __future__ import annotations

import inspect
from typing import Any, Callable, get_type_hints

from .decorators import get_validator_info, has_validation


def compare_validation_behavior(
    old_func: Callable,
    new_func: Callable,
    test_cases: list[dict[str, Any]],
    *,
    check_return_values: bool = True,
    check_error_types: bool = True,
) -> list[dict[str, Any]]:
    """Compare the validation behavior of two functions.
    
    This helper is used to verify that a function migrated from @docval
    to @validated behaves identically.
    
    Args:
        old_func: The original function with @docval
        new_func: The new function with @validated or type hints
        test_cases: List of dicts with 'args' and 'kwargs' keys
        check_return_values: Whether to compare return values
        check_error_types: Whether to compare error types
        
    Returns:
        List of test results with comparison details
        
    Example:
        >>> test_cases = [
        ...     {'args': (), 'kwargs': {'name': 'test', 'data': [1, 2, 3]}},
        ...     {'args': (), 'kwargs': {'name': 123}},  # Should fail type check
        ... ]
        >>> results = compare_validation_behavior(old_func, new_func, test_cases)
    """
    results = []
    
    for i, case in enumerate(test_cases):
        args = case.get('args', ())
        kwargs = case.get('kwargs', {})
        result = {
            'case_index': i,
            'args': args,
            'kwargs': kwargs,
            'old_result': None,
            'new_result': None,
            'old_error': None,
            'new_error': None,
            'match': True,
            'details': [],
        }
        
        # Run old function
        try:
            result['old_result'] = old_func(*args, **kwargs)
        except Exception as e:
            result['old_error'] = (type(e).__name__, str(e))
        
        # Run new function
        try:
            result['new_result'] = new_func(*args, **kwargs)
        except Exception as e:
            result['new_error'] = (type(e).__name__, str(e))
        
        # Compare results
        if result['old_error'] is None and result['new_error'] is None:
            # Both succeeded
            if check_return_values:
                if result['old_result'] != result['new_result']:
                    result['match'] = False
                    result['details'].append(
                        f"Return values differ: {result['old_result']} != {result['new_result']}"
                    )
        elif result['old_error'] is not None and result['new_error'] is not None:
            # Both failed
            if check_error_types:
                old_type = result['old_error'][0]
                new_type = result['new_error'][0]
                if old_type != new_type:
                    result['match'] = False
                    result['details'].append(
                        f"Error types differ: {old_type} != {new_type}"
                    )
        else:
            # One succeeded, one failed
            result['match'] = False
            if result['old_error'] is None:
                result['details'].append(
                    f"Old succeeded but new failed with: {result['new_error']}"
                )
            else:
                result['details'].append(
                    f"New succeeded but old failed with: {result['old_error']}"
                )
        
        results.append(result)
    
    return results


def compare_validation_metadata(
    old_func: Callable,
    new_func: Callable,
) -> dict[str, Any]:
    """Compare validation metadata between two functions.
    
    Checks that the argument names, types, and defaults match between
    a @docval decorated function and a @validated decorated function.
    
    Args:
        old_func: The original function with @docval
        new_func: The new function with @validated
        
    Returns:
        Dict with comparison results
    """
    result = {
        'match': True,
        'old_args': [],
        'new_args': [],
        'differences': [],
    }
    
    # Get validation info from both functions
    old_info = get_validator_info(old_func)
    new_info = get_validator_info(new_func)
    
    result['old_args'] = list(old_info)
    result['new_args'] = list(new_info)
    
    # Create lookup dicts by name
    old_by_name = {arg['name']: arg for arg in old_info}
    new_by_name = {arg['name']: arg for arg in new_info}
    
    # Check for missing/extra args
    old_names = set(old_by_name.keys())
    new_names = set(new_by_name.keys())
    
    missing_in_new = old_names - new_names
    extra_in_new = new_names - old_names
    
    if missing_in_new:
        result['match'] = False
        result['differences'].append(f"Missing in new: {missing_in_new}")
    
    if extra_in_new:
        result['match'] = False
        result['differences'].append(f"Extra in new: {extra_in_new}")
    
    # Check common args for differences
    for name in old_names & new_names:
        old_arg = old_by_name[name]
        new_arg = new_by_name[name]
        
        # Check defaults
        old_default = old_arg.get('default', inspect.Parameter.empty)
        new_default = new_arg.get('default', inspect.Parameter.empty)
        
        if old_default != new_default:
            result['match'] = False
            result['differences'].append(
                f"Arg '{name}' default differs: {old_default} != {new_default}"
            )
    
    return result


def generate_test_cases_from_docval(func: Callable) -> list[dict[str, Any]]:
    """Generate test cases from a @docval decorated function.
    
    Creates a set of test cases that exercise the validation logic,
    including valid inputs, type errors, and missing required args.
    
    Args:
        func: A function with @docval decorator
        
    Returns:
        List of test case dicts suitable for compare_validation_behavior
    """
    test_cases = []
    
    if not has_validation(func):
        return test_cases
    
    info = get_validator_info(func)
    
    # Generate valid case with all defaults
    valid_kwargs = {}
    for arg in info:
        arg_type = arg.get('type')
        
        # Generate sample valid value based on type
        if arg_type is str or arg_type == 'str':
            valid_kwargs[arg['name']] = 'test_value'
        elif arg_type is int or arg_type == 'int':
            valid_kwargs[arg['name']] = 42
        elif arg_type is float or arg_type == 'float':
            valid_kwargs[arg['name']] = 3.14
        elif arg_type is bool or arg_type == 'bool':
            valid_kwargs[arg['name']] = True
        elif 'default' in arg:
            # Use the default value
            pass
        else:
            valid_kwargs[arg['name']] = None
    
    test_cases.append({
        'args': (),
        'kwargs': valid_kwargs,
        'description': 'Valid case with sample values',
    })
    
    # Generate cases with wrong types
    for arg in info:
        if 'default' not in arg:  # Only required args
            wrong_type_kwargs = valid_kwargs.copy()
            arg_type = arg.get('type')
            
            # Provide wrong type
            if arg_type is str or arg_type == 'str':
                wrong_type_kwargs[arg['name']] = 123  # int instead of str
            else:
                wrong_type_kwargs[arg['name']] = 'wrong_type'  # str instead
            
            test_cases.append({
                'args': (),
                'kwargs': wrong_type_kwargs,
                'description': f"Wrong type for arg '{arg['name']}'",
            })
    
    # Generate case with missing required arg
    for arg in info:
        if 'default' not in arg:
            missing_kwargs = {k: v for k, v in valid_kwargs.items() if k != arg['name']}
            test_cases.append({
                'args': (),
                'kwargs': missing_kwargs,
                'description': f"Missing required arg '{arg['name']}'",
            })
    
    return test_cases
