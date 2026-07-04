"""Helpers for verifying that a function migrated from ``@docval`` behaves identically.

Use these in migration PRs (in hdmf and in downstream libraries): keep a copy of the
old ``@docval``-decorated function, migrate the real one, and assert parity over a
set of call cases.
"""

from __future__ import annotations

import typing
from typing import Any, Callable

from ..utils import get_docval


def compare_validation_behavior(
    old_func: Callable,
    new_func: Callable,
    test_cases: list[dict[str, Any]],
    *,
    check_return_values: bool = True,
    check_error_types: bool = True,
) -> list[dict[str, Any]]:
    """Run both functions over the test cases and report any behavioral differences.

    Args:
        old_func: the original ``@docval``-decorated function
        new_func: the migrated type-hinted function
        test_cases: list of dicts with optional 'args' (tuple) and 'kwargs' (dict) keys
        check_return_values: also compare return values on success
        check_error_types: also compare exception types on failure

    Returns:
        One result dict per case; ``result['match']`` is False where behavior diverged.
    """
    results = []
    for i, case in enumerate(test_cases):
        args = case.get('args', ())
        kwargs = case.get('kwargs', {})
        result = {'case_index': i, 'args': args, 'kwargs': kwargs,
                  'old_result': None, 'new_result': None,
                  'old_error': None, 'new_error': None,
                  'match': True, 'details': []}
        try:
            result['old_result'] = old_func(*args, **kwargs)
        except Exception as e:
            result['old_error'] = (type(e), str(e))
        try:
            result['new_result'] = new_func(*args, **kwargs)
        except Exception as e:
            result['new_error'] = (type(e), str(e))

        if result['old_error'] is None and result['new_error'] is None:
            if check_return_values and result['old_result'] != result['new_result']:
                result['match'] = False
                result['details'].append(
                    f"return values differ: {result['old_result']!r} != {result['new_result']!r}")
        elif result['old_error'] is not None and result['new_error'] is not None:
            if check_error_types and result['old_error'][0] is not result['new_error'][0]:
                result['match'] = False
                result['details'].append(
                    f"error types differ: {result['old_error'][0].__name__} != "
                    f"{result['new_error'][0].__name__}")
        else:
            result['match'] = False
            if result['old_error'] is None:
                result['details'].append(f"old succeeded but new failed with: {result['new_error']}")
            else:
                result['details'].append(f"new succeeded but old failed with: {result['old_error']}")
        results.append(result)
    return results


def assert_validation_parity(old_func, new_func, test_cases, **kwargs):
    """Assert that both functions behave identically over the test cases."""
    results = compare_validation_behavior(old_func, new_func, test_cases, **kwargs)
    mismatches = [r for r in results if not r['match']]
    if mismatches:
        lines = []
        for r in mismatches:
            lines.append(f"case {r['case_index']} (args={r['args']!r}, kwargs={r['kwargs']!r}): "
                         + '; '.join(r['details']))
        raise AssertionError("validation behavior diverged:\n" + '\n'.join(lines))


def compare_docval_specs(old_func: Callable, new_func: Callable) -> dict[str, Any]:
    """Compare ``get_docval`` output between two functions.

    Works for any mix of ``@docval``-decorated and type-hinted functions, since
    :func:`hdmf.utils.get_docval` serves both. Verifies argument names, types,
    defaults, shapes, enums, and docs.
    """
    result = {'match': True, 'differences': []}
    old_by_name = {a['name']: a for a in get_docval(old_func)}
    new_by_name = {a['name']: a for a in get_docval(new_func)}

    missing = set(old_by_name) - set(new_by_name)
    extra = set(new_by_name) - set(old_by_name)
    if missing:
        result['match'] = False
        result['differences'].append(f"missing in new: {sorted(missing)}")
    if extra:
        result['match'] = False
        result['differences'].append(f"extra in new: {sorted(extra)}")

    for name in set(old_by_name) & set(new_by_name):
        old_arg, new_arg = old_by_name[name], new_by_name[name]
        for key in ('type', 'default', 'shape', 'enum', 'doc'):
            old_val = old_arg.get(key, '<absent>')
            new_val = new_arg.get(key, '<absent>')
            if key == 'type':
                old_val = _normalize_type(old_val)
                new_val = _normalize_type(new_val)
            if old_val != new_val:
                result['match'] = False
                result['differences'].append(
                    f"arg '{name}' {key} differs: {old_val!r} != {new_val!r}")
    return result


def _normalize_type(argtype):
    """Normalize a docval type expression for comparison.

    docval resolves macro strings to type tuples at decoration time while the
    synthesizer intentionally keeps the macro string; normalize both to a comparable
    frozenset of names.
    """
    from ..utils import get_docval_macro
    if argtype is None:
        return None
    if isinstance(argtype, str):
        try:
            expanded = get_docval_macro(argtype)
        except KeyError:
            return frozenset({argtype})
        return frozenset(t.__name__ for t in expanded)
    if isinstance(argtype, type):
        return frozenset({argtype.__name__})
    if isinstance(argtype, (list, tuple)):
        out = set()
        for t in argtype:
            out.update(_normalize_type(t))
        return frozenset(out)
    return frozenset({str(argtype)})


def find_required_nullable_params(func: Callable) -> list[str]:
    """Lint helper: return names of required parameters hinted ``T | None``.

    docval cannot express a required-but-nullable argument, so these hints are lossy
    for downstream splicing; give the parameter a default or drop ``| None``.
    """
    import inspect
    import types as _types

    from ._compat import _NoneType, _safe_hints
    sig = inspect.signature(func)
    hints = _safe_hints(func)
    flagged = []
    for name, param in sig.parameters.items():
        if name in ('self', 'cls') or param.default is not inspect.Parameter.empty:
            continue
        hint = hints.get(name)
        if typing.get_origin(hint) in (typing.Union, _types.UnionType) \
                and _NoneType in typing.get_args(hint):
            flagged.append(name)
    return flagged
