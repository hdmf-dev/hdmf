"""The @validated decorator: runtime validation driven by type hints.

Validation semantics intentionally mirror ``@docval`` so that a migrated function
accepts and rejects exactly the same inputs, with the same error messages. For each
parameter, the hint is mapped to a docval spec (see ``_compat.map_hint``); hints that
map exactly are checked with :func:`hdmf.utils.check_type` (numpy widening, live
macro registry, MRO-name matching), and hints that docval cannot express
(parametrized generics, numpydantic ``NDArray``) are checked against the original
hint via beartype.
"""

import functools
import inspect
import os
import warnings

from beartype.door import is_bearable

from ..utils import AllowPositional, check_type
from ._compat import _is_numpydantic_ndarray, _safe_hints, synthesize_docval
from ._shapes import check_shape

_TYPE_CHECKING_ENABLED = os.environ.get('HDMF_TYPE_CHECKING', '').lower() not in ('off', '0', 'false')


def set_type_checking(enabled):
    """Globally enable or disable call-time validation by @validated functions.

    Validation can also be disabled by setting the environment variable
    ``HDMF_TYPE_CHECKING=off`` before importing hdmf.
    """
    global _TYPE_CHECKING_ENABLED
    _TYPE_CHECKING_ENABLED = bool(enabled)


def _format_type(argtype):
    # mirrors hdmf.utils.__format_type
    if isinstance(argtype, str):
        return argtype
    elif isinstance(argtype, type):
        return argtype.__name__
    elif isinstance(argtype, (tuple, list)):
        parts = [_format_type(i) for i in argtype]
        if len(parts) > 1:
            return "%s or %s" % (", ".join(parts[:-1]), parts[-1])
        return parts[0]
    elif argtype is None:
        return "any type"
    raise ValueError("argtype must be a type, str, list, or tuple")


def _fmt_str_quotes(x):
    # mirrors hdmf.utils.__fmt_str_quotes
    if isinstance(x, (list, tuple)):
        return '{}'.format(x)
    if isinstance(x, str):
        return "'%s'" % x
    return str(x)


class _ParamCheck:
    """Precomputed validation info for one parameter."""

    __slots__ = ('name', 'spec', 'hint', 'use_hint', 'is_ndarray_hint', 'allow_none', 'required')

    def __init__(self, name, spec, hint, use_hint):
        self.name = name
        self.spec = spec
        self.hint = hint
        self.use_hint = use_hint  # validate against the original hint (lossy mapping)
        self.is_ndarray_hint = use_hint and _is_numpydantic_ndarray(hint)
        self.required = 'default' not in spec
        self.allow_none = (not self.required
                           and (spec['default'] is None or spec.get('allow_none', False)))

    def type_ok(self, argval):
        if argval is None:
            return self.allow_none
        if self.use_hint:
            if self.is_ndarray_hint:
                # numpydantic NDArray implements isinstance() with dtype/shape checks
                return isinstance(argval, self.hint)
            return is_bearable(argval, self.hint)
        return check_type(argval, self.spec['type'], allow_none=self.allow_none)

    def expected_str(self):
        if self.use_hint:
            hint_str = repr(self.hint)
            return hint_str.removeprefix('typing.') if hint_str.startswith('typing.') else str(self.hint)
        return _format_type(self.spec['type'])


def validated(func=None, *, enforce_type=True, enforce_shape=True,
              allow_positional=AllowPositional.ALLOWED):
    """Decorator validating arguments of a type-hinted function with docval semantics.

    Args:
        enforce_type: whether to check argument types at call time
        enforce_shape: whether to check array shapes (from ``Shaped[...]`` hints)
        allow_positional: policy for positional arguments, mirroring the docval
            option of the same name. Prefer real keyword-only parameters (``*,``)
            over ``AllowPositional.ERROR`` in new code.
    """
    if func is None:
        return lambda f: _apply_validated(f, enforce_type, enforce_shape, allow_positional)
    return _apply_validated(func, enforce_type, enforce_shape, allow_positional)


def _apply_validated(func, enforce_type, enforce_shape, allow_positional):  # noqa: C901
    sig = inspect.signature(func)
    specs, idx, meta = synthesize_docval(func)
    hints = _safe_hints(func)

    param_names = list(sig.parameters)
    has_receiver = bool(param_names) and param_names[0] in ('self', 'cls')

    checks = {}
    for spec in specs:
        name = spec['name']
        hint = hints.get(name, sig.parameters[name].annotation)
        use_hint = not meta['exact'].get(name, True)
        checks[name] = _ParamCheck(name, spec, hint, use_hint)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if not _TYPE_CHECKING_ENABLED:
            return func(*args, **kwargs)

        n_positional = len(args) - (1 if has_receiver else 0)
        if n_positional > 0:
            if allow_positional == AllowPositional.WARNING:
                msg = ('%s: Using positional arguments for this method is discouraged and will be '
                       'deprecated in a future major release. Please use keyword arguments to '
                       'ensure future compatibility.' % func.__qualname__)
                warnings.warn(msg, category=FutureWarning, stacklevel=2)
            elif allow_positional == AllowPositional.ERROR:
                msg = ('%s: Only keyword arguments (e.g., func(argname=value, ...)) are allowed '
                       'for this method.' % func.__qualname__)
                raise SyntaxError(msg)

        try:
            bound = sig.bind(*args, **kwargs)
        except TypeError as e:
            raise TypeError('%s: %s' % (func.__qualname__, e)) from None

        type_errors = []
        value_errors = []
        from ..term_set import TermSetWrapper  # circular import fix, as in hdmf.utils

        for name, argval in bound.arguments.items():
            check = checks.get(name)
            if check is None:  # self/cls or **kwargs extras: passed through unvalidated
                continue
            if isinstance(argval, TermSetWrapper):
                # validate the wrapped value; the wrapper itself is passed to the body
                argval = argval.value
            spec = check.spec
            if enforce_type:
                if not check.type_ok(argval):
                    if argval is None and (check.required or spec['default'] is None):
                        type_errors.append("None is not allowed for '%s' (expected '%s', not None)"
                                           % (name, check.expected_str()))
                    else:
                        type_errors.append("incorrect type for '%s' (got '%s', expected '%s')"
                                           % (name, type(argval).__name__, check.expected_str()))
            if enforce_shape and 'shape' in spec and argval is not None:
                err = check_shape(name, argval, spec['shape'])
                if err is not None:
                    value_errors.append(err)
            if 'enum' in spec and argval is not None and argval not in spec['enum']:
                value_errors.append("forbidden value for '%s' (got %s, expected %s)"
                                    % (name, _fmt_str_quotes(argval), spec['enum']))

        if type_errors:
            raise TypeError('%s: %s' % (func.__qualname__, ', '.join(type_errors)))
        if value_errors:
            raise ValueError('%s: %s' % (func.__qualname__, ', '.join(value_errors)))

        return func(*args, **kwargs)

    wrapper.__validated__ = {
        'args': specs,
        'enforce_type': enforce_type,
        'enforce_shape': enforce_shape,
        'allow_positional': allow_positional,
        'allow_extra': meta['allow_extra'],
    }
    return wrapper
