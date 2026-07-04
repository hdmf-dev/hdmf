"""The @validated decorator: runtime validation driven purely by type hints.

Validation runs through beartype (``beartype.door.is_bearable``) for every
parameter, including the :mod:`hdmf.typing` aliases, whose semantics are
implemented as beartype validators (see ``_validators``). numpydantic ``NDArray``
hints are checked through numpydantic's own ``isinstance`` machinery. No docval
machinery is involved; the docval-format specs synthesized at decoration time exist
only so :func:`hdmf.utils.get_docval` keeps working for downstream code during the
migration, and are otherwise used here only to phrase error messages.
"""

import functools
import inspect
import os
import typing
import warnings

from beartype.door import is_bearable

from ..utils import AllowPositional
from ._compat import _is_numpydantic_ndarray, _safe_hints, synthesize_docval
from ._shapes import check_shape
from ._validators import compat_info, matches_type_name

_TYPE_CHECKING_ENABLED = os.environ.get('HDMF_TYPE_CHECKING', '').lower() not in ('off', '0', 'false')


def set_type_checking(enabled):
    """Globally enable or disable call-time validation by @validated functions.

    Validation can also be disabled by setting the environment variable
    ``HDMF_TYPE_CHECKING=off`` before importing hdmf.
    """
    global _TYPE_CHECKING_ENABLED
    _TYPE_CHECKING_ENABLED = bool(enabled)


def _format_type(argtype):
    # renders synthesized docval-vocabulary types into readable error messages
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
    if isinstance(x, (list, tuple)):
        return '{}'.format(x)
    if isinstance(x, str):
        return "'%s'" % x
    return str(x)


def _without_shape_validators(hint):
    """Return the hint with HDMF shape validators removed.

    ``@validated`` runs its own shape pass (with the unwrap-by-argument-name
    fallback and shape violations raising ValueError, matching docval), so shape
    must not also fail the beartype type check, where it would be misclassified as
    a type error. The full annotation still enforces shape for plain-beartype
    consumers.
    """
    if typing.get_origin(hint) is not typing.Annotated:
        return hint
    base, *metadata = typing.get_args(hint)
    kept = [m for m in metadata if 'shape' not in (compat_info(m) or {})]
    if len(kept) == len(metadata):
        return hint
    if kept:
        return typing.Annotated[tuple([base] + kept)]
    return base


class _ParamCheck:
    """Precomputed validation info for one parameter."""

    __slots__ = ('name', 'spec', 'hint', 'checker', 'allow_none', 'required')

    def __init__(self, name, spec, hint, func_qualname):
        self.name = name
        self.spec = spec
        self.hint = hint
        self.required = 'default' not in spec
        self.allow_none = not self.required and (spec['default'] is None or spec.get('allow_none', False))
        self.checker = self._build_checker(func_qualname)

    def _build_checker(self, func_qualname):
        hint = _without_shape_validators(self.hint)
        if hint is None or hint is inspect.Parameter.empty or hint is object:
            return None  # unannotated: accept anything
        if 'enum' in self.spec:
            # Literal membership is checked separately (ValueError, like docval);
            # here only check the value's type against the literal values' types
            base_types = tuple(dict.fromkeys(type(v) for v in self.spec['enum']))
            return lambda value: isinstance(value, base_types)
        if isinstance(hint, str):
            # unresolvable forward reference: match by class name in the MRO
            return lambda value: matches_type_name(value, hint)
        if isinstance(hint, typing.ForwardRef):
            forward_name = hint.__forward_arg__
            return lambda value: matches_type_name(value, forward_name)
        if _is_numpydantic_ndarray(hint):
            # numpydantic implements isinstance() with dtype and shape checking
            return lambda value: isinstance(value, hint)
        try:
            is_bearable(None, hint)  # force beartype to compile the hint now
        except Exception as e:
            warnings.warn(
                f"{func_qualname}: type hint {hint!r} for argument '{self.name}' is not "
                f"checkable at runtime ({type(e).__name__}); it will not be validated",
                stacklevel=4,
            )
            return None
        return lambda value: is_bearable(value, hint)

    def type_ok(self, argval):
        if argval is None:
            # match the long-standing `arg: T = None` idiom: None is valid whenever
            # the default is None, even if the hint does not include None
            return self.allow_none or (self.checker is not None and self.checker(None))
        return self.checker is None or self.checker(argval)

    def expected_str(self):
        spec_type = self.spec.get('type')
        if spec_type is not None:
            return _format_type(spec_type)
        return str(self.hint)


def validated(func=None, *, enforce_type=True, enforce_shape=True,
              allow_positional=AllowPositional.ALLOWED):
    """Decorator validating arguments of a type-hinted function at call time.

    Args:
        enforce_type: whether to check argument types at call time
        enforce_shape: whether to check array shapes (from ``Shaped[...]`` hints)
        allow_positional: policy for positional arguments. Prefer real keyword-only
            parameters (``*,``) over ``AllowPositional.ERROR`` in new code.
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
        checks[name] = _ParamCheck(name, spec, hint, func.__qualname__)

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
        from ..term_set import TermSetWrapper  # deferred to avoid a circular import

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
