"""Synthesis of docval argument specifications from type-hinted signatures.

This is the compatibility linchpin for the migration away from ``@docval``
(see https://github.com/hdmf-dev/hdmf/issues/1129): :func:`hdmf.utils.get_docval`
falls back to :func:`synthesize_docval` for functions that have type hints instead
of a ``__docval__`` attribute. The synthesized dicts use only keys and value forms
that legacy ``@docval`` accepts, so they can be spliced into downstream decorators
(``@docval(*get_docval(Parent.__init__, ...))``) and consumed by HDMF's build
machinery unchanged.
"""

import inspect
import types
import typing
import warnings
from typing import Annotated, Any, Literal

from ._docstrings import parse_docstring
from ._types import _bool_types, _float_types, _int_types, _uint_types
from ._validators import compat_info

_synth_attr_name = '__synth_docval__'

_NoneType = type(None)

# used to render numeric alias unions back to docval's numeric vocabulary; order
# matters only for readability of the resulting spec
_NUMERIC_ALIASES = (
    ('uint', frozenset(_uint_types)),
    ('int', frozenset(_int_types)),
    ('float', frozenset(_float_types)),
    ('bool', frozenset(_bool_types)),
)


def _collapse_numeric(members):
    """Replace complete numeric-alias type sets in a union with docval's string names.

    E.g. ``(int, np.int8, ..., str)`` (from an ``Int | str`` hint) becomes
    ``('int', str)``.
    """
    mset = {m for m in members if isinstance(m, type)}
    member_to_name = {}
    for name, type_set in _NUMERIC_ALIASES:
        if type_set <= mset:
            for t in type_set:
                member_to_name[t] = name
    out = []
    for m in members:
        name = member_to_name.get(m) if isinstance(m, type) else None
        if name is None:
            out.append(m)
        elif name not in out:
            out.append(name)
    return tuple(out)


class MappedHint:
    """Result of mapping one type hint to docval spec fields.

    ``fields`` holds the docval spec keys derived from the hint ('type', and
    optionally 'shape' and 'enum'). ``exact`` is False when the mapping lost
    information (e.g. ``list[int]`` degraded to ``list``), in which case runtime
    validation should prefer the original hint over the synthesized spec.
    ``none_allowed`` is True when the hint had an explicit ``| None`` member.
    """

    __slots__ = ('fields', 'exact', 'none_allowed')

    def __init__(self, fields, exact=True, none_allowed=False):
        self.fields = fields
        self.exact = exact
        self.none_allowed = none_allowed


def _is_numpydantic_ndarray(hint):
    """Return True if the hint is a numpydantic/nptyping NDArray specialization."""
    mod = getattr(type(hint), '__module__', '') or ''
    if not (mod.startswith('numpydantic') or mod.startswith('nptyping')):
        return False
    return 'NDArray' in (getattr(hint, '__name__', '') or repr(hint))


def _ndarray_shape_tuple(hint):
    """Best-effort extraction of a docval shape tuple from an NDArray hint.

    Returns None when the shape is unconstrained or cannot be interpreted.
    """
    try:
        shape_expr = hint.__args__[0]  # nptyping-style: NDArray[Shape[...], dtype]
        entries = getattr(shape_expr, 'prepared_args', None)
        if not entries:
            return None
        dims = []
        for entry in entries:
            # entries look like '*', '2', '* x', '3 y'
            size = str(entry).split()[0]
            if size == '*':
                dims.append(None)
            elif size.isdigit():
                dims.append(int(size))
            else:
                return None  # named/variadic dims we cannot express in docval
        return tuple(dims)
    except Exception:
        return None


def map_hint(hint):  # noqa: C901
    """Map a single type hint to docval spec fields. Returns a ``MappedHint``."""
    if hint is None or hint is _NoneType or hint is inspect.Parameter.empty or hint is Any:
        return MappedHint({'type': None})

    # unresolved forward reference: docval matches the string against the MRO
    if isinstance(hint, str):
        if not hint.replace('.', '').isidentifier():
            # a compound annotation string (e.g. "A | None") that could not be
            # resolved; matching it against MRO names would never succeed
            warnings.warn(
                f"could not resolve string annotation {hint!r}; it will not be validated. "
                "Use hdmf.typing.TypeName[...] for forward references to later-defined names.",
                stacklevel=2,
            )
            return MappedHint({'type': None}, exact=False)
        return MappedHint({'type': hint})
    if isinstance(hint, typing.ForwardRef):
        return MappedHint({'type': hint.__forward_arg__})

    origin = typing.get_origin(hint)

    if origin is Annotated:
        base, *metadata = typing.get_args(hint)
        docval_name = None
        shape = None
        for meta in metadata:
            info = compat_info(meta)
            if info is None:
                continue
            if 'docval_name' in info:
                docval_name = info['docval_name']
            if 'shape' in info:
                shape = info['shape']
        if docval_name is not None:
            fields = {'type': docval_name}
            if shape is not None:
                fields['shape'] = shape
            return MappedHint(fields)
        mapped = map_hint(base)
        if shape is not None:
            mapped.fields['shape'] = shape
        return mapped

    if origin in (typing.Union, types.UnionType):
        members = []
        exact = True
        none_allowed = False
        shape = None
        enum = None
        for arg in typing.get_args(hint):
            if arg is _NoneType:
                none_allowed = True
                continue
            mapped = map_hint(arg)
            exact = exact and mapped.exact
            shape = shape if shape is not None else mapped.fields.get('shape')
            enum = enum if enum is not None else mapped.fields.get('enum')
            member_type = mapped.fields['type']
            if isinstance(member_type, tuple):
                members.extend(member_type)
            else:
                members.append(member_type)
        # dedupe while preserving order (unhashable members are not produced here),
        # then render complete numeric alias sets back to docval's string names
        members = _collapse_numeric(tuple(dict.fromkeys(members)))
        fields = {'type': members[0] if len(members) == 1 else members}
        if shape is not None:
            fields['shape'] = shape
        if enum is not None:
            fields['enum'] = enum
        return MappedHint(fields, exact=exact, none_allowed=none_allowed)

    if origin is Literal:
        values = typing.get_args(hint)
        value_types = tuple(dict.fromkeys(type(v) for v in values))
        fields = {
            'type': value_types[0] if len(value_types) == 1 else value_types,
            'enum': tuple(values),
        }
        return MappedHint(fields)

    if _is_numpydantic_ndarray(hint):
        fields = {'type': 'array_data'}
        shape = _ndarray_shape_tuple(hint)
        if shape is not None:
            fields['shape'] = shape
        # numpydantic's isinstance() checks dtype and named dims that docval cannot;
        # runtime validation should use the hint itself
        return MappedHint(fields, exact=False)

    if origin is not None:
        # parametrized generic (list[int], dict[str, int], Callable[..., X], ...):
        # docval never checked element types, so degrade to the bare origin class
        if isinstance(origin, type):
            return MappedHint({'type': origin}, exact=False)
        return MappedHint({'type': None}, exact=False)

    if isinstance(hint, type):
        return MappedHint({'type': hint})

    # anything else (TypeVar, Protocol instance, special form): docval "any type"
    return MappedHint({'type': None}, exact=False)


def _safe_hints(func):
    """Return the function's type hints with extras, tolerating unresolvable names.

    ``typing.get_type_hints`` raises on the first unresolvable forward reference; in
    that case fall back to the raw ``__annotations__``, evaluating each entry
    individually and keeping unresolvable ones as strings (docval MRO-name semantics).
    """
    try:
        return typing.get_type_hints(func, include_extras=True)
    except Exception:
        pass
    hints = {}
    raw = getattr(func, '__annotations__', {})
    globalns = getattr(func, '__globals__', {})
    for name, annotation in raw.items():
        if isinstance(annotation, str):
            try:
                hints[name] = eval(annotation, globalns)  # noqa: S307
            except Exception:
                hints[name] = annotation
        else:
            hints[name] = annotation
    return hints


def synthesize_docval(func):
    """Synthesize docval argument specs from a type-hinted function.

    Returns ``(specs, idx, meta)`` where ``specs`` is a tuple of docval-compatible
    argument spec dicts in signature order, ``idx`` maps argument name to spec, and
    ``meta`` is a dict with function-level info: 'allow_extra' (function accepts
    ``**kwargs``), 'rtype'/'returns' (from the return annotation and docstring), and
    'exact' (name -> bool, False where the spec is a lossy rendering of the hint).
    The result is cached on the function.
    """
    target = inspect.unwrap(getattr(func, '__func__', func))
    cached = getattr(target, _synth_attr_name, None)
    if cached is not None:
        return cached

    sig = inspect.signature(target)
    hints = _safe_hints(target)
    param_docs, returns_doc, _ = parse_docstring(target)

    specs = []
    exact = {}
    allow_extra = False
    for name, param in sig.parameters.items():
        if name in ('self', 'cls'):
            continue
        if param.kind is inspect.Parameter.VAR_KEYWORD:
            allow_extra = True
            continue
        if param.kind is inspect.Parameter.VAR_POSITIONAL:
            raise TypeError(
                f"cannot synthesize docval arguments for {target.__qualname__}: "
                "variadic positional arguments (*args) are not supported"
            )
        mapped = map_hint(hints.get(name, param.annotation))
        spec = {'name': name, 'doc': param_docs.get(name, '')}
        spec.update(mapped.fields)
        if param.default is not inspect.Parameter.empty:
            spec['default'] = param.default
            if mapped.none_allowed and param.default is not None:
                spec['allow_none'] = True
        # required args with `| None` hints are not expressible in docval and the
        # spec omits None; see the migration guide
        specs.append(spec)
        exact[name] = mapped.exact

    rtype_hint = hints.get('return')
    meta = {
        'allow_extra': allow_extra,
        'rtype': rtype_hint if rtype_hint is not None else None,
        'returns': returns_doc,
        'exact': exact,
    }
    result = (tuple(specs), {s['name']: s for s in specs}, meta)
    try:
        setattr(target, _synth_attr_name, result)
    except (AttributeError, TypeError):  # non-writable callables (builtins, slots)
        pass
    return result
