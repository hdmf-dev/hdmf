"""beartype validators implementing HDMF's type semantics.

These are real :mod:`beartype.vale` validators, so any beartype-aware consumer
(``@beartype``, ``beartype.door.is_bearable``, or HDMF's ``@validated``) enforces
them natively. Each validator also carries compatibility info in a side registry
(see :func:`compat_info`) so the ``get_docval`` shim can render it back into legacy
docval vocabulary; that shim is the only docval-facing piece, and it goes away when
docval does.
"""

from beartype.vale import Is

# side registry: beartype validators are slotted and cannot carry attributes, so map
# validator identity -> compat info (with a strong reference to keep the id stable)
_COMPAT_INFO = {}


def _remember(validator, info):
    _COMPAT_INFO[id(validator)] = (validator, info)
    return validator


def compat_info(metadata):
    """Return the compat-info dict for an HDMF validator, or None for foreign metadata."""
    entry = _COMPAT_INFO.get(id(metadata))
    return entry[1] if entry is not None else None


def matches_type_name(value, name):
    """Return True if any class in the value's MRO matches ``name``.

    ``name`` may be a bare class name or a fully qualified ``module.qualname``.
    This mirrors how docval matched string type names, without importing the type.
    """
    for cls in type(value).__mro__:
        if cls.__name__ == name or f"{cls.__module__}.{cls.__qualname__}" == name:
            return True
    return False


_macro_validators = {}
_type_name_validators = {}
_shape_validators = {}


def macro_validator(macro_name):
    """A validator accepting instances of any type registered under ``macro_name``.

    The registry is read at call time, so types registered later (e.g. by hdmf-zarr)
    are honored.
    """
    if macro_name in _macro_validators:
        return _macro_validators[macro_name]

    def checker(value):
        types_ = _macro_types(macro_name)
        return bool(types_) and isinstance(value, types_)

    validator = _remember(Is[checker], {'docval_name': macro_name})
    _macro_validators[macro_name] = validator
    return validator


def _macro_types(macro_name):
    # the registry currently lives in hdmf.utils for docval compatibility; it will
    # move here when docval is removed
    from ..utils import get_docval_macro
    try:
        return tuple(t for t in get_docval_macro(macro_name) if isinstance(t, type))
    except KeyError:
        return ()


def type_name_validator(name):
    """A validator accepting values with ``name`` anywhere in their class MRO."""
    if name in _type_name_validators:
        return _type_name_validators[name]

    def checker(value):
        return matches_type_name(value, name)

    validator = _remember(Is[checker], {'docval_name': name})
    _type_name_validators[name] = validator
    return validator


def shape_validator(shape):
    """A validator checking array shape when the value's shape is determinable.

    Values whose shape cannot be read pass here; ``@validated`` applies the stricter
    check (including the unwrap-by-argument-name fallback, which needs the argument
    name and therefore cannot live in a type validator).
    """
    if shape in _shape_validators:
        return _shape_validators[shape]

    def checker(value):
        from ..utils import get_data_shape
        from ._shapes import _shape_okay_multi
        valshape = get_data_shape(value)
        if valshape is None:
            return True
        return _shape_okay_multi(valshape, shape)

    validator = _remember(Is[checker], {'shape': shape})
    _shape_validators[shape] = validator
    return validator
