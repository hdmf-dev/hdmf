"""Array shape annotations and validation.

For plain array arguments, prefer numpydantic (``NDArray[Shape["* x, 3 y"], ...]``),
which checks dtype and shape through its own ``isinstance`` machinery. ``Shaped``
exists for HDMF's looser shape semantics: it accepts anything (not just arrays), and
under ``@validated`` it applies the historical fallback of unwrapping a value by
argument name (e.g. a ``TimeSeries`` passed for an argument named ``data`` has its
``.data`` checked) when the value's own shape cannot be determined.
"""

from typing import Annotated

from ._validators import shape_validator


class Shaped:
    """Annotate a type with a required array shape: ``Shaped[ArrayData, (None, 3)]``.

    A shape is a tuple where each element is an int (exact dimension length) or None
    (any length); a tuple of such tuples means any of the listed shapes is allowed.
    The annotation is enforced by beartype wherever the value's shape is
    determinable; ``@validated`` additionally applies the unwrap-by-argument-name
    fallback.
    """

    def __class_getitem__(cls, item):
        if not (isinstance(item, tuple) and len(item) == 2):
            raise TypeError("Shaped[...] requires two arguments: Shaped[type, shape]")
        t, shape = item
        if not isinstance(shape, (tuple, list)):
            raise TypeError(f"shape must be a tuple or list, got {shape!r}")
        shape = tuple(tuple(s) if isinstance(s, (tuple, list)) else s for s in shape)
        return Annotated[t, shape_validator(shape)]


def _shape_okay(valshape, argshape):
    if len(valshape) != len(argshape):
        return False
    return all(b in (a, None) for a, b in zip(valshape, argshape))


def _shape_okay_multi(valshape, argshape):
    if argshape and isinstance(argshape[0], (tuple, list)):  # multiple allowable shapes
        return any(_shape_okay(valshape, a) for a in argshape)
    return _shape_okay(valshape, argshape)


def _shape_error_message(argname, valshape, allowable_shapes):
    if isinstance(allowable_shapes, (list, tuple)) and all(isinstance(e, (list, tuple)) for e in allowable_shapes):
        allowable_shapes_str = " or ".join(map(str, allowable_shapes))
    else:
        allowable_shapes_str = str(allowable_shapes)
    allowable_shapes_str = allowable_shapes_str.replace("None", "*")
    return f"incorrect shape for {argname}: got {valshape}, and expected {allowable_shapes_str}"


def check_shape(argname, value, shape):
    """Check ``value`` against a shape spec, unwrapping by argument name if needed.

    Returns None if the shape validates, otherwise an error message string.
    """
    from ..utils import get_data_shape

    argval = value
    valshape = get_data_shape(argval)
    while valshape is None:
        if argval is None:
            return None
        if not hasattr(argval, argname):
            return ("cannot check shape of object '%s' for argument '%s' (expected shape '%s')"
                    % (argval, argname, shape))
        # unpack, e.g. if TimeSeries is passed for arg 'data', then TimeSeries.data is checked
        argval = getattr(argval, argname)
        valshape = get_data_shape(argval)
    if valshape is not None and not _shape_okay_multi(valshape, shape):
        return _shape_error_message(argname, valshape, shape)
    return None
