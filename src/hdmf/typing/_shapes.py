"""Array shape annotations and validation with docval-parity semantics."""

from typing import Annotated


class ShapeSpec:
    """Annotated metadata carrying a docval-style shape specification.

    A shape is a tuple where each element is an int (exact dimension length) or None
    (any length), e.g. ``(None, 3)``. A tuple of such tuples means any of the listed
    shapes is allowed.
    """

    __slots__ = ('shape',)

    def __init__(self, shape):
        if not isinstance(shape, (tuple, list)):
            raise TypeError(f"shape must be a tuple or list, got {shape!r}")
        self.shape = tuple(tuple(s) if isinstance(s, (tuple, list)) else s for s in shape)

    def __repr__(self):
        return f"ShapeSpec({self.shape!r})"

    def __eq__(self, other):
        return isinstance(other, ShapeSpec) and other.shape == self.shape

    def __hash__(self):
        return hash((ShapeSpec, self.shape))


class Shaped:
    """Annotate a type with a required array shape: ``Shaped[ArrayData, (None, 3)]``.

    Equivalent to the docval ``'shape'`` key, including its fallback behaviors:
    values whose shape cannot be determined are unwrapped via ``getattr(value, argname)``
    (e.g. a ``TimeSeries`` passed for an argument named ``data`` has its ``.data``
    checked).
    """

    def __class_getitem__(cls, item):
        if not (isinstance(item, tuple) and len(item) == 2):
            raise TypeError("Shaped[...] requires two arguments: Shaped[type, shape]")
        t, shape = item
        return Annotated[t, ShapeSpec(shape)]


def _shape_okay(valshape, argshape):
    if len(valshape) != len(argshape):
        return False
    return all(b in (a, None) for a, b in zip(valshape, argshape))


def _shape_okay_multi(valshape, argshape):
    if argshape and isinstance(argshape[0], (tuple, list)):  # multiple allowable shapes
        return any(_shape_okay(valshape, a) for a in argshape)
    return _shape_okay(valshape, argshape)


def _shape_error_message(argname, valshape, allowable_shapes):
    # mirrors hdmf.utils.__shape_error_message
    if isinstance(allowable_shapes, (list, tuple)) and all(isinstance(e, (list, tuple)) for e in allowable_shapes):
        allowable_shapes_str = " or ".join(map(str, allowable_shapes))
    else:
        allowable_shapes_str = str(allowable_shapes)
    allowable_shapes_str = allowable_shapes_str.replace("None", "*")
    return f"incorrect shape for {argname}: got {valshape}, and expected {allowable_shapes_str}"


def check_shape(argname, value, shape):
    """Check ``value`` against a docval-style ``shape`` spec with docval's exact semantics.

    Returns None if the shape validates, otherwise an error message string.
    """
    from ..utils import get_data_shape  # deferred; utils must not import this module at top level

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
