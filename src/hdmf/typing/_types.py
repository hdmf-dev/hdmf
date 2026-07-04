"""Type aliases for use in type-hinted HDMF function signatures.

These aliases carry :class:`DocvalType` metadata so that :func:`hdmf.utils.get_docval`
can map them back to the exact docval type vocabulary (``'int'``, ``'array_data'``,
etc.), preserving behavior when the synthesized specs are spliced into legacy
``@docval`` decorators downstream.
"""

import typing
from typing import Annotated, Any

import numpy as np


class DocvalType:
    """Annotated metadata marking a type alias as equivalent to a docval type string.

    The string is interpreted exactly as docval interprets it: macro names
    (``'array_data'``, ``'scalar_data'``, ``'data'``) resolve against the live macro
    registry, ``'int'``/``'uint'``/``'float'``/``'bool'`` use numpy-widening checks,
    and any other string matches class names in the value's MRO.
    """

    __slots__ = ('name',)

    def __init__(self, name: str):
        self.name = name

    def __repr__(self):
        return f"DocvalType({self.name!r})"

    def __eq__(self, other):
        return isinstance(other, DocvalType) and other.name == self.name

    def __hash__(self):
        return hash((DocvalType, self.name))


# numeric aliases with the same numpy widening as docval's check_type
_int_types = (int, np.int8, np.int16, np.int32, np.int64)
_uint_types = (np.uint8, np.uint16, np.uint32, np.uint64)
_float_types = [float, np.float16, np.float32, np.float64]
if hasattr(np, "float128"):  # pragma: no cover
    _float_types.append(np.float128)
if hasattr(np, "longdouble"):  # pragma: no cover
    _float_types.append(np.longdouble)
_float_types = tuple(dict.fromkeys(_float_types))  # dedupe (longdouble may alias float128)

Int = Annotated[typing.Union[_int_types], DocvalType('int')]
UInt = Annotated[typing.Union[_uint_types], DocvalType('uint')]
Float = Annotated[typing.Union[_float_types], DocvalType('float')]
Bool = Annotated[bool | np.bool_, DocvalType('bool')]

# macro aliases resolve against the live registry in hdmf.utils at call time,
# so types registered later (e.g. by hdmf-zarr via @docval_macro) are honored
ArrayData = Annotated[Any, DocvalType('array_data')]
ScalarData = Annotated[Any, DocvalType('scalar_data')]
AnyData = Annotated[Any, DocvalType('data')]


class TypeName:
    """Reference a type by class name, matched against the value's MRO at call time.

    ``TypeName['DynamicTable']`` behaves exactly like the docval type string
    ``'DynamicTable'``: the value passes if any class in its MRO is named
    ``DynamicTable`` (or has that fully qualified ``module.qualname``). Use this for
    forward references that cross module boundaries, where a PEP 484 string
    annotation would not resolve.
    """

    def __class_getitem__(cls, name):
        if not isinstance(name, str):
            raise TypeError(f"TypeName[...] requires a class name string, got {name!r}")
        return Annotated[Any, DocvalType(name)]
