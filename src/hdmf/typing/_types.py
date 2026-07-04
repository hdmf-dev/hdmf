"""Type aliases for use in type-hinted HDMF function signatures.

All aliases are enforceable by beartype directly (``@beartype``,
``beartype.door.is_bearable``, or HDMF's :func:`~hdmf.typing.validated`): the
numeric aliases are plain unions, and the macro/name aliases carry
:mod:`beartype.vale` validators. The ``get_docval`` compatibility shim knows how to
render each alias back into legacy docval vocabulary for downstream code that still
splices docval specs; that is the aliases' only connection to docval.
"""

import typing
from typing import Annotated, Any

import numpy as np

from ._validators import macro_validator, type_name_validator

# numeric aliases accepting numpy scalar types alongside the Python types, matching
# how HDMF has always treated numeric data. A bare `int` hint is strict under
# beartype and will NOT accept np.int32 — use these aliases for numeric arguments.
_int_types = (int, np.int8, np.int16, np.int32, np.int64)
_uint_types = (np.uint8, np.uint16, np.uint32, np.uint64)
_float_types = [float, np.float16, np.float32, np.float64]
if hasattr(np, "float128"):  # pragma: no cover
    _float_types.append(np.float128)
if hasattr(np, "longdouble"):  # pragma: no cover
    _float_types.append(np.longdouble)
_float_types = tuple(dict.fromkeys(_float_types))  # dedupe (longdouble may alias float128)
_bool_types = (bool, np.bool_)

Int = typing.Union[_int_types]
UInt = typing.Union[_uint_types]
Float = typing.Union[_float_types]
Bool = typing.Union[_bool_types]

# macro aliases accept instances of any type registered under the corresponding
# macro name; the registry is read at call time, so types registered later
# (e.g. by hdmf-zarr) are honored
ArrayData = Annotated[Any, macro_validator('array_data')]
ScalarData = Annotated[Any, macro_validator('scalar_data')]
AnyData = Annotated[Any, macro_validator('data')]


class TypeName:
    """Reference a type by class name, matched against the value's MRO at call time.

    ``TypeName['DynamicTable']`` accepts any value with a class named
    ``DynamicTable`` (or with that fully qualified ``module.qualname``) anywhere in
    its MRO. Use this for forward references that cross module boundaries, where a
    PEP 484 string annotation would not resolve.
    """

    def __class_getitem__(cls, name):
        if not isinstance(name, str):
            raise TypeError(f"TypeName[...] requires a class name string, got {name!r}")
        return Annotated[Any, type_name_validator(name)]


def register_macro(macro_name):
    """Class decorator registering a type under a macro name (e.g. ``'array_data'``).

    Successor to :func:`hdmf.utils.docval_macro`; both write to the same registry,
    which the macro aliases (:data:`ArrayData`, :data:`ScalarData`, :data:`AnyData`)
    read at call time.
    """
    from ..utils import docval_macro
    return docval_macro(macro_name)
