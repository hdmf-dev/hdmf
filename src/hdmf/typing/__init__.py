"""Type hints and runtime validation for HDMF (successor to ``@docval``).

This package provides:

- Type aliases enforceable by beartype anywhere (``@beartype``,
  ``beartype.door.is_bearable``, or HDMF's :func:`validated`): numeric aliases
  (``Int``, ``UInt``, ``Float``, ``Bool``) accepting numpy scalar
  types, macro aliases (``ArrayData``, ``ScalarData``, ``AnyData``)
  backed by a live type registry, :class:`TypeName` for cross-module forward
  references, and :class:`Shaped` for array shape requirements. numpydantic
  ``NDArray[...]`` hints are also supported for dtype- and shape-checked arrays.
- The :func:`validated` decorator, which validates arguments of a type-hinted
  function at call time through beartype/numpydantic.
- A compatibility layer so :func:`hdmf.utils.get_docval` works on plain type-hinted
  functions: docval-format argument specs are synthesized from the signature, type
  hints, and Google-style docstring. This keeps downstream code that splices parent
  argument specs (``@docval(*get_docval(Parent.__init__, ...))``) working while
  ``@docval`` is phased out (https://github.com/hdmf-dev/hdmf/issues/1129); it is
  the only part of this package that speaks docval, and it will be removed together
  with docval.

Migration tooling lives in :mod:`hdmf.typing.migrate`
(``python -m hdmf.typing.migrate --help``) and parity-testing helpers in
:mod:`hdmf.typing.testing`.
"""

from ..utils import AllowPositional  # re-export: used as a @validated option
from ._build import hint_from_spec, signature_function
from ._compat import map_hint, synthesize_docval
from ._decorator import set_type_checking, validated
from ._shapes import Shaped
from ._types import (
    AnyData,
    ArrayData,
    Bool,
    Float,
    Int,
    ScalarData,
    TypeName,
    UInt,
    register_macro,
)

__all__ = [
    'AllowPositional',
    'AnyData',
    'ArrayData',
    'Bool',
    'Float',
    'Int',
    'ScalarData',
    'Shaped',
    'TypeName',
    'UInt',
    'hint_from_spec',
    'map_hint',
    'register_macro',
    'signature_function',
    'set_type_checking',
    'synthesize_docval',
    'validated',
]
