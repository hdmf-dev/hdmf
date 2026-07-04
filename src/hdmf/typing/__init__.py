"""Type hints and runtime validation for HDMF (successor to ``@docval``).

This package provides:

- Type aliases (:data:`Int`, :data:`Float`, :data:`ArrayData`, :class:`TypeName`,
  :class:`Shaped`, ...) that carry docval-equivalent semantics: numpy numeric
  widening, the live docval macro registry, MRO-name matching for forward
  references, and array shape specifications.
- The :func:`validated` decorator, which validates arguments of a type-hinted
  function at call time with the same semantics and error messages as ``@docval``.
- A compatibility layer so :func:`hdmf.utils.get_docval` works on plain type-hinted
  functions, keeping downstream libraries that splice parent argument specs
  (``@docval(*get_docval(Parent.__init__, ...))``) working during and after the
  migration away from ``@docval`` (https://github.com/hdmf-dev/hdmf/issues/1129).

Migration tooling lives in :mod:`hdmf.typing.migrate`
(``python -m hdmf.typing.migrate --help``) and parity-testing helpers in
:mod:`hdmf.typing.testing`.
"""

from ..utils import AllowPositional  # re-export: used as a @validated option
from ._compat import map_hint, synthesize_docval
from ._decorator import set_type_checking, validated
from ._shapes import ShapeSpec, Shaped
from ._types import (
    AnyData,
    ArrayData,
    Bool,
    DocvalType,
    Float,
    Int,
    ScalarData,
    TypeName,
    UInt,
)

__all__ = [
    'AllowPositional',
    'AnyData',
    'ArrayData',
    'Bool',
    'DocvalType',
    'Float',
    'Int',
    'ScalarData',
    'ShapeSpec',
    'Shaped',
    'TypeName',
    'UInt',
    'map_hint',
    'set_type_checking',
    'synthesize_docval',
    'validated',
]
