"""Runtime construction of real-signature functions from docval-style argument specs.

HDMF generates constructors and convenience methods at runtime: `hdmf.build.classgenerator`
synthesizes ``__init__`` for schema-derived classes, and `MultiContainerInterface`
composes ``add_*``/``create_*``/``get_*`` methods at class-definition time. Historically
these were assembled with ``@docval(*spec_dicts)``. :func:`signature_function` builds the
same functions with real type-hinted signatures instead, so generated classes look like
ordinary Python to IDEs, ``inspect.signature``, and ``get_docval`` alike.
"""

import copy
import typing
from typing import Any, Literal

from ..utils import AllowPositional
from ._decorator import validated
from ._shapes import Shaped
from ._types import AnyData, ArrayData, Bool, Float, Int, ScalarData, TypeName, UInt

_NUMERIC_HINTS = {'int': Int, 'uint': UInt, 'float': Float, 'bool': Bool}
_MACRO_HINTS = {'array_data': ArrayData, 'scalar_data': ScalarData, 'data': AnyData}
# docval widened the bare python numeric classes exactly like the corresponding strings
_WIDENED_CLASSES = {int: Int, float: Float, bool: Bool}


def _hint_from_type(t):
    """Map a docval type expression (type, name string, macro, or tuple) to a type hint."""
    if t is None:
        return Any
    if isinstance(t, str):
        if t in _NUMERIC_HINTS:
            return _NUMERIC_HINTS[t]
        if t in _MACRO_HINTS:
            return _MACRO_HINTS[t]
        return TypeName[t]
    if isinstance(t, type):
        return _WIDENED_CLASSES.get(t, t)
    if isinstance(t, (list, tuple)):
        members = tuple(_hint_from_type(x) for x in t)
        if len(members) == 1:
            return members[0]
        return typing.Union[members]
    raise TypeError(f"cannot map docval type expression to a hint: {t!r}")


def hint_from_spec(spec):
    """Map one docval argument spec dict to the equivalent type hint."""
    if 'enum' in spec:
        hint = Literal[tuple(spec['enum'])]
    else:
        hint = _hint_from_type(spec.get('type'))
    if spec.get('shape') is not None:
        hint = Shaped[hint, tuple(spec['shape'])]
    if 'default' in spec and (spec['default'] is None or spec.get('allow_none', False)) and hint is not Any:
        hint = typing.Optional[hint]
    return hint


def _google_docstring(intro, arg_specs, returns=None):
    lines = [intro or '', '']
    if arg_specs:
        lines.append('Args:')
        for spec in arg_specs:
            lines.append(f"    {spec['name']}: {spec.get('doc') or spec.get('help', '')}")
    if returns:
        lines.append('')
        lines.append('Returns:')
        lines.append(f'    {returns}')
    return '\n'.join(lines).strip() + '\n'


def signature_function(func_name, arg_specs, body, *, doc=None, returns=None,
                       allow_positional=AllowPositional.ALLOWED, validate=True):
    """Build a real-signature function from docval-style argument spec dicts.

    Args:
        func_name: the ``__name__`` for the built function
        arg_specs: docval-style argument spec dicts (name/type/doc/default/shape/enum/allow_none)
        body: callable invoked as ``body(self, kwargs)``, where ``kwargs`` maps every
            declared argument name to its value (as ``@docval`` used to provide)
        doc: description for the generated docstring
        returns: description of the return value for the generated docstring
        allow_positional: positional-argument policy for the ``@validated`` wrapper
        validate: whether to wrap the function with ``@validated``

    Returns:
        the built function, with type-hinted signature, Google-style docstring, and
        (by default) ``@validated`` runtime checking
    """
    required = [a for a in arg_specs if 'default' not in a]
    optional = [a for a in arg_specs if 'default' in a]
    ordered = required + optional

    if not func_name.isidentifier() or any(not a['name'].isidentifier() for a in ordered):
        # python signatures cannot express non-identifier names; keep the legacy
        # docval behavior for these exotic cases (removed together with docval)
        from ..utils import docval

        def _adapter(self, **kwargs):
            return body(self, kwargs)

        return docval(*arg_specs, func_name=func_name, doc=doc,
                      allow_positional=allow_positional, enforce_type=validate)(_adapter)

    namespace = {'__body__': body, '__deepcopy__': copy.deepcopy}
    sig_parts = ['self']
    dict_parts = []
    for i, spec in enumerate(ordered):
        name = spec['name']
        if 'default' in spec:
            default_ref = f'__dflt_{i}__'
            namespace[default_ref] = spec['default']
            sig_parts.append(f'{name}={default_ref}')
            if isinstance(spec['default'], (list, dict, set)):
                # docval deepcopied defaults on every call; preserve that for mutables
                dict_parts.append(f'{name!r}: ({name} if {name} is not {default_ref} '
                                  f'else __deepcopy__({default_ref}))')
                continue
        else:
            sig_parts.append(name)
        dict_parts.append(f'{name!r}: {name}')

    src = (f"def {func_name}({', '.join(sig_parts)}):\n"
           f"    return __body__(self, {{{', '.join(dict_parts)}}})\n")
    exec(src, namespace)  # noqa: S102
    func = namespace[func_name]
    func.__annotations__ = {spec['name']: hint_from_spec(spec) for spec in ordered}
    func.__doc__ = _google_docstring(doc, ordered, returns)
    if validate:
        func = validated(func, allow_positional=allow_positional)
    return func
