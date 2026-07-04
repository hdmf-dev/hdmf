"""Tests for the @validated decorator: docval-parity runtime validation of type hints."""

import typing

import numpy as np

from hdmf.testing import TestCase
from hdmf.typing import (
    AllowPositional,
    AnyData,
    ArrayData,
    Float,
    Int,
    Shaped,
    TypeName,
    set_type_checking,
    validated,
)
from hdmf.utils import get_docval


class TestValidatedTypes(TestCase):

    def test_basic_type_error_message_matches_docval(self):
        @validated
        def func(a: str):
            """F.

            Args:
                a: doc a
            """
            return a

        self.assertEqual(func('x'), 'x')
        with self.assertRaisesWith(TypeError,
                                   "TestValidatedTypes.test_basic_type_error_message_matches_docval."
                                   "<locals>.func: incorrect type for 'a' (got 'int', expected 'str')"):
            func(5)

    def test_numpy_widening(self):
        @validated
        def func(a: Int, b: Float):
            """F.

            Args:
                a: doc a
                b: doc b
            """
            return a

        self.assertEqual(func(np.int64(3), np.float16(0.5)), 3)
        self.assertEqual(func(3, 0.5), 3)
        with self.assertRaisesRegex(TypeError, "incorrect type for 'a'"):
            func(3.0, 0.5)

    def test_union(self):
        @validated
        def func(a: slice | list):
            """F.

            Args:
                a: doc a
            """
            return a

        func(slice(0, 5))
        func([1, 2])
        with self.assertRaisesRegex(TypeError, "expected 'slice or list'"):
            func((1, 2))

    def test_macro_alias_uses_live_registry(self):
        from hdmf.utils import docval_macro

        @validated
        def func(a: AnyData):
            """F.

            Args:
                a: doc a
            """
            return a

        @docval_macro('data')
        class RegisteredAfterDecoration:
            pass

        func(RegisteredAfterDecoration())

    def test_none_default_allows_none(self):
        @validated
        def func(a: str | None = None):
            """F.

            Args:
                a: doc a
            """
            return a

        self.assertIsNone(func())
        self.assertIsNone(func(None))
        self.assertEqual(func('x'), 'x')

    def test_none_for_required_arg(self):
        @validated
        def func(a: str):
            """F.

            Args:
                a: doc a
            """
            return a

        with self.assertRaisesRegex(TypeError, "None is not allowed for 'a' \\(expected 'str', not None\\)"):
            func(None)

    def test_shape(self):
        @validated
        def func(data: Shaped[ArrayData, ((None,), (None, None))]):
            """F.

            Args:
                data: doc
            """
            return data

        func([1, 2, 3])
        func([[1], [2]])
        with self.assertRaisesRegex(ValueError, "incorrect shape for data"):
            func([[[1]]])

    def test_shape_unwraps_by_argname(self):
        """docval parity: if the value's shape is unreadable, getattr(value, argname) is checked."""
        class HasData:
            def __init__(self, data):
                self.data = data

        @validated
        def func(data: Shaped[typing.Any, (None, 3)]):
            """F.

            Args:
                data: doc
            """
            return data

        func(HasData([[1, 2, 3]]))
        with self.assertRaisesRegex(ValueError, "incorrect shape for data"):
            func(HasData([[1, 2]]))

    def test_enum(self):
        @validated
        def func(mode: typing.Literal['r', 'w', 'a']):
            """F.

            Args:
                mode: doc
            """
            return mode

        self.assertEqual(func('w'), 'w')
        with self.assertRaisesRegex(ValueError, "forbidden value for 'mode' \\(got 'x', expected"):
            func('x')

    def test_type_name_mro_matching(self):
        @validated
        def func(thing: TypeName['MroTestTarget']):
            """F.

            Args:
                thing: doc
            """
            return thing

        class MroTestTarget:
            pass

        class Subclass(MroTestTarget):
            pass

        func(MroTestTarget())
        func(Subclass())
        with self.assertRaisesRegex(TypeError, "expected 'MroTestTarget'"):
            func(5)

    def test_multiple_errors_aggregated(self):
        @validated
        def func(a: str, b: Int):
            """F.

            Args:
                a: doc a
                b: doc b
            """

        with self.assertRaisesRegex(
                TypeError,
                "incorrect type for 'a' \\(got 'int', expected 'str'\\), "
                "incorrect type for 'b' \\(got 'str', expected 'int'\\)"):
            func(5, 'x')

    def test_term_set_wrapper_passes_through(self):
        from hdmf.term_set import TermSetWrapper

        # a minimal object satisfying TermSetWrapper's interface is complex to build;
        # instead check that the wrapper type is unwrapped for validation using __new__
        wrapper = TermSetWrapper.__new__(TermSetWrapper)
        wrapper.__dict__['_TermSetWrapper__value'] = 'wrapped-value'

        @validated
        def func(a: str):
            """F.

            Args:
                a: doc a
            """
            return a

        result = func(wrapper)
        self.assertIs(result, wrapper)  # wrapper object, not the unwrapped value, reaches the body


class TestValidatedCallConventions(TestCase):

    def test_positional_warning(self):
        @validated(allow_positional=AllowPositional.WARNING)
        def func(a: str):
            """F.

            Args:
                a: doc a
            """
            return a

        with self.assertWarnsRegex(FutureWarning, "Using positional arguments for this method is discouraged"):
            func('x')

    def test_positional_error(self):
        @validated(allow_positional=AllowPositional.ERROR)
        def func(a: str):
            """F.

            Args:
                a: doc a
            """
            return a

        with self.assertRaisesRegex(SyntaxError, "Only keyword arguments"):
            func('x')
        self.assertEqual(func(a='x'), 'x')

    def test_method_self_not_counted_as_positional(self):
        class C:
            @validated(allow_positional=AllowPositional.ERROR)
            def method(self, a: str = 'd'):
                """M.

                Args:
                    a: doc a
                """
                return a

        self.assertEqual(C().method(a='x'), 'x')  # self alone must not trigger the error

    def test_missing_and_unrecognized_args(self):
        @validated
        def func(a: str):
            """F.

            Args:
                a: doc a
            """
            return a

        with self.assertRaisesRegex(TypeError, "missing a required argument: 'a'"):
            func()
        with self.assertRaisesRegex(TypeError, "unexpected keyword argument 'b'"):
            func(a='x', b=1)

    def test_kwargs_extras_pass_through_unvalidated(self):
        @validated
        def func(a: str, **kwargs):
            """F.

            Args:
                a: doc a
            """
            return kwargs

        sentinel = object()
        self.assertEqual(func(a='x', anything=sentinel), {'anything': sentinel})

    def test_keyword_only_params(self):
        @validated
        def func(*, a: str):
            """F.

            Args:
                a: doc a
            """
            return a

        self.assertEqual(func(a='x'), 'x')
        with self.assertRaisesRegex(TypeError, "too many positional arguments"):
            func('x')

    def test_enforce_flags(self):
        @validated(enforce_type=False, enforce_shape=False)
        def func(a: str, data: Shaped[ArrayData, (None, 3)] = None):
            """F.

            Args:
                a: doc a
                data: doc
            """
            return a

        self.assertEqual(func(5, data=[[1, 2]]), 5)

    def test_type_checking_kill_switch(self):
        @validated
        def func(a: str):
            """F.

            Args:
                a: doc a
            """
            return a

        set_type_checking(False)
        try:
            self.assertEqual(func(5), 5)  # no validation
        finally:
            set_type_checking(True)
        with self.assertRaises(TypeError):
            func(5)

    def test_wrapper_metadata(self):
        @validated
        def func(a: str):
            """The description.

            Args:
                a: doc a
            """
            return a

        self.assertEqual(func.__name__, 'func')
        self.assertIn('The description.', func.__doc__)
        self.assertEqual(func.__validated__['args'][0]['name'], 'a')

    def test_get_docval_on_validated_function(self):
        @validated
        def func(a: str, data: ArrayData | None = None):
            """F.

            Args:
                a: doc a
                data: the data
            """

        self.assertEqual(get_docval(func, 'a')[0], {'name': 'a', 'doc': 'doc a', 'type': str})
        self.assertEqual(get_docval(func, 'data')[0],
                         {'name': 'data', 'doc': 'the data', 'type': 'array_data', 'default': None})


class TestValidatedLossyHints(TestCase):
    """Hints docval cannot express are validated against the original hint via beartype."""

    def test_parametrized_generic_element_check(self):
        @validated
        def func(m: dict[str, int]):
            """F.

            Args:
                m: doc m
            """
            return m

        func({'a': 1})
        with self.assertRaisesRegex(TypeError, "incorrect type for 'm'"):
            func({'a': 'not-an-int'})

    def test_numpydantic_ndarray(self):
        from numpydantic import NDArray, Shape

        @validated
        def func(data: NDArray[Shape["* x, 3 y"], np.int64]):  # noqa: F722
            """F.

            Args:
                data: doc
            """
            return data

        func(np.zeros((4, 3), dtype=np.int64))
        with self.assertRaisesRegex(TypeError, "incorrect type for 'data'"):
            func(np.zeros((4, 2), dtype=np.int64))

        # the synthesized docval spec still carries an equivalent shape
        self.assertEqual(get_docval(func, 'data')[0]['shape'], (None, 3))


class TestBeartypeNativeEnforcement(TestCase):
    """The hdmf.typing aliases are real beartype validators: they are enforced by
    plain beartype (no @validated involved), because validation is built on the
    type-hint system itself, not on docval."""

    def test_macro_alias_enforced_by_plain_beartype(self):
        from beartype import beartype
        from beartype.roar import BeartypeCallHintParamViolation

        @beartype
        def func(a: ArrayData):
            return a

        func([1, 2, 3])
        func(np.arange(3))
        with self.assertRaises(BeartypeCallHintParamViolation):
            func(5)

    def test_type_name_enforced_by_plain_beartype(self):
        from beartype import beartype
        from beartype.roar import BeartypeCallHintParamViolation

        @beartype
        def func(thing: TypeName['BeartypeNativeTarget']):
            return thing

        class BeartypeNativeTarget:
            pass

        func(BeartypeNativeTarget())
        with self.assertRaises(BeartypeCallHintParamViolation):
            func(5)

    def test_shaped_enforced_by_plain_beartype(self):
        from beartype import beartype
        from beartype.roar import BeartypeCallHintParamViolation

        @beartype
        def func(data: Shaped[ArrayData, (None, 3)]):
            return data

        func([[1, 2, 3], [4, 5, 6]])
        with self.assertRaises(BeartypeCallHintParamViolation):
            func([[1, 2], [3, 4]])

    def test_bare_int_hint_is_strict(self):
        """A bare `int` hint has standard type-hint semantics: numpy ints are
        rejected. Use hdmf.typing.Int for numpy widening."""
        @validated
        def func(a: int):
            """F.

            Args:
                a: doc a
            """
            return a

        self.assertEqual(func(5), 5)
        with self.assertRaisesRegex(TypeError, "incorrect type for 'a'"):
            func(np.int32(5))

    def test_int_alias_in_union_collapses_in_spec(self):
        """Int | str synthesizes to docval ('int', str), not the raw numpy union."""
        def func(a: Int | str):
            """F.

            Args:
                a: doc a
            """

        self.assertEqual(get_docval(func, 'a')[0]['type'], ('int', str))


class TestValidationParityHarness(TestCase):
    """Test the parity harness itself on a known-equivalent function pair."""

    def test_assert_validation_parity(self):
        from hdmf.typing.testing import assert_validation_parity
        from hdmf.utils import docval, getargs

        @docval({'name': 'name', 'type': str, 'doc': 'the name'},
                {'name': 'count', 'type': 'int', 'doc': 'how many', 'default': 1},
                is_method=False)
        def old(**kwargs):
            name, count = getargs('name', 'count', kwargs)
            return (name, count)

        @validated
        def new(name: str, count: Int = 1):
            """N.

            Args:
                name: the name
                count: how many
            """
            return (name, count)

        assert_validation_parity(old, new, [
            {'kwargs': {'name': 'a'}},
            {'kwargs': {'name': 'a', 'count': 5}},
            {'kwargs': {'name': 'a', 'count': np.int16(5)}},
            {'kwargs': {'name': 5}},
            {'kwargs': {'name': 'a', 'count': 'x'}},
            {'kwargs': {}},
        ])
