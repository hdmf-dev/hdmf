"""Round-trip tests for the get_docval compatibility shim.

The compatibility guarantee under test: for a type-hinted function, splicing
``get_docval(hinted_func, ...)`` into a legacy ``@docval`` decorator (the pervasive
downstream pattern, e.g. in PyNWB) must validate inputs exactly like the equivalent
hand-written docval spec.
"""

import typing

import numpy as np

from hdmf.testing import TestCase
from hdmf.typing import AnyData, ArrayData, Bool, Float, Int, ScalarData, Shaped, TypeName, UInt
from hdmf.utils import docval, get_docval, getargs


class TestSynthesizedSpecs(TestCase):
    """Test the docval spec dicts synthesized from type-hinted signatures."""

    def test_basic_types(self):
        def func(a: str, b: dict, c: bytes):
            """F.

            Args:
                a: doc a
                b: doc b
                c: doc c
            """

        self.assertTupleEqual(get_docval(func), (
            {'name': 'a', 'doc': 'doc a', 'type': str},
            {'name': 'b', 'doc': 'doc b', 'type': dict},
            {'name': 'c', 'doc': 'doc c', 'type': bytes},
        ))

    def test_numeric_aliases(self):
        def func(a: Int, b: UInt, c: Float, d: Bool):
            """F.

            Args:
                a: doc a
                b: doc b
                c: doc c
                d: doc d
            """

        types = [spec['type'] for spec in get_docval(func)]
        self.assertListEqual(types, ['int', 'uint', 'float', 'bool'])

    def test_macro_aliases_stay_strings(self):
        """Macro aliases must map to macro *strings* so late registrations apply."""
        def func(a: ArrayData, b: ScalarData, c: AnyData):
            """F.

            Args:
                a: doc a
                b: doc b
                c: doc c
            """

        types = [spec['type'] for spec in get_docval(func)]
        self.assertListEqual(types, ['array_data', 'scalar_data', 'data'])

    def test_union(self):
        def func(a: slice | list | tuple):
            """F.

            Args:
                a: doc a
            """

        self.assertEqual(get_docval(func, 'a')[0]['type'], (slice, list, tuple))

    def test_optional_with_none_default(self):
        def func(a: str | None = None):
            """F.

            Args:
                a: doc a
            """

        self.assertEqual(get_docval(func, 'a')[0], {'name': 'a', 'doc': 'doc a', 'type': str, 'default': None})

    def test_optional_with_non_none_default_sets_allow_none(self):
        def func(a: int | None = 5):
            """F.

            Args:
                a: doc a
            """

        self.assertEqual(get_docval(func, 'a')[0],
                         {'name': 'a', 'doc': 'doc a', 'type': int, 'default': 5, 'allow_none': True})

    def test_bare_int_keeps_numpy_widening(self):
        """Bare int/float/bool hints map to the classes, which docval's check_type widens."""
        def func(a: int, b: float, c: bool):
            """F.

            Args:
                a: doc a
                b: doc b
                c: doc c
            """

        types = [spec['type'] for spec in get_docval(func)]
        self.assertListEqual(types, [int, float, bool])
        # widening parity after splice: np.int32 must pass for a bare int hint
        @docval(*get_docval(func), is_method=False)
        def downstream(**kwargs):
            return kwargs

        downstream(a=np.int32(1), b=np.float32(0.5), c=np.bool_(True))

    def test_literal_maps_to_enum(self):
        def func(a: typing.Literal['r', 'w'] = 'r'):
            """F.

            Args:
                a: doc a
            """

        self.assertEqual(get_docval(func, 'a')[0],
                         {'name': 'a', 'doc': 'doc a', 'type': str, 'enum': ('r', 'w'), 'default': 'r'})

    def test_shaped(self):
        def func(a: Shaped[ArrayData, (None, 3)]):
            """F.

            Args:
                a: doc a
            """

        self.assertEqual(get_docval(func, 'a')[0],
                         {'name': 'a', 'doc': 'doc a', 'type': 'array_data', 'shape': (None, 3)})

    def test_type_name(self):
        def func(a: TypeName['DynamicTable']):  # noqa: F821
            """F.

            Args:
                a: doc a
            """

        self.assertEqual(get_docval(func, 'a')[0]['type'], 'DynamicTable')

    def test_unresolvable_forward_ref_kept_as_string(self):
        def func(a: "NotARealClassAnywhere"):  # noqa: F821
            """F.

            Args:
                a: doc a
            """

        self.assertEqual(get_docval(func, 'a')[0]['type'], 'NotARealClassAnywhere')

    def test_parametrized_generic_degrades_to_origin(self):
        def func(a: dict[str, int]):
            """F.

            Args:
                a: doc a
            """

        self.assertEqual(get_docval(func, 'a')[0]['type'], dict)

    def test_unannotated_param_is_any_type(self):
        def func(a: str, b=None):
            """F.

            Args:
                a: doc a
                b: doc b
            """

        self.assertEqual(get_docval(func, 'b')[0], {'name': 'b', 'doc': 'doc b', 'type': None, 'default': None})

    def test_missing_docstring_doc_is_empty_string(self):
        def func(a: str):
            pass

        self.assertEqual(get_docval(func, 'a')[0], {'name': 'a', 'doc': '', 'type': str})

    def test_self_and_kwargs_skipped(self):
        class C:
            def method(self, a: str, **kwargs):
                """M.

                Args:
                    a: doc a
                """

        self.assertEqual([s['name'] for s in get_docval(C.method)], ['a'])

    def test_named_arg_selection_and_missing_name_raises(self):
        def func(a: str, b: int = 1):
            """F.

            Args:
                a: doc a
                b: doc b
            """

        self.assertEqual(get_docval(func, 'b')[0]['name'], 'b')
        with self.assertRaisesRegex(ValueError, "does not have docval argument"):
            get_docval(func, 'nonexistent')

    def test_no_annotations_legacy_behavior(self):
        def func(a, b=None):
            pass

        self.assertTupleEqual(get_docval(func), tuple())
        with self.assertRaisesRegex(ValueError, "has no docval arguments"):
            get_docval(func, 'a')

    def test_star_args_rejected(self):
        def func(a: str, *args):
            pass

        with self.assertRaisesRegex(TypeError, r"\*args"):
            get_docval(func)

    def test_docval_decorated_takes_precedence(self):
        """A @docval function with annotations elsewhere must use the legacy path."""
        @docval({'name': 'a', 'type': str, 'doc': 'doc a'})
        def func(self, **kwargs):
            pass

        self.assertEqual(get_docval(func, 'a')[0]['doc'], 'doc a')


class TestSpliceRoundTrip(TestCase):
    """Splice synthesized specs into legacy @docval and verify validation parity."""

    @staticmethod
    def _make_downstream(*specs):
        @docval(*specs, is_method=False)
        def downstream(**kwargs):
            return kwargs
        return downstream

    def test_str_and_macro_splice(self):
        def parent(name: str, data: ArrayData):
            """P.

            Args:
                name: the name
                data: the data
            """

        downstream = self._make_downstream(*get_docval(parent))
        self.assertEqual(downstream(name='n', data=[1, 2])['name'], 'n')
        downstream(name='n', data=np.arange(3))  # ndarray accepted by macro
        with self.assertRaisesRegex(TypeError, "incorrect type for 'name'"):
            downstream(name=5, data=[1, 2])
        with self.assertRaisesRegex(TypeError, "incorrect type for 'data'"):
            downstream(name='n', data=5)

    def test_numpy_widening_after_splice(self):
        def parent(count: Int, frac: Float, flag: Bool):
            """P.

            Args:
                count: c
                frac: f
                flag: g
            """

        downstream = self._make_downstream(*get_docval(parent))
        result = downstream(count=np.int32(5), frac=np.float32(0.5), flag=np.bool_(True))
        self.assertEqual(result['count'], 5)
        with self.assertRaisesRegex(TypeError, "incorrect type for 'count'"):
            downstream(count=5.0, frac=0.5, flag=True)

    def test_shape_after_splice(self):
        def parent(data: Shaped[ArrayData, (None, 3)]):
            """P.

            Args:
                data: the data
            """

        downstream = self._make_downstream(*get_docval(parent))
        downstream(data=[[1, 2, 3], [4, 5, 6]])
        with self.assertRaisesRegex(ValueError, "incorrect shape for data"):
            downstream(data=[[1, 2], [3, 4]])

    def test_enum_after_splice(self):
        def parent(mode: typing.Literal['r', 'w'] = 'r'):
            """P.

            Args:
                mode: the mode
            """

        downstream = self._make_downstream(*get_docval(parent))
        downstream(mode='w')
        with self.assertRaisesRegex(ValueError, "forbidden value for 'mode'"):
            downstream(mode='x')

    def test_default_after_splice(self):
        def parent(count: Int = 7):
            """P.

            Args:
                count: c
            """

        downstream = self._make_downstream(*get_docval(parent))
        self.assertEqual(downstream()['count'], 7)

    def test_mixed_splice_with_new_docval_args(self):
        """The canonical pynwb pattern: parent args spliced next to new spec dicts."""
        def parent(name: str, data: AnyData | None = None):
            """P.

            Args:
                name: the name
                data: the data
            """

        @docval(*get_docval(parent, 'name'),
                {'name': 'extra', 'type': 'int', 'doc': 'an extra', 'default': 0},
                *get_docval(parent, 'data'),
                is_method=False)
        def downstream(**kwargs):
            return getargs('name', 'extra', 'data', kwargs)

        self.assertEqual(downstream(name='n'), ['n', 0, None])
        with self.assertRaisesRegex(TypeError, "incorrect type for 'extra'"):
            downstream(name='n', extra='x')

    def test_type_name_after_splice(self):
        from hdmf.common import DynamicTable

        def parent(table: TypeName['DynamicTable']):  # noqa: F821
            """P.

            Args:
                table: a table
            """

        downstream = self._make_downstream(*get_docval(parent))
        downstream(table=DynamicTable(name='t', description='d'))
        with self.assertRaisesRegex(TypeError, "incorrect type for 'table'"):
            downstream(table=5)

    def test_macro_registration_before_splice_applies(self):
        """Macro registrations made before splice decoration must be honored.

        Note: docval resolves macro strings to concrete type tuples at decoration
        time, so (exactly as in pure-docval code) registrations made *after* a
        downstream class is decorated do not apply to it. Synthesized specs keep
        the macro string, so resolution happens at each splice's decoration.
        """
        from hdmf.utils import docval_macro

        def parent(data: TypeName['SpliceTestData']):
            """P.

            Args:
                data: the data
            """

        downstream = self._make_downstream(*get_docval(parent))

        class SpliceTestData:
            pass

        downstream(data=SpliceTestData())  # MRO-name matching needs no registration

        # macro string case: register a type, then splice — the new type is accepted
        @docval_macro('data')
        class RegisteredBeforeSplice:
            pass

        def parent2(data: AnyData):
            """P.

            Args:
                data: the data
            """

        downstream2 = self._make_downstream(*get_docval(parent2))
        downstream2(data=RegisteredBeforeSplice())


class TestDocvalSpecComparison(TestCase):
    """Verify synthesized specs match hand-written docval specs for equivalent functions."""

    def test_parity_with_hand_written_docval(self):
        from hdmf.typing.testing import compare_docval_specs

        @docval({'name': 'name', 'type': str, 'doc': 'the name'},
                {'name': 'data', 'type': ('array_data', 'data'), 'doc': 'the data', 'shape': (None, 3)},
                {'name': 'count', 'type': 'int', 'doc': 'how many', 'default': 1})
        def old(self, **kwargs):
            pass

        def new(name: str, data: Shaped[ArrayData | AnyData, (None, 3)], count: Int = 1):
            """N.

            Args:
                name: the name
                data: the data
                count: how many
            """

        result = compare_docval_specs(old, new)
        self.assertTrue(result['match'], result['differences'])

    def test_required_nullable_lint(self):
        from hdmf.typing.testing import find_required_nullable_params

        def func(a: str | None, b: int, c: str | None = None):
            pass

        self.assertEqual(find_required_nullable_params(func), ['a'])
