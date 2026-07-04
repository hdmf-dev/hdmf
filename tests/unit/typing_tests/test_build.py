"""Tests for hdmf.typing.signature_function (runtime signature builder)."""

import inspect

import numpy as np

from hdmf.testing import TestCase
from hdmf.typing import signature_function
from hdmf.utils import AllowPositional, docval, get_docval


SPECS = [
    {'name': 'name', 'type': str, 'doc': 'the name'},
    {'name': 'data', 'type': ('array_data', 'data'), 'doc': 'the data', 'shape': (None,)},
    {'name': 'count', 'type': 'int', 'doc': 'how many', 'default': 1},
    {'name': 'tags', 'type': list, 'doc': 'tags', 'default': list()},
]


def _body(self, kwargs):
    return kwargs


class Dummy:
    pass


class TestSignatureFunction(TestCase):

    def setUp(self):
        self.func = signature_function('__init__', SPECS, _body, doc='Make a thing.')
        self.obj = Dummy()

    def test_real_signature(self):
        params = list(inspect.signature(self.func).parameters)
        self.assertEqual(params, ['self', 'name', 'data', 'count', 'tags'])

    def test_call_and_defaults(self):
        out = self.func(self.obj, name='n', data=[1, 2], count=np.int8(3))
        self.assertEqual(out['name'], 'n')
        self.assertEqual(out['count'], 3)
        self.assertEqual(out['tags'], [])

    def test_mutable_default_isolated(self):
        out1 = self.func(self.obj, name='n', data=[1])
        out1['tags'].append('x')
        out2 = self.func(self.obj, name='n', data=[1])
        self.assertEqual(out2['tags'], [])

    def test_validation(self):
        with self.assertRaisesRegex(TypeError, "incorrect type for 'name'"):
            self.func(self.obj, name=5, data=[1])
        with self.assertRaisesRegex(ValueError, "incorrect shape for data"):
            self.func(self.obj, name='n', data=[[1, 2]])

    def test_get_docval_round_trip_splices_into_docval(self):
        specs = get_docval(self.func)
        self.assertEqual([s['name'] for s in specs], ['name', 'data', 'count', 'tags'])

        @docval(*[dict(s) for s in specs], is_method=False)
        def downstream(**kwargs):
            return kwargs

        self.assertEqual(downstream(name='n', data=[1, 2, 3])['count'], 1)
        with self.assertRaisesRegex(TypeError, "incorrect type for 'name'"):
            downstream(name=5, data=[1])

    def test_docstring(self):
        self.assertIn('Make a thing.', self.func.__doc__)
        self.assertIn('name: the name', self.func.__doc__)

    def test_non_identifier_names_fall_back_to_docval(self):
        func = signature_function('...', [{'name': '...', 'type': str, 'doc': 'weird'}], _body)
        self.assertTrue(hasattr(func, '__docval__'))  # legacy path
        self.assertEqual(func(self.obj, **{'...': 'x'})['...'], 'x')

    def test_positional_policy(self):
        func = signature_function('f', [{'name': 'a', 'type': str, 'doc': 'a'}], _body,
                                  allow_positional=AllowPositional.ERROR)
        with self.assertRaisesRegex(SyntaxError, 'Only keyword arguments'):
            func(self.obj, 'x')
        self.assertEqual(func(self.obj, a='x')['a'], 'x')

    def test_enum_spec(self):
        func = signature_function('f', [{'name': 'mode', 'type': str, 'doc': 'm', 'enum': ['r', 'w'],
                                         'default': 'r'}], _body)
        self.assertEqual(func(self.obj, mode='w')['mode'], 'w')
        with self.assertRaisesRegex(ValueError, "forbidden value for 'mode'"):
            func(self.obj, mode='x')
