"""Tests for the @docval -> @validated migration tool."""

import textwrap

from hdmf.testing import TestCase
from hdmf.typing.migrate import DocvalMigrator


class TestDocvalMigrator(TestCase):

    def _migrate(self, source):
        migrator = DocvalMigrator()
        return migrator.migrate_source(textwrap.dedent(source))

    def test_basic_conversion(self):
        migrated, n_converted, n_skipped = self._migrate('''\
            from hdmf.utils import docval, getargs


            class Thing:

                @docval({'name': 'name', 'type': str, 'doc': 'the name'},
                        {'name': 'count', 'type': 'int', 'doc': 'how many', 'default': 1})
                def __init__(self, **kwargs):
                    name, count = getargs('name', 'count', kwargs)
                    self.name = name
                    self.count = count
            ''')
        self.assertEqual(n_converted, 1)
        self.assertEqual(n_skipped, 0)
        self.assertIn('@validated', migrated)
        self.assertIn('def __init__(self, name: str, count: Int = 1):', migrated)
        self.assertIn('name: the name', migrated)
        self.assertNotIn('getargs', migrated.split('import')[-1].split('\n', 1)[1])
        self.assertIn('from hdmf.typing import Int, validated', migrated)
        # the migrated module must be valid python
        compile(migrated, '<migrated>', 'exec')

    def test_macro_shape_enum_conversion(self):
        migrated, n_converted, _ = self._migrate('''\
            from hdmf.utils import docval, getargs

            @docval({'name': 'data', 'type': ('array_data', 'data'), 'doc': 'd', 'shape': (None, 3)},
                    {'name': 'mode', 'type': str, 'doc': 'm', 'enum': ['r', 'w'], 'default': 'r'},
                    is_method=False)
            def func(**kwargs):
                data, mode = getargs('data', 'mode', kwargs)
                return data
            ''')
        self.assertEqual(n_converted, 1)
        self.assertIn('data: Shaped[ArrayData | AnyData, (None, 3)]', migrated)
        self.assertIn("mode: Literal['r', 'w'] = 'r'", migrated)
        self.assertIn('from typing import Literal', migrated)
        compile(migrated, '<migrated>', 'exec')

    def test_mutable_default_flagged(self):
        migrated, n_converted, _ = self._migrate('''\
            from hdmf.utils import docval, getargs

            @docval({'name': 'tags', 'type': list, 'doc': 't', 'default': list()}, is_method=False)
            def func(**kwargs):
                tags = getargs('tags', kwargs)
                return tags
            ''')
        self.assertEqual(n_converted, 1)
        self.assertIn('tags: list | None = None', migrated)
        self.assertIn('TODO(migrate)', migrated)
        self.assertIn('None-guard', migrated)

    def test_splice_decorator_skipped_with_todo(self):
        migrated, n_converted, n_skipped = self._migrate('''\
            from hdmf.utils import docval, get_docval, getargs

            @docval({'name': 'a', 'type': str, 'doc': 'a'}, is_method=False)
            def parent(**kwargs):
                return getargs('a', kwargs)

            @docval(*get_docval(parent), {'name': 'b', 'type': str, 'doc': 'b'}, is_method=False)
            def child(**kwargs):
                return getargs('a', 'b', kwargs)
            ''')
        self.assertEqual(n_converted, 1)
        self.assertEqual(n_skipped, 1)
        self.assertIn('convert by hand', migrated)

    def test_leftover_kwargs_flagged(self):
        migrated, n_converted, _ = self._migrate('''\
            from hdmf.utils import docval, popargs

            class Sub(Base):

                @docval({'name': 'a', 'type': str, 'doc': 'a'},
                        {'name': 'b', 'type': str, 'doc': 'b'})
                def __init__(self, **kwargs):
                    a = popargs('a', kwargs)
                    super().__init__(**kwargs)
                    self.a = a
            ''')
        self.assertEqual(n_converted, 1)
        self.assertIn('body still references `kwargs`', migrated)
