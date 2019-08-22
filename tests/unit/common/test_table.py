from hdmf.common import DynamicTable

from . import base

import pandas as pd


class TestDynamicTable(base.TestMapRoundTrip):

    def setUpContainer(self):
        # this will get ignored
        table = DynamicTable('table0', 'an example table')
        table.add_column('foo', 'an int column')
        table.add_column('bar', 'a float column')
        table.add_column('baz', 'a string column')
        table.add_column('qux', 'a boolean column')
        table.add_row(foo=27, bar=28.0, baz="cat", qux=True)
        table.add_row(foo=37, bar=38.0, baz="dog", qux=False)
        return table

    def test_from_dataframe(self):
        # this will get ignored
        expected = DynamicTable('test_table', 'the expected table')
        expected.add_column('a', '2d column')
        expected.add_column('b', '1d column')
        expected.add_row(a=[1, 2, 3], b='4')
        expected.add_row(a=[1, 2, 3], b='5')
        expected.add_row(a=[1, 2, 3], b='6')

        coldesc = {'a': '2d column', 'b': '1d column'}

        received = DynamicTable.from_dataframe(pd.DataFrame({
                'a': [[1, 2, 3],
                      [1, 2, 3],
                      [1, 2, 3]],
                'b': ['4', '5', '6']
            }), 'test_table', table_description='the expected table', column_descriptions=coldesc)
        self.assertContainerEqual(expected, received)
