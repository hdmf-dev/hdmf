import unittest2 as unittest

from hdmf.common import DynamicTable, VectorData, ElementIdentifiers, DynamicTableRegion

from . import base

import pandas as pd
import numpy as np


class TestDynamicTable(unittest.TestCase):

    def setUp(self):
        self.spec = [
            {'name': 'foo', 'description': 'foo column'},
            {'name': 'bar', 'description': 'bar column'},
            {'name': 'baz', 'description': 'baz column'},
        ]
        self.data = [
            [1, 2, 3, 4, 5],
            [10.0, 20.0, 30.0, 40.0, 50.0],
            ['cat', 'dog', 'bird', 'fish', 'lizard']
        ]

    def with_table_columns(self):
        cols = [VectorData(**d) for d in self.spec]
        table = DynamicTable("with_table_columns", 'a test table', columns=cols)
        return table

    def with_columns_and_data(self):
        columns = [
            VectorData(name=s['name'], description=s['description'], data=d)
            for s, d in zip(self.spec, self.data)
        ]
        return DynamicTable("with_columns_and_data", 'a test table', columns=columns)

    def with_spec(self):
        table = DynamicTable("with_spec", 'a test table', columns=self.spec)
        return table

    def check_empty_table(self, table):
        self.assertIsInstance(table.columns[0], VectorData)
        self.assertEqual(len(table.columns), 3)
        self.assertEqual(table.colnames, ('foo', 'bar', 'baz'))

    def test_constructor_table_columns(self):
        table = self.with_table_columns()
        self.assertEqual(table.name, 'with_table_columns')
        self.check_empty_table(table)

    def test_constructor_spec(self):
        table = self.with_spec()
        self.assertEqual(table.name, 'with_spec')
        self.check_empty_table(table)

    def check_table(self, table):
        self.assertEqual(len(table), 5)
        self.assertEqual(table.columns[0].data, [1, 2, 3, 4, 5])
        self.assertEqual(table.columns[1].data, [10.0, 20.0, 30.0, 40.0, 50.0])
        self.assertEqual(table.columns[2].data, ['cat', 'dog', 'bird', 'fish', 'lizard'])
        self.assertEqual(table.id.data, [0, 1, 2, 3, 4])

    def test_constructor_ids_default(self):
        columns = [VectorData(name=s['name'], description=s['description'], data=d)
                   for s, d in zip(self.spec, self.data)]
        table = DynamicTable("with_spec", 'a test table', columns=columns)
        self.check_table(table)

    def test_constructor_ids(self):
        columns = [VectorData(name=s['name'], description=s['description'], data=d)
                   for s, d in zip(self.spec, self.data)]
        table = DynamicTable("with_columns", 'a test table', id=[0, 1, 2, 3, 4], columns=columns)
        self.check_table(table)

    def test_constructor_ElementIdentifier_ids(self):
        columns = [VectorData(name=s['name'], description=s['description'], data=d)
                   for s, d in zip(self.spec, self.data)]
        ids = ElementIdentifiers('ids', [0, 1, 2, 3, 4])
        table = DynamicTable("with_columns", 'a test table', id=ids, columns=columns)
        self.check_table(table)

    def test_constructor_ids_bad_ids(self):
        columns = [VectorData(name=s['name'], description=s['description'], data=d)
                   for s, d in zip(self.spec, self.data)]
        msg = "must provide same number of ids as length of columns"
        with self.assertRaisesRegex(ValueError, msg):
            DynamicTable("with_columns", 'a test table', id=[0, 1], columns=columns)

    def add_rows(self, table):
        table.add_row({'foo': 1, 'bar': 10.0, 'baz': 'cat'})
        table.add_row({'foo': 2, 'bar': 20.0, 'baz': 'dog'})
        table.add_row({'foo': 3, 'bar': 30.0, 'baz': 'bird'})
        table.add_row({'foo': 4, 'bar': 40.0, 'baz': 'fish'})
        table.add_row({'foo': 5, 'bar': 50.0, 'baz': 'lizard'})

    def test_add_row(self):
        table = self.with_spec()
        self.add_rows(table)
        self.check_table(table)

    def test_get_item(self):
        table = self.with_spec()
        self.add_rows(table)
        self.check_table(table)

    def test_add_column(self):
        table = self.with_spec()
        table.add_column(name='qux', description='qux column')
        self.assertEqual(table.colnames, ('foo', 'bar', 'baz', 'qux'))

    def test_getitem_row_num(self):
        table = self.with_spec()
        self.add_rows(table)
        row = table[2]
        self.assertEqual(row[0], 2)
        self.assertEqual(row[1], 3)
        self.assertEqual(row[2], 30.0)
        self.assertEqual(row[3], 'bird')

    def test_getitem_column(self):
        table = self.with_spec()
        self.add_rows(table)
        col = table['bar']
        self.assertEqual(col[0], 10.0)
        self.assertEqual(col[1], 20.0)
        self.assertEqual(col[2], 30.0)
        self.assertEqual(col[3], 40.0)
        self.assertEqual(col[4], 50.0)

    def test_getitem_list_idx(self):
        table = self.with_spec()
        self.add_rows(table)
        row = table[[0, 2, 4]]
        self.assertEqual(len(row), 3)
        self.assertEqual(row[0], (0, 1, 10.0, 'cat'))
        self.assertEqual(row[1], (2, 3, 30.0, 'bird'))
        self.assertEqual(row[2], (4, 5, 50.0, 'lizard'))

    def test_getitem_point_idx_colname(self):
        table = self.with_spec()
        self.add_rows(table)
        val = table[2, 'bar']
        self.assertEqual(val, 30.0)

    def test_getitem_point_idx_colidx(self):
        table = self.with_spec()
        self.add_rows(table)
        val = table[2, 2]
        self.assertEqual(val, 30.0)

    def test_pandas_roundtrip(self):
        df = pd.DataFrame({
            'a': [1, 2, 3, 4],
            'b': ['a', 'b', 'c', '4']
        }, index=pd.Index(name='an_index', data=[2, 4, 6, 8]))

        table = DynamicTable.from_dataframe(df, 'foo')
        obtained = table.to_dataframe()

        assert df.equals(obtained)

    def test_to_dataframe(self):
        table = self.with_columns_and_data()
        expected_df = pd.DataFrame({
            'foo': [1, 2, 3, 4, 5],
            'bar': [10.0, 20.0, 30.0, 40.0, 50.0],
            'baz': ['cat', 'dog', 'bird', 'fish', 'lizard']
        })
        obtained_df = table.to_dataframe()
        assert expected_df.equals(obtained_df)

    def test_from_dataframe(self):
        df = pd.DataFrame({
            'foo': [1, 2, 3, 4, 5],
            'bar': [10.0, 20.0, 30.0, 40.0, 50.0],
            'baz': ['cat', 'dog', 'bird', 'fish', 'lizard']
        }).loc[:, ('foo', 'bar', 'baz')]

        obtained_table = DynamicTable.from_dataframe(df, 'test')
        self.check_table(obtained_table)

    def test_missing_columns(self):
        table = self.with_spec()

        with self.assertRaises(ValueError):
            table.add_row({'bar': 60.0, 'foo': [6]}, None)

    def test_extra_columns(self):
        table = self.with_spec()

        with self.assertRaises(ValueError):
            table.add_row({'bar': 60.0, 'foo': 6, 'baz': 'oryx', 'qax': -1}, None)

    def test_indexed_dynamic_table_region(self):
        table = self.with_columns_and_data()

        dynamic_table_region = DynamicTableRegion('dtr', [0, 1, 1], 'desc', table=table)
        fetch_ids = [x[1] for x in dynamic_table_region[:3]]
        self.assertEqual(fetch_ids, [1, 2, 2])

    def test_dynamic_table_iteration(self):
        table = self.with_columns_and_data()

        dynamic_table_region = DynamicTableRegion('dtr', [0, 1, 2, 3, 4], 'desc', table=table)
        for ii, item in enumerate(dynamic_table_region):
            self.assertEqual(table[ii], item)

    def test_nd_array_to_df(self):
        data = np.array([[1, 1, 1], [2, 2, 2], [3, 3, 3]])
        col = VectorData(name='name', description='desc', data=data)
        df = DynamicTable('test', 'desc', np.arange(3, dtype='int'), (col, )).to_dataframe()
        df2 = pd.DataFrame({'name': [x for x in data]},
                           index=pd.Index(name='id', data=[0, 1, 2]))
        pd.testing.assert_frame_equal(df, df2)


class TestDynamicTableRoundTrip(base.TestMapRoundTrip):

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
