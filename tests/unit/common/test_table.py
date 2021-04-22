from collections import OrderedDict
import h5py
import numpy as np
import pandas as pd
import unittest


from hdmf import Container
from hdmf.backends.hdf5 import H5DataIO, HDF5IO
from hdmf.common import (DynamicTable, VectorData, VectorIndex, ElementIdentifiers, EnumData,
                         DynamicTableRegion, get_manager, SimpleMultiContainer)
from hdmf.testing import TestCase, H5RoundTripMixin, remove_test_file


class TestDynamicTable(TestCase):

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
        self.assertIsInstance(table.columns, tuple)
        self.assertIsInstance(table.columns[0], VectorData)
        self.assertEqual(len(table.columns), 3)
        self.assertTupleEqual(table.colnames, ('foo', 'bar', 'baz'))

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
        self.assertTrue(hasattr(table, 'baz'))

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
        with self.assertRaisesWith(ValueError, msg):
            DynamicTable("with_columns", 'a test table', id=[0, 1], columns=columns)

    def test_constructor_bad_columns(self):
        columns = ['bad_column']
        msg = "'columns' must be a list of dict, VectorData, DynamicTableRegion, or VectorIndex"
        with self.assertRaisesWith(ValueError, msg):
            DynamicTable("with_columns", 'a test table', columns=columns)

    def test_constructor_unequal_length_columns(self):
        columns = [VectorData(name='col1', description='desc', data=[1, 2, 3]),
                   VectorData(name='col2', description='desc', data=[1, 2])]
        msg = "columns must be the same length"
        with self.assertRaisesWith(ValueError, msg):
            DynamicTable("with_columns", 'a test table', columns=columns)

    def test_constructor_colnames(self):
        """Test that passing colnames correctly sets the order of the columns."""
        cols = [VectorData(**d) for d in self.spec]
        table = DynamicTable("with_columns", 'a test table', columns=cols, colnames=['baz', 'bar', 'foo'])
        self.assertTupleEqual(table.columns, tuple(cols[::-1]))

    def test_constructor_colnames_no_columns(self):
        """Test that passing colnames without columns raises an error."""
        msg = "Must supply 'columns' if specifying 'colnames'"
        with self.assertRaisesWith(ValueError, msg):
            DynamicTable("with_columns", 'a test table',  colnames=['baz', 'bar', 'foo'])

    def test_constructor_colnames_vectorindex(self):
        """Test that passing colnames with a VectorIndex column puts the index in the right location in columns."""
        cols = [VectorData(**d) for d in self.spec]
        ind = VectorIndex(name='foo_index', data=list(), target=cols[0])
        cols.append(ind)
        table = DynamicTable("with_columns", 'a test table', columns=cols, colnames=['baz', 'bar', 'foo'])
        self.assertTupleEqual(table.columns, (cols[2], cols[1], ind, cols[0]))

    def test_constructor_colnames_vectorindex_rev(self):
        """Test that passing colnames with a VectorIndex column puts the index in the right location in columns."""
        cols = [VectorData(**d) for d in self.spec]
        ind = VectorIndex(name='foo_index', data=list(), target=cols[0])
        cols.insert(0, ind)  # put index before its target
        table = DynamicTable("with_columns", 'a test table', columns=cols, colnames=['baz', 'bar', 'foo'])
        self.assertTupleEqual(table.columns, (cols[3], cols[2], ind, cols[1]))

    def test_constructor_dup_index(self):
        """Test that passing two indices for the same column raises an error."""
        cols = [VectorData(**d) for d in self.spec]
        cols.append(VectorIndex(name='foo_index', data=list(), target=cols[0]))
        cols.append(VectorIndex(name='foo_index2', data=list(), target=cols[0]))
        msg = "'columns' contains index columns with the same target: ['foo', 'foo']"
        with self.assertRaisesWith(ValueError, msg):
            DynamicTable("with_columns", 'a test table', columns=cols)

    def test_constructor_index_missing_target(self):
        """Test that passing an index without its target raises an error."""
        cols = [VectorData(**d) for d in self.spec]
        missing_col = cols.pop(2)
        cols.append(VectorIndex(name='foo_index', data=list(), target=missing_col))
        msg = "Found VectorIndex 'foo_index' but not its target 'baz'"
        with self.assertRaisesWith(ValueError, msg):
            DynamicTable("with_columns", 'a test table', columns=cols)

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

    def test_get(self):
        table = self.with_spec()
        self.add_rows(table)
        self.assertIsInstance(table.get('foo'), VectorData)
        self.assertEqual(table.get('foo'), table['foo'])

    def test_get_not_found(self):
        table = self.with_spec()
        self.add_rows(table)
        self.assertIsNone(table.get('qux'))

    def test_get_not_found_default(self):
        table = self.with_spec()
        self.add_rows(table)
        self.assertEqual(table.get('qux', 1), 1)

    def test_get_item(self):
        table = self.with_spec()
        self.add_rows(table)
        self.check_table(table)

    def test_add_column(self):
        table = self.with_spec()
        table.add_column(name='qux', description='qux column')
        self.assertTupleEqual(table.colnames, ('foo', 'bar', 'baz', 'qux'))
        self.assertTrue(hasattr(table, 'qux'))

    def test_add_column_twice(self):
        table = self.with_spec()
        table.add_column(name='qux', description='qux column')

        msg = "column 'qux' already exists in DynamicTable 'with_spec'"
        with self.assertRaisesWith(ValueError, msg):
            table.add_column(name='qux', description='qux column')

    def test_add_column_vectorindex(self):
        table = self.with_spec()
        table.add_column(name='qux', description='qux column')
        ind = VectorIndex(name='bar', data=list(), target=table['qux'])

        msg = ("Passing a VectorIndex in for index may lead to unexpected behavior. This functionality will be "
               "deprecated in a future version of HDMF.")
        with self.assertWarnsWith(FutureWarning, msg):
            table.add_column(name='bad', description='bad column', index=ind)

    def test_add_column_multi_index(self):
        table = self.with_spec()
        table.add_column(name='qux', description='qux column', index=2)
        table.add_row(foo=5, bar=50.0, baz='lizard',
                      qux=[
                            [1, 2, 3],
                            [1, 2, 3, 4]
                      ])
        table.add_row(foo=5, bar=50.0, baz='lizard',
                      qux=[
                            [1, 2]
                      ]
                      )

    def test_auto_multi_index(self):

        class TestTable(DynamicTable):
            __columns__ = (dict(name='qux', description='qux column', index=2),)

        table = TestTable('table_name', 'table_description')
        table.add_row(qux=[
                          [1, 2, 3],
                          [1, 2, 3, 4]
                      ])
        table.add_row(qux=[
                          [1, 2]
                      ]
                      )

        expected = [
            [
                [1, 2, 3],
                [1, 2, 3, 4]
            ],
            [
                [1, 2]
            ]
        ]
        self.assertListEqual(table['qux'][:], expected)

    def test_getitem_row_num(self):
        table = self.with_spec()
        self.add_rows(table)
        row = table[2]
        self.assertTupleEqual(row.shape, (1, 3))
        self.assertTupleEqual(tuple(row.iloc[0]), (3, 30.0, 'bird'))

    def test_getitem_row_slice(self):
        table = self.with_spec()
        self.add_rows(table)
        rows = table[1:3]
        self.assertIsInstance(rows, pd.DataFrame)
        self.assertTupleEqual(rows.shape, (2, 3))
        self.assertTupleEqual(tuple(rows.iloc[1]), (3, 30.0, 'bird'))

    def test_getitem_row_slice_with_step(self):
        table = self.with_spec()
        self.add_rows(table)
        rows = table[0:5:2]
        self.assertIsInstance(rows, pd.DataFrame)
        self.assertTupleEqual(rows.shape, (3, 3))
        self.assertEqual(rows.iloc[2][0], 5)
        self.assertEqual(rows.iloc[2][1], 50.0)
        self.assertEqual(rows.iloc[2][2], 'lizard')

    def test_getitem_invalid_keytype(self):
        table = self.with_spec()
        self.add_rows(table)
        with self.assertRaises(KeyError):
            _ = table[0.1]

    def test_getitem_col_select_and_row_slice(self):
        table = self.with_spec()
        self.add_rows(table)
        col = table[1:3, 'bar']
        self.assertEqual(len(col), 2)
        self.assertEqual(col[0], 20.0)
        self.assertEqual(col[1], 30.0)

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
        self.assertTupleEqual(tuple(row.iloc[0]), (1, 10.0, 'cat'))
        self.assertTupleEqual(tuple(row.iloc[1]), (3, 30.0, 'bird'))
        self.assertTupleEqual(tuple(row.iloc[2]), (5, 50.0, 'lizard'))

    def test_getitem_point_idx_colname(self):
        table = self.with_spec()
        self.add_rows(table)
        val = table[2, 'bar']
        self.assertEqual(val, 30.0)

    def test_getitem_point_idx(self):
        table = self.with_spec()
        self.add_rows(table)
        row = table[2]
        self.assertTupleEqual(tuple(row.iloc[0]), (3, 30.0, 'bird'))

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
        self.assertTrue(df.equals(obtained))

    def test_to_dataframe(self):
        table = self.with_columns_and_data()
        data = OrderedDict()
        for name in table.colnames:
            if name == 'foo':
                data[name] = [1, 2, 3, 4, 5]
            elif name == 'bar':
                data[name] = [10.0, 20.0, 30.0, 40.0, 50.0]
            elif name == 'baz':
                data[name] = ['cat', 'dog', 'bird', 'fish', 'lizard']
        expected_df = pd.DataFrame(data)
        obtained_df = table.to_dataframe()
        self.assertTrue(expected_df.equals(obtained_df))

    def test_from_dataframe(self):
        df = pd.DataFrame({
            'foo': [1, 2, 3, 4, 5],
            'bar': [10.0, 20.0, 30.0, 40.0, 50.0],
            'baz': ['cat', 'dog', 'bird', 'fish', 'lizard']
        }).loc[:, ('foo', 'bar', 'baz')]

        obtained_table = DynamicTable.from_dataframe(df, 'test')
        self.check_table(obtained_table)

    def test_from_dataframe_eq(self):
        expected = DynamicTable('test_table', 'the expected table')
        expected.add_column('a', '2d column')
        expected.add_column('b', '1d column')
        expected.add_row(a=[1, 2, 3], b='4')
        expected.add_row(a=[1, 2, 3], b='5')
        expected.add_row(a=[1, 2, 3], b='6')

        df = pd.DataFrame({
            'a': [[1, 2, 3],
                  [1, 2, 3],
                  [1, 2, 3]],
            'b': ['4', '5', '6']
        })
        coldesc = {'a': '2d column', 'b': '1d column'}
        received = DynamicTable.from_dataframe(df,
                                               'test_table',
                                               table_description='the expected table',
                                               column_descriptions=coldesc)
        self.assertContainerEqual(expected, received, ignore_hdmf_attrs=True)

    def test_from_dataframe_dup_attr(self):
        """
        Test that when a DynamicTable is generated from a dataframe where one of the column names is an existing
        DynamicTable attribute (e.g., description), that the table can be created, the existing attribute is not
        altered, a warning is raised, and the column can still be accessed using the table[col_name] syntax.
        """
        df = pd.DataFrame({
            'parent': [1, 2, 3, 4, 5],
            'name': [10.0, 20.0, 30.0, 40.0, 50.0],
            'description': ['cat', 'dog', 'bird', 'fish', 'lizard']
        })

        # technically there are three separate warnings but just catch one here
        msg1 = ("An attribute 'parent' already exists on DynamicTable 'test' so this column cannot be accessed "
                "as an attribute, e.g., table.parent; it can only be accessed using other methods, e.g., "
                "table['parent'].")
        with self.assertWarnsWith(UserWarning, msg1):
            table = DynamicTable.from_dataframe(df, 'test')
        self.assertEqual(table.name, 'test')
        self.assertEqual(table.description, '')
        self.assertIsNone(table.parent)
        self.assertEqual(table['name'].name, 'name')
        self.assertEqual(table['description'].name, 'description')
        self.assertEqual(table['parent'].name, 'parent')

    def test_missing_columns(self):
        table = self.with_spec()
        with self.assertRaises(ValueError):
            table.add_row({'bar': 60.0, 'foo': [6]}, None)

    def test_enforce_unique_id_error(self):
        table = self.with_spec()
        table.add_row(id=10, data={'foo': 1, 'bar': 10.0, 'baz': 'cat'}, enforce_unique_id=True)
        with self.assertRaises(ValueError):
            table.add_row(id=10, data={'foo': 1, 'bar': 10.0, 'baz': 'cat'}, enforce_unique_id=True)

    def test_not_enforce_unique_id_error(self):
        table = self.with_spec()
        table.add_row(id=10, data={'foo': 1, 'bar': 10.0, 'baz': 'cat'}, enforce_unique_id=False)
        try:
            table.add_row(id=10, data={'foo': 1, 'bar': 10.0, 'baz': 'cat'}, enforce_unique_id=False)
        except ValueError as e:
            self.fail("add row with non unique id raised error %s" % str(e))

    def test_bad_id_type_error(self):
        table = self.with_spec()
        with self.assertRaises(TypeError):
            table.add_row(id=10.1, data={'foo': 1, 'bar': 10.0, 'baz': 'cat'}, enforce_unique_id=True)
        with self.assertRaises(TypeError):
            table.add_row(id='str', data={'foo': 1, 'bar': 10.0, 'baz': 'cat'}, enforce_unique_id=True)

    def test_extra_columns(self):
        table = self.with_spec()

        with self.assertRaises(ValueError):
            table.add_row({'bar': 60.0, 'foo': 6, 'baz': 'oryx', 'qax': -1}, None)

    def test_nd_array_to_df(self):
        data = np.array([[1, 1, 1], [2, 2, 2], [3, 3, 3]])
        col = VectorData(name='data', description='desc', data=data)
        df = DynamicTable('test', 'desc', np.arange(3, dtype='int'), (col, )).to_dataframe()
        df2 = pd.DataFrame({'data': [x for x in data]},
                           index=pd.Index(name='id', data=[0, 1, 2]))
        pd.testing.assert_frame_equal(df, df2)

    def test_id_search(self):
        table = self.with_spec()
        data = [{'foo': 1, 'bar': 10.0, 'baz': 'cat'},
                {'foo': 2, 'bar': 20.0, 'baz': 'dog'},
                {'foo': 3, 'bar': 30.0, 'baz': 'bird'},    # id=2
                {'foo': 4, 'bar': 40.0, 'baz': 'fish'},
                {'foo': 5, 'bar': 50.0, 'baz': 'lizard'}   # id=4
                ]
        for i in data:
            table.add_row(i)
        res = table[table.id == [2, 4]]
        self.assertEqual(len(res), 2)
        self.assertTupleEqual(tuple(res.iloc[0]), (3, 30.0, 'bird'))
        self.assertTupleEqual(tuple(res.iloc[1]), (5, 50.0, 'lizard'))

    def test_repr(self):
        table = self.with_spec()
        expected = """with_spec hdmf.common.table.DynamicTable at 0x%d
Fields:
  colnames: ['foo' 'bar' 'baz']
  columns: (
    foo <class 'hdmf.common.table.VectorData'>,
    bar <class 'hdmf.common.table.VectorData'>,
    baz <class 'hdmf.common.table.VectorData'>
  )
  description: a test table
"""
        expected = expected % id(table)
        self.assertEqual(str(table), expected)

    def test_add_column_existing_attr(self):
        table = self.with_table_columns()
        attrs = ['name', 'description', 'parent', 'id', 'fields']  # just a few
        for attr in attrs:
            with self.subTest(attr=attr):
                msg = ("An attribute '%s' already exists on DynamicTable 'with_table_columns' so this column cannot be "
                       "accessed as an attribute, e.g., table.%s; it can only be accessed using other methods, "
                       "e.g., table['%s']." % (attr, attr, attr))
                with self.assertWarnsWith(UserWarning, msg):
                    table.add_column(name=attr, description='')

    def test_init_columns_existing_attr(self):
        attrs = ['name', 'description', 'parent', 'id', 'fields']  # just a few
        for attr in attrs:
            with self.subTest(attr=attr):
                cols = [VectorData(name=attr, description='')]
                msg = ("An attribute '%s' already exists on DynamicTable 'test_table' so this column cannot be "
                       "accessed as an attribute, e.g., table.%s; it can only be accessed using other methods, "
                       "e.g., table['%s']." % (attr, attr, attr))
                with self.assertWarnsWith(UserWarning, msg):
                    DynamicTable("test_table", 'a test table', columns=cols)

    def test_colnames_none(self):
        table = DynamicTable('table0', 'an example table')
        self.assertTupleEqual(table.colnames, tuple())
        self.assertTupleEqual(table.columns, tuple())

    def test_index_out_of_bounds(self):
        table = self.with_columns_and_data()
        msg = "Row index out of range for DynamicTable 'with_columns_and_data' (length 5)."
        with self.assertRaisesWith(IndexError, msg):
            table[5]

    def test_multidim_col(self):
        multidim_data = [
            [[1, 2], [3, 4], [5, 6]],
            ((1, 2), (3, 4), (5, 6)),
            [(1, 'a', True), (2, 'b', False), (3, 'c', True)],
        ]
        columns = [
            VectorData(name=s['name'], description=s['description'], data=d)
            for s, d in zip(self.spec, multidim_data)
        ]
        table = DynamicTable("with_columns_and_data", 'a test table', columns=columns)
        df = table.to_dataframe()
        df2 = pd.DataFrame({'foo': multidim_data[0],
                            'bar': multidim_data[1],
                            'baz': multidim_data[2]},
                           index=pd.Index(name='id', data=[0, 1, 2]))
        pd.testing.assert_frame_equal(df, df2)

        df3 = pd.DataFrame({'foo': [multidim_data[0][0]],
                            'bar': [multidim_data[1][0]],
                            'baz': [multidim_data[2][0]]},
                           index=pd.Index(name='id', data=[0]))
        pd.testing.assert_frame_equal(table.get(0), df3)

    def test_multidim_col_one_elt_list(self):
        data = [[1, 2]]
        col = VectorData(name='data', description='desc', data=data)
        table = DynamicTable('test', 'desc', columns=(col, ))
        df = table.to_dataframe()
        df2 = pd.DataFrame({'data': [x for x in data]},
                           index=pd.Index(name='id', data=[0]))
        pd.testing.assert_frame_equal(df, df2)
        pd.testing.assert_frame_equal(table.get(0), df2)

    def test_multidim_col_one_elt_tuple(self):
        data = [(1, 2)]
        col = VectorData(name='data', description='desc', data=data)
        table = DynamicTable('test', 'desc', columns=(col, ))
        df = table.to_dataframe()
        df2 = pd.DataFrame({'data': [x for x in data]},
                           index=pd.Index(name='id', data=[0]))
        pd.testing.assert_frame_equal(df, df2)
        pd.testing.assert_frame_equal(table.get(0), df2)

    def test_eq(self):
        columns = [
            VectorData(name=s['name'], description=s['description'], data=d)
            for s, d in zip(self.spec, self.data)
        ]
        test_table = DynamicTable("with_columns_and_data", 'a test table', columns=columns)

        table = self.with_columns_and_data()
        self.assertTrue(table == test_table)

    def test_eq_from_df(self):
        df = pd.DataFrame({
            'foo': [1, 2, 3, 4, 5],
            'bar': [10.0, 20.0, 30.0, 40.0, 50.0],
            'baz': ['cat', 'dog', 'bird', 'fish', 'lizard']
        }).loc[:, ('foo', 'bar', 'baz')]

        test_table = DynamicTable.from_dataframe(df, 'with_columns_and_data', table_description='a test table')
        table = self.with_columns_and_data()
        self.assertTrue(table == test_table)

    def test_eq_diff_missing_col(self):
        columns = [
            VectorData(name=s['name'], description=s['description'], data=d)
            for s, d in zip(self.spec, self.data)
        ]
        del columns[-1]
        test_table = DynamicTable("with_columns_and_data", 'a test table', columns=columns)

        table = self.with_columns_and_data()
        self.assertFalse(table == test_table)

    def test_eq_diff_name(self):
        columns = [
            VectorData(name=s['name'], description=s['description'], data=d)
            for s, d in zip(self.spec, self.data)
        ]
        test_table = DynamicTable("wrong name", 'a test table', columns=columns)

        table = self.with_columns_and_data()
        self.assertFalse(table == test_table)

    def test_eq_diff_desc(self):
        columns = [
            VectorData(name=s['name'], description=s['description'], data=d)
            for s, d in zip(self.spec, self.data)
        ]
        test_table = DynamicTable("with_columns_and_data", 'wrong description', columns=columns)

        table = self.with_columns_and_data()
        self.assertFalse(table == test_table)

    def test_eq_bad_type(self):
        container = Container('test_container')
        table = self.with_columns_and_data()
        self.assertFalse(table == container)


class TestDynamicTableRoundTrip(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        table = DynamicTable('table0', 'an example table')
        table.add_column('foo', 'an int column')
        table.add_column('bar', 'a float column')
        table.add_column('baz', 'a string column')
        table.add_column('qux', 'a boolean column')
        table.add_column('quux', 'an enum column', enum=True)
        table.add_row(foo=27, bar=28.0, baz="cat", qux=True, quux='a')
        table.add_row(foo=37, bar=38.0, baz="dog", qux=False, quux='b')
        return table

    def test_index_out_of_bounds(self):
        table = self.roundtripContainer()
        msg = "Row index 5 out of range for DynamicTable 'root' (length 2)."
        with self.assertRaisesWith(IndexError, msg):
            table[5]


class TestEmptyDynamicTableRoundTrip(H5RoundTripMixin, TestCase):
    """Test roundtripping a DynamicTable with no rows and no columns."""

    def setUpContainer(self):
        table = DynamicTable('table0', 'an example table')
        return table


class TestDynamicTableRegion(TestCase):

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

    def with_columns_and_data(self):
        columns = [
            VectorData(name=s['name'], description=s['description'], data=d)
            for s, d in zip(self.spec, self.data)
        ]
        return DynamicTable("with_columns_and_data", 'a test table', columns=columns)

    def test_indexed_dynamic_table_region(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion('dtr', [1, 2, 2], 'desc', table=table)
        fetch_ids = dynamic_table_region[:3].index.values
        self.assertListEqual(fetch_ids.tolist(), [1, 2, 2])

    def test_dynamic_table_region_iteration(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion('dtr', [0, 1, 2, 3, 4], 'desc', table=table)
        for ii, item in enumerate(dynamic_table_region):
            self.assertTrue(table[ii].equals(item))

    def test_dynamic_table_region_shape(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion('dtr', [0, 1, 2, 3, 4], 'desc', table=table)
        self.assertTupleEqual(dynamic_table_region.shape, (5, 3))

    def test_dynamic_table_region_to_dataframe(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion('dtr', [0, 1, 2, 2], 'desc', table=table)
        res = dynamic_table_region.to_dataframe()
        self.assertListEqual(res.index.tolist(), [0, 1, 2, 2])
        self.assertListEqual(res['foo'].tolist(), [1, 2, 3, 3])
        self.assertListEqual(res['bar'].tolist(), [10.0, 20.0, 30.0, 30.0])
        self.assertListEqual(res['baz'].tolist(), ['cat', 'dog', 'bird', 'bird'])

    def test_dynamic_table_region_to_dataframe_exclude_cols(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion('dtr', [0, 1, 2, 2], 'desc', table=table)
        res = dynamic_table_region.to_dataframe(exclude={'baz', 'foo'})
        self.assertListEqual(res.index.tolist(), [0, 1, 2, 2])
        self.assertEqual(len(res.columns), 1)
        self.assertListEqual(res['bar'].tolist(), [10.0, 20.0, 30.0, 30.0])

    def test_dynamic_table_region_getitem_slice(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion('dtr', [0, 1, 2, 2], 'desc', table=table)
        res = dynamic_table_region[1:3]
        self.assertListEqual(res.index.tolist(), [1, 2])
        self.assertListEqual(res['foo'].tolist(), [2, 3])
        self.assertListEqual(res['bar'].tolist(), [20.0, 30.0])
        self.assertListEqual(res['baz'].tolist(), ['dog', 'bird'])

    def test_dynamic_table_region_getitem_single_row_by_index(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion('dtr', [0, 1, 2, 2], 'desc', table=table)
        res = dynamic_table_region[2]
        self.assertListEqual(res.index.tolist(), [2, ])
        self.assertListEqual(res['foo'].tolist(), [3, ])
        self.assertListEqual(res['bar'].tolist(), [30.0, ])
        self.assertListEqual(res['baz'].tolist(), ['bird', ])

    def test_dynamic_table_region_getitem_single_cell(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion('dtr', [0, 1, 2, 2], 'desc', table=table)
        res = dynamic_table_region[2, 'foo']
        self.assertEqual(res, 3)
        res = dynamic_table_region[1, 'baz']
        self.assertEqual(res, 'dog')

    def test_dynamic_table_region_getitem_slice_of_column(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion('dtr', [0, 1, 2, 2], 'desc', table=table)
        res = dynamic_table_region[0:3, 'foo']
        self.assertListEqual(res, [1, 2, 3])
        res = dynamic_table_region[1:3, 'baz']
        self.assertListEqual(res, ['dog', 'bird'])

    def test_dynamic_table_region_getitem_bad_index(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion('dtr', [0, 1, 2, 2], 'desc', table=table)
        with self.assertRaises(ValueError):
            _ = dynamic_table_region['bad index']

    def test_dynamic_table_region_table_prop(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion('dtr', [0, 1, 2, 2], 'desc', table=table)
        self.assertEqual(table, dynamic_table_region.table)

    def test_dynamic_table_region_set_table_prop(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion('dtr', [0, 1, 2, 2], 'desc')
        dynamic_table_region.table = table
        self.assertEqual(table, dynamic_table_region.table)

    def test_dynamic_table_region_set_table_prop_to_none(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion('dtr', [0, 1, 2, 2], 'desc', table=table)
        try:
            dynamic_table_region.table = None
        except AttributeError:
            self.fail("DynamicTableRegion table setter raised AttributeError unexpectedly!")

    @unittest.skip('we no longer check data contents for performance reasons')
    def test_dynamic_table_region_set_with_bad_data(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion('dtr', [5, 1], 'desc')   # index 5 is out of range
        with self.assertRaises(IndexError):
            dynamic_table_region.table = table
        self.assertIsNone(dynamic_table_region.table)

    def test_repr(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion('dtr', [1, 2, 2], 'desc', table=table)
        expected = """dtr hdmf.common.table.DynamicTableRegion at 0x%d
    Target table: with_columns_and_data hdmf.common.table.DynamicTable at 0x%d
"""
        expected = expected % (id(dynamic_table_region), id(table))
        self.assertEqual(str(dynamic_table_region), expected)


class DynamicTableRegionRoundTrip(H5RoundTripMixin, TestCase):

    def make_tables(self):
        self.spec2 = [
            {'name': 'qux', 'description': 'qux column'},
            {'name': 'quz', 'description': 'quz column'},
        ]
        self.data2 = [
            ['qux_1', 'qux_2'],
            ['quz_1', 'quz_2'],
        ]

        target_columns = [
            VectorData(name=s['name'], description=s['description'], data=d)
            for s, d in zip(self.spec2, self.data2)
        ]
        target_table = DynamicTable("target_table", 'example table to target with a DynamicTableRegion',
                                    columns=target_columns)

        self.spec1 = [
            {'name': 'foo', 'description': 'foo column'},
            {'name': 'bar', 'description': 'bar column'},
            {'name': 'baz', 'description': 'baz column'},
            {'name': 'dtr', 'description': 'DTR'},
        ]
        self.data1 = [
            [1, 2, 3, 4, 5],
            [10.0, 20.0, 30.0, 40.0, 50.0],
            ['cat', 'dog', 'bird', 'fish', 'lizard']
        ]
        columns = [
            VectorData(name=s['name'], description=s['description'], data=d)
            for s, d in zip(self.spec1, self.data1)
        ]
        columns.append(DynamicTableRegion(name='dtr', description='example DynamicTableRegion',
                                          data=[0, 1, 1, 0, 1], table=target_table))
        table = DynamicTable("table_with_dtr", 'a test table that has a DynamicTableRegion', columns=columns)
        return table, target_table

    def setUp(self):
        self.table, self.target_table = self.make_tables()
        super().setUp()

    def setUpContainer(self):
        multi_container = SimpleMultiContainer('multi', [self.table, self.target_table])
        return multi_container

    def _get(self, arg):
        mc = self.roundtripContainer()
        table = mc.containers['table_with_dtr']
        return table.get(arg)

    def _get_nodf(self, arg):
        mc = self.roundtripContainer()
        table = mc.containers['table_with_dtr']
        return table.get(arg, df=False)

    def _getitem(self, arg):
        mc = self.roundtripContainer()
        table = mc.containers['table_with_dtr']
        return table[arg]

    def test_getitem_oor(self):
        msg = 'Row index 12 out of range for DynamicTable \'table_with_dtr\' (length 5).'
        with self.assertRaisesWith(IndexError, msg):
            self._getitem(12)

    def test_getitem_badcol(self):
        with self.assertRaisesWith(KeyError, '\'boo\''):
            self._getitem('boo')

    def _assert_two_elem_df(self, rec):
        columns = ['foo', 'bar', 'baz', 'dtr_id', 'dtr_qux', 'dtr_quz']
        data = [[1, 10.0, 'cat', 0, 'qux_1', 'quz_1'],
                [2, 20.0, 'dog', 1, 'qux_2', 'quz_2']]
        exp = pd.DataFrame(data=data, columns=columns, index=pd.Series(name='id', data=[0, 1]))
        pd.testing.assert_frame_equal(rec, exp, check_dtype=False)

    def _assert_one_elem_df(self, rec):
        columns = ['foo', 'bar', 'baz', 'dtr_id', 'dtr_qux', 'dtr_quz']
        data = [[1, 10.0, 'cat', 0, 'qux_1', 'quz_1']]
        exp = pd.DataFrame(data=data, columns=columns, index=pd.Series(name='id', data=[0]))
        pd.testing.assert_frame_equal(rec, exp, check_dtype=False)

    #####################
    # tests DynamicTableRegion.__getitem__
    def test_getitem_int(self):
        rec = self._getitem(0)
        self._assert_one_elem_df(rec)

    def test_getitem_list(self):
        rec = self._getitem([0, 1])
        self._assert_two_elem_df(rec)

    def test_getitem_slice(self):
        rec = self._getitem(slice(0, 2, None))
        self._assert_two_elem_df(rec)

    #####################
    # tests DynamicTableRegion.get, return a DataFrame
    def test_get_int(self):
        rec = self._get(0)
        self._assert_one_elem_df(rec)

    def test_get_list(self):
        rec = self._get([0, 1])
        self._assert_two_elem_df(rec)

    def test_get_slice(self):
        rec = self._get(slice(0, 2, None))
        self._assert_two_elem_df(rec)

    #####################
    # tests DynamicTableRegion.get, DO NOT return a DataFrame
    def test_get_nodf_int(self):
        rec = self._get_nodf(0)
        exp = [0, 1, 10.0, 'cat', [0, 'qux_1', 'quz_1']]
        self.assertListEqual(rec, exp)

    def _assert_list_of_ndarray_equal(self, l1, l2):
        """
        This is a helper function for test_get_nodf_list and test_get_nodf_slice.
        It compares ndarrays from a list of ndarrays
        """
        for a1, a2 in zip(l1, l2):
            if isinstance(a1, list):
                self._assert_list_of_ndarray_equal(a1, a2)
            else:
                np.testing.assert_array_equal(a1, a2)

    def test_get_nodf_list(self):
        rec = self._get_nodf([0, 1])
        exp = [np.array([0, 1]), np.array([1, 2]), np.array([10.0, 20.0]), np.array(['cat', 'dog']),
               [np.array([0, 1]), np.array(['qux_1', 'qux_2']), np.array(['quz_1', 'quz_2'])]]
        self._assert_list_of_ndarray_equal(exp, rec)

    def test_get_nodf_slice(self):
        rec = self._get_nodf(slice(0, 2, None))
        exp = [np.array([0, 1]), np.array([1, 2]), np.array([10.0, 20.0]), np.array(['cat', 'dog']),
               [np.array([0, 1]), np.array(['qux_1', 'qux_2']), np.array(['quz_1', 'quz_2'])]]
        self._assert_list_of_ndarray_equal(exp, rec)


class TestElementIdentifiers(TestCase):

    def test_identifier_search_single_list(self):
        e = ElementIdentifiers('ids', [0, 1, 2, 3, 4])
        a = (e == [1])
        np.testing.assert_array_equal(a, [1])

    def test_identifier_search_single_int(self):
        e = ElementIdentifiers('ids', [0, 1, 2, 3, 4])
        a = (e == 2)
        np.testing.assert_array_equal(a, [2])

    def test_identifier_search_single_list_not_found(self):
        e = ElementIdentifiers('ids', [0, 1, 2, 3, 4])
        a = (e == [10])
        np.testing.assert_array_equal(a, [])

    def test_identifier_search_single_int_not_found(self):
        e = ElementIdentifiers('ids', [0, 1, 2, 3, 4])
        a = (e == 10)
        np.testing.assert_array_equal(a, [])

    def test_identifier_search_single_list_all_match(self):
        e = ElementIdentifiers('ids', [0, 1, 2, 3, 4])
        a = (e == [1, 2, 3])
        np.testing.assert_array_equal(a, [1, 2, 3])

    def test_identifier_search_single_list_partial_match(self):
        e = ElementIdentifiers('ids', [0, 1, 2, 3, 4])
        a = (e == [1, 2, 10])
        np.testing.assert_array_equal(a, [1, 2])
        a = (e == [-1, 2, 10])
        np.testing.assert_array_equal(a, [2, ])

    def test_identifier_search_with_element_identifier(self):
        e = ElementIdentifiers('ids', [0, 1, 2, 3, 4])
        a = (e == ElementIdentifiers('ids', [1, 2, 10]))
        np.testing.assert_array_equal(a, [1, 2])

    def test_identifier_search_with_bad_ids(self):
        e = ElementIdentifiers('ids', [0, 1, 2, 3, 4])
        with self.assertRaises(TypeError):
            _ = (e == 0.1)
        with self.assertRaises(TypeError):
            _ = (e == 'test')


class SubTable(DynamicTable):

    __columns__ = (
        {'name': 'col1', 'description': 'required column', 'required': True},
        {'name': 'col2', 'description': 'optional column'},
        {'name': 'col3', 'description': 'required, indexed column', 'required': True, 'index': True},
        {'name': 'col4', 'description': 'optional, indexed column', 'index': True},
        {'name': 'col5', 'description': 'required region', 'required': True, 'table': True},
        {'name': 'col6', 'description': 'optional region', 'table': True},
        {'name': 'col7', 'description': 'required, indexed region', 'required': True, 'index': True, 'table': True},
        {'name': 'col8', 'description': 'optional, indexed region', 'index': True, 'table': True},
        {'name': 'col10', 'description': 'optional, indexed enum column', 'index': True, 'class': EnumData},
        {'name': 'col11', 'description': 'optional, enumerable column', 'enum': True, 'index': True},
    )


class SubSubTable(SubTable):

    __columns__ = (
        {'name': 'col9', 'description': 'required column', 'required': True},
        # TODO handle edge case where subclass re-defines a column from superclass
        # {'name': 'col2', 'description': 'optional column subsub', 'required': True},  # make col2 required
    )


class TestDynamicTableClassColumns(TestCase):
    """Test functionality related to the predefined __columns__ field of a DynamicTable class."""

    def test_init(self):
        """Test that required columns, and not optional columns, in __columns__ are created on init."""
        table = SubTable(name='subtable', description='subtable description')
        self.assertEqual(table.colnames, ('col1', 'col3', 'col5', 'col7'))
        # test different access methods. note: table.get('col1') is equivalent to table['col1']
        self.assertEqual(table.col1.description, 'required column')
        self.assertEqual(table.col3.description, 'required, indexed column')
        self.assertEqual(table.col5.description, 'required region')
        self.assertEqual(table.col7.description, 'required, indexed region')
        self.assertEqual(table['col1'].description, 'required column')
        # self.assertEqual(table['col3'].description, 'required, indexed column')  # TODO this should work

        self.assertIsNone(table.col2)
        self.assertIsNone(table.col4)
        self.assertIsNone(table.col4_index)
        self.assertIsNone(table.col6)
        self.assertIsNone(table.col8)
        self.assertIsNone(table.col8_index)
        self.assertIsNone(table.col11)
        self.assertIsNone(table.col11_index)

        # uninitialized optional predefined columns cannot be accessed in this manner
        with self.assertRaisesWith(KeyError, "'col2'"):
            table['col2']

    def test_gather_columns_inheritance(self):
        """Test that gathering columns across a type hierarchy works."""
        table = SubSubTable(name='subtable', description='subtable description')
        self.assertEqual(table.colnames, ('col1', 'col3', 'col5', 'col7', 'col9'))

    def test_bad_predefined_columns(self):
        """Test that gathering columns across a type hierarchy works."""
        msg = "'__columns__' must be of type tuple, found <class 'list'>"
        with self.assertRaisesWith(TypeError, msg):
            class BadSubTable(DynamicTable):

                __columns__ = []

    def test_add_req_column(self):
        """Test that adding a required column from __columns__ raises an error."""
        table = SubTable(name='subtable', description='subtable description')
        msg = "column 'col1' already exists in SubTable 'subtable'"
        with self.assertRaisesWith(ValueError, msg):
            table.add_column(name='col1', description='column #1')

    def test_add_req_ind_column(self):
        """Test that adding a required, indexed column from __columns__ raises an error."""
        table = SubTable(name='subtable', description='subtable description')
        msg = "column 'col3' already exists in SubTable 'subtable'"
        with self.assertRaisesWith(ValueError, msg):
            table.add_column(name='col3', description='column #3')

    def test_add_opt_column(self):
        """Test that adding an optional column from __columns__ with matching specs except for description works."""
        table = SubTable(name='subtable', description='subtable description')

        table.add_column(name='col2', description='column #2')  # override __columns__ description
        self.assertEqual(table.col2.description, 'column #2')

        table.add_column(name='col4', description='column #4', index=True)
        self.assertEqual(table.col4.description, 'column #4')

        table.add_column(name='col6', description='column #6', table=True)
        self.assertEqual(table.col6.description, 'column #6')

        table.add_column(name='col8', description='column #8', index=True, table=True)
        self.assertEqual(table.col8.description, 'column #8')

        table.add_column(name='col10', description='column #10', index=True, col_cls=EnumData)
        self.assertIsInstance(table.col10, EnumData)

        table.add_column(name='col11', description='column #11', enum=True, index=True)
        self.assertIsInstance(table.col11, EnumData)

    def test_add_opt_column_mismatched_table_true(self):
        """Test that adding an optional column from __columns__ with non-matched table raises a warning."""
        table = SubTable(name='subtable', description='subtable description')
        msg = ("Column 'col2' is predefined in SubTable with table=False which does not match the entered table "
               "argument. The predefined table spec will be ignored. "
               "Please ensure the new column complies with the spec. "
               "This will raise an error in a future version of HDMF.")
        with self.assertWarnsWith(UserWarning, msg):
            table.add_column(name='col2', description='column #2', table=True)
        self.assertEqual(table.col2.description, 'column #2')
        self.assertEqual(type(table.col2), DynamicTableRegion)  # not VectorData

    def test_add_opt_column_mismatched_table_table(self):
        """Test that adding an optional column from __columns__ with non-matched table raises a warning."""
        table = SubTable(name='subtable', description='subtable description')
        msg = ("Column 'col2' is predefined in SubTable with table=False which does not match the entered table "
               "argument. The predefined table spec will be ignored. "
               "Please ensure the new column complies with the spec. "
               "This will raise an error in a future version of HDMF.")
        with self.assertWarnsWith(UserWarning, msg):
            table.add_column(name='col2', description='column #2', table=DynamicTable('dummy', 'dummy'))
        self.assertEqual(table.col2.description, 'column #2')
        self.assertEqual(type(table.col2), DynamicTableRegion)  # not VectorData

    def test_add_opt_column_mismatched_index_true(self):
        """Test that adding an optional column from __columns__ with non-matched table raises a warning."""
        table = SubTable(name='subtable', description='subtable description')
        msg = ("Column 'col2' is predefined in SubTable with index=False which does not match the entered index "
               "argument. The predefined index spec will be ignored. "
               "Please ensure the new column complies with the spec. "
               "This will raise an error in a future version of HDMF.")
        with self.assertWarnsWith(UserWarning, msg):
            table.add_column(name='col2', description='column #2', index=True)
        self.assertEqual(table.col2.description, 'column #2')
        self.assertEqual(type(table.get('col2')), VectorIndex)  # not VectorData

    def test_add_opt_column_mismatched_index_data(self):
        """Test that adding an optional column from __columns__ with non-matched table raises a warning."""
        table = SubTable(name='subtable', description='subtable description')
        table.add_row(col1='a', col3='c', col5='e', col7='g')
        table.add_row(col1='a', col3='c', col5='e', col7='g')
        msg = ("Column 'col2' is predefined in SubTable with index=False which does not match the entered index "
               "argument. The predefined index spec will be ignored. "
               "Please ensure the new column complies with the spec. "
               "This will raise an error in a future version of HDMF.")
        with self.assertWarnsWith(UserWarning, msg):
            table.add_column(name='col2', description='column #2', data=[1, 2, 3], index=[1, 2])
        self.assertEqual(table.col2.description, 'column #2')
        self.assertEqual(type(table.get('col2')), VectorIndex)  # not VectorData

    def test_add_opt_column_mismatched_col_cls(self):
        """Test that adding an optional column from __columns__ with non-matched table raises a warning."""
        table = SubTable(name='subtable', description='subtable description')
        msg = ("Column 'col10' is predefined in SubTable with class=<class 'hdmf.common.table.EnumData'> "
               "which does not match the entered col_cls "
               "argument. The predefined class spec will be ignored. "
               "Please ensure the new column complies with the spec. "
               "This will raise an error in a future version of HDMF.")
        with self.assertWarnsWith(UserWarning, msg):
            table.add_column(name='col10', description='column #10', index=True)
        self.assertEqual(table.col10.description, 'column #10')
        self.assertEqual(type(table.col10), VectorData)
        self.assertEqual(type(table.get('col10')), VectorIndex)

    def test_add_opt_column_twice(self):
        """Test that adding an optional column from __columns__ twice fails the second time."""
        table = SubTable(name='subtable', description='subtable description')
        table.add_column(name='col2', description='column #2')

        msg = "column 'col2' already exists in SubTable 'subtable'"
        with self.assertRaisesWith(ValueError, msg):
            table.add_column(name='col2', description='column #2b')

    def test_add_opt_column_after_data(self):
        """Test that adding an optional column from __columns__ with data works."""
        table = SubTable(name='subtable', description='subtable description')
        table.add_row(col1='a', col3='c', col5='e', col7='g')
        table.add_column(name='col2', description='column #2', data=('b', ))
        self.assertTupleEqual(table.col2.data, ('b', ))

    def test_add_opt_ind_column_after_data(self):
        """Test that adding an optional, indexed column from __columns__ with data works."""
        table = SubTable(name='subtable', description='subtable description')
        table.add_row(col1='a', col3='c', col5='e', col7='g')
        # TODO this use case is tricky and should not be allowed
        # table.add_column(name='col4', description='column #4', data=(('b', 'b2'), ))

    def test_add_row_opt_column(self):
        """Test that adding a row with an optional column works."""
        table = SubTable(name='subtable', description='subtable description')
        table.add_row(col1='a', col2='b', col3='c', col4=('d1', 'd2'), col5='e', col7='g')
        table.add_row(col1='a', col2='b2', col3='c', col4=('d3', 'd4'), col5='e', col7='g')
        self.assertTupleEqual(table.colnames, ('col1', 'col3', 'col5', 'col7', 'col2', 'col4'))
        self.assertEqual(table.col2.description, 'optional column')
        self.assertEqual(table.col4.description, 'optional, indexed column')
        self.assertListEqual(table.col2.data, ['b', 'b2'])
        # self.assertListEqual(table.col4.data, [('d1', 'd2'), ('d3', 'd4')])  # TODO this should work

    def test_add_row_opt_column_after_data(self):
        """Test that adding a row with an optional column after adding a row without the column raises an error."""
        table = SubTable(name='subtable', description='subtable description')
        table.add_row(col1='a', col3='c', col5='e', col7='g')
        msg = "column must have the same number of rows as 'id'"  # TODO improve error message
        with self.assertRaisesWith(ValueError, msg):
            table.add_row(col1='a', col2='b', col3='c', col5='e', col7='g')

    def test_init_columns_add_req_column(self):
        """Test that passing a required column to init works."""
        col1 = VectorData(name='col1', description='column #1')  # override __columns__ description
        table = SubTable(name='subtable', description='subtable description', columns=[col1])
        self.assertEqual(table.colnames, ('col1', 'col3', 'col5', 'col7'))
        self.assertEqual(table.col1.description, 'column #1')
        self.assertTrue(hasattr(table, 'col1'))

    def test_init_columns_add_req_column_mismatch_index(self):
        """Test that passing a required column that does not match the predefined column specs raises an error."""
        col1 = VectorData(name='col1', description='column #1')  # override __columns__ description
        col1_ind = VectorIndex(name='col1_index', data=list(), target=col1)

        # TODO raise an error
        SubTable(name='subtable', description='subtable description', columns=[col1_ind, col1])

    def test_init_columns_add_req_column_mismatch_table(self):
        """Test that passing a required column that does not match the predefined column specs raises an error."""
        dummy_table = DynamicTable(name='dummy', description='dummy table')
        col1 = DynamicTableRegion(name='col1', data=list(), description='column #1', table=dummy_table)

        # TODO raise an error
        SubTable(name='subtable', description='subtable description', columns=[col1])

    def test_init_columns_add_opt_column(self):
        """Test that passing an optional column to init works."""
        col2 = VectorData(name='col2', description='column #2')  # override __columns__ description
        table = SubTable(name='subtable', description='subtable description', columns=[col2])
        self.assertEqual(table.colnames, ('col2', 'col1', 'col3', 'col5', 'col7'))
        self.assertEqual(table.col2.description, 'column #2')

    def test_init_columns_add_dup_column(self):
        """Test that passing two columns with the same name raises an error."""
        col1 = VectorData(name='col1', description='column #1')  # override __columns__ description
        col1_ind = VectorIndex(name='col1', data=list(), target=col1)

        msg = "'columns' contains columns with duplicate names: ['col1', 'col1']"
        with self.assertRaisesWith(ValueError, msg):
            SubTable(name='subtable', description='subtable description', columns=[col1_ind, col1])


class TestEnumData(TestCase):

    def test_init(self):
        ed = EnumData('cv_data', 'a test EnumData', elements=['a', 'b', 'c'], data=np.array([0, 0, 1, 1, 2, 2]))
        self.assertIsInstance(ed.elements, VectorData)

    def test_get(self):
        ed = EnumData('cv_data', 'a test EnumData', elements=['a', 'b', 'c'], data=np.array([0, 0, 1, 1, 2, 2]))
        dat = ed[2]
        self.assertEqual(dat, 'b')
        dat = ed[-1]
        self.assertEqual(dat, 'c')
        dat = ed[0]
        self.assertEqual(dat, 'a')

    def test_get_list(self):
        ed = EnumData('cv_data', 'a test EnumData', elements=['a', 'b', 'c'], data=np.array([0, 0, 1, 1, 2, 2]))
        dat = ed[[0, 1, 2]]
        np.testing.assert_array_equal(dat, ['a', 'a', 'b'])

    def test_get_list_join(self):
        ed = EnumData('cv_data', 'a test EnumData', elements=['a', 'b', 'c'], data=np.array([0, 0, 1, 1, 2, 2]))
        dat = ed.get([0, 1, 2], join=True)
        self.assertEqual(dat, 'aab')

    def test_get_list_indices(self):
        ed = EnumData('cv_data', 'a test EnumData', elements=['a', 'b', 'c'], data=np.array([0, 0, 1, 1, 2, 2]))
        dat = ed.get([0, 1, 2], index=True)
        np.testing.assert_array_equal(dat, [0, 0, 1])

    def test_get_2d(self):
        ed = EnumData('cv_data', 'a test EnumData',
                      elements=['a', 'b', 'c'],
                      data=np.array([[0, 0], [1, 1], [2, 2]]))
        dat = ed[0]
        np.testing.assert_array_equal(dat, ['a', 'a'])

    def test_get_2d_w_2d(self):
        ed = EnumData('cv_data', 'a test EnumData',
                      elements=['a', 'b', 'c'],
                      data=np.array([[0, 0], [1, 1], [2, 2]]))
        dat = ed[[0, 1]]
        np.testing.assert_array_equal(dat, [['a', 'a'], ['b', 'b']])

    def test_add_row(self):
        ed = EnumData('cv_data', 'a test EnumData', elements=['a', 'b', 'c'])
        ed.add_row('b')
        ed.add_row('a')
        ed.add_row('c')
        np.testing.assert_array_equal(ed.data, np.array([1, 0, 2], dtype=np.uint8))

    def test_add_row_index(self):
        ed = EnumData('cv_data', 'a test EnumData', elements=['a', 'b', 'c'])
        ed.add_row(1, index=True)
        ed.add_row(0, index=True)
        ed.add_row(2, index=True)
        np.testing.assert_array_equal(ed.data, np.array([1, 0, 2], dtype=np.uint8))


class TestIndexedEnumData(TestCase):

    def test_init(self):
        ed = EnumData('cv_data', 'a test EnumData', elements=['a', 'b', 'c'], data=np.array([0, 0, 1, 1, 2, 2]))
        idx = VectorIndex('enum_index', [2, 4, 6], target=ed)
        self.assertIsInstance(ed.elements, VectorData)
        self.assertIsInstance(idx.target, EnumData)

    def test_add_row(self):
        ed = EnumData('cv_data', 'a test EnumData', elements=['a', 'b', 'c'])
        idx = VectorIndex('enum_index', list(), target=ed)
        idx.add_row(['a', 'a', 'a'])
        idx.add_row(['b', 'b'])
        idx.add_row(['c', 'c', 'c', 'c'])
        np.testing.assert_array_equal(idx[0], ['a', 'a', 'a'])
        np.testing.assert_array_equal(idx[1], ['b', 'b'])
        np.testing.assert_array_equal(idx[2], ['c', 'c', 'c', 'c'])

    def test_add_row_index(self):
        ed = EnumData('cv_data', 'a test EnumData', elements=['a', 'b', 'c'])
        idx = VectorIndex('enum_index', list(), target=ed)
        idx.add_row([0, 0, 0], index=True)
        idx.add_row([1, 1], index=True)
        idx.add_row([2, 2, 2, 2], index=True)
        np.testing.assert_array_equal(idx[0], ['a', 'a', 'a'])
        np.testing.assert_array_equal(idx[1], ['b', 'b'])
        np.testing.assert_array_equal(idx[2], ['c', 'c', 'c', 'c'])

    @unittest.skip("feature is not yet supported")
    def test_add_2d_row_index(self):
        ed = EnumData('cv_data', 'a test EnumData', elements=['a', 'b', 'c'])
        idx = VectorIndex('enum_index', list(), target=ed)
        idx.add_row([['a', 'a'], ['a', 'a'], ['a', 'a']])
        idx.add_row([['b', 'b'], ['b', 'b']])
        idx.add_row([['c', 'c'], ['c', 'c'], ['c', 'c'], ['c', 'c']])
        np.testing.assert_array_equal(idx[0], [['a', 'a'], ['a', 'a'], ['a', 'a']])
        np.testing.assert_array_equal(idx[1], [['b', 'b'], ['b', 'b']])
        np.testing.assert_array_equal(idx[2], [['c', 'c'], ['c', 'c'], ['c', 'c'], ['c', 'c']])


class TestIndexing(TestCase):

    def setUp(self):
        dt = DynamicTable(name='slice_test_table', description='a table to test slicing',
                          id=[0, 1, 2])
        dt.add_column('foo', 'scalar column', data=np.array([0.0, 1.0, 2.0]))
        dt.add_column('bar', 'ragged column', index=np.array([2, 3, 6]),
                      data=np.array(['r11', 'r12', 'r21', 'r31', 'r32', 'r33']))
        dt.add_column('baz', 'multi-dimension column',
                      data=np.array([[10.0, 11.0, 12.0],
                                     [20.0, 21.0, 22.0],
                                     [30.0, 31.0, 32.0]]))
        self.table = dt

    def test_single_item(self):
        elem = self.table[0]
        data = OrderedDict()
        data['foo'] = 0.0
        data['bar'] = [np.array(['r11', 'r12'])]
        data['baz'] = [np.array([10.0, 11.0, 12.0])]
        idx = [0]
        exp = pd.DataFrame(data=data, index=pd.Index(name='id', data=idx))
        pd.testing.assert_frame_equal(elem, exp)

    def test_single_item_no_df(self):
        elem = self.table.get(0, df=False)
        self.assertEqual(elem[0], 0)
        self.assertEqual(elem[1], 0.0)
        np.testing.assert_array_equal(elem[2], np.array(['r11', 'r12']))
        np.testing.assert_array_equal(elem[3], np.array([10.0, 11.0, 12.0]))

    def test_slice(self):
        elem = self.table[0:2]
        data = OrderedDict()
        data['foo'] = [0.0, 1.0]
        data['bar'] = [np.array(['r11', 'r12']), np.array(['r21'])]
        data['baz'] = [np.array([10.0, 11.0, 12.0]),
                       np.array([20.0, 21.0, 22.0])]
        idx = [0, 1]
        exp = pd.DataFrame(data=data, index=pd.Index(name='id', data=idx))
        pd.testing.assert_frame_equal(elem, exp)

    def test_slice_no_df(self):
        elem = self.table.get(slice(0, 2), df=False)
        self.assertEqual(elem[0], [0, 1])
        np.testing.assert_array_equal(elem[1], np.array([0.0, 1.0]))
        np.testing.assert_array_equal(elem[2][0], np.array(['r11', 'r12']))
        np.testing.assert_array_equal(elem[2][1], np.array(['r21']))
        np.testing.assert_array_equal(elem[3], np.array([[10.0, 11.0, 12.0], [20.0, 21.0, 22.0]]))

    def test_list(self):
        elem = self.table[[0, 1]]
        data = OrderedDict()
        data['foo'] = [0.0, 1.0]
        data['bar'] = [np.array(['r11', 'r12']), np.array(['r21'])]
        data['baz'] = [np.array([10.0, 11.0, 12.0]),
                       np.array([20.0, 21.0, 22.0])]
        idx = [0, 1]
        exp = pd.DataFrame(data=data, index=pd.Index(name='id', data=idx))
        pd.testing.assert_frame_equal(elem, exp)

    def test_list_no_df(self):
        elem = self.table.get([0, 1], df=False)
        self.assertEqual(elem[0], [0, 1])
        np.testing.assert_array_equal(elem[1], np.array([0.0, 1.0]))
        np.testing.assert_array_equal(elem[2][0], np.array(['r11', 'r12']))
        np.testing.assert_array_equal(elem[2][1], np.array(['r21']))
        np.testing.assert_array_equal(elem[3], np.array([[10.0, 11.0, 12.0], [20.0, 21.0, 22.0]]))


class TestVectorIndex(TestCase):

    def test_init_empty(self):
        foo = VectorData(name='foo', description='foo column')
        foo_ind = VectorIndex(name='foo_index', target=foo, data=list())
        self.assertEqual(foo_ind.name, 'foo_index')
        self.assertEqual(foo_ind.description, "Index for VectorData 'foo'")
        self.assertIs(foo_ind.target, foo)
        self.assertListEqual(foo_ind.data, list())

    def test_init_data(self):
        foo = VectorData(name='foo', description='foo column', data=['a', 'b', 'c'])
        foo_ind = VectorIndex(name='foo_index', target=foo, data=[2, 3])
        self.assertListEqual(foo_ind.data, [2, 3])
        self.assertListEqual(foo_ind[0], ['a', 'b'])
        self.assertListEqual(foo_ind[1], ['c'])


class TestDoubleIndex(TestCase):

    def test_index(self):
        # row 1 has three entries
        # the first entry has two sub-entries
        # the first sub-entry has two values, the second sub-entry has one value
        # the second entry has one sub-entry, which has one value
        foo = VectorData(name='foo', description='foo column', data=['a11', 'a12', 'a21', 'b11'])
        foo_ind = VectorIndex(name='foo_index', target=foo, data=[2, 3, 4])
        foo_ind_ind = VectorIndex(name='foo_index_index', target=foo_ind, data=[2, 3])

        self.assertListEqual(foo_ind[0], ['a11', 'a12'])
        self.assertListEqual(foo_ind[1], ['a21'])
        self.assertListEqual(foo_ind[2], ['b11'])
        self.assertListEqual(foo_ind_ind[0], [['a11', 'a12'], ['a21']])
        self.assertListEqual(foo_ind_ind[1], [['b11']])

    def test_add_vector(self):
        # row 1 has three entries
        # the first entry has two sub-entries
        # the first sub-entry has two values, the second sub-entry has one value
        # the second entry has one sub-entry, which has one value
        foo = VectorData(name='foo', description='foo column', data=['a11', 'a12', 'a21', 'b11'])
        foo_ind = VectorIndex(name='foo_index', target=foo, data=[2, 3, 4])
        foo_ind_ind = VectorIndex(name='foo_index_index', target=foo_ind, data=[2, 3])

        foo_ind_ind.add_vector([['c11', 'c12', 'c13'], ['c21', 'c22']])

        self.assertListEqual(foo.data, ['a11', 'a12', 'a21', 'b11', 'c11', 'c12', 'c13', 'c21', 'c22'])
        self.assertListEqual(foo_ind.data, [2, 3, 4, 7, 9])
        self.assertListEqual(foo_ind[3], ['c11', 'c12', 'c13'])
        self.assertListEqual(foo_ind[4], ['c21', 'c22'])
        self.assertListEqual(foo_ind_ind.data, [2, 3, 5])
        self.assertListEqual(foo_ind_ind[2], [['c11', 'c12', 'c13'], ['c21', 'c22']])


class TestDTDoubleIndex(TestCase):

    def test_double_index(self):
        foo = VectorData(name='foo', description='foo column', data=['a11', 'a12', 'a21', 'b11'])
        foo_ind = VectorIndex(name='foo_index', target=foo, data=[2, 3, 4])
        foo_ind_ind = VectorIndex(name='foo_index_index', target=foo_ind, data=[2, 3])

        table = DynamicTable('table0', 'an example table', columns=[foo, foo_ind, foo_ind_ind])

        self.assertIs(table['foo'], foo_ind_ind)
        self.assertIs(table.foo, foo)
        self.assertListEqual(table['foo'][0], [['a11', 'a12'], ['a21']])
        self.assertListEqual(table[0, 'foo'], [['a11', 'a12'], ['a21']])
        self.assertListEqual(table[1, 'foo'], [['b11']])

    def test_double_index_reverse(self):
        foo = VectorData(name='foo', description='foo column', data=['a11', 'a12', 'a21', 'b11'])
        foo_ind = VectorIndex(name='foo_index', target=foo, data=[2, 3, 4])
        foo_ind_ind = VectorIndex(name='foo_index_index', target=foo_ind, data=[2, 3])

        table = DynamicTable('table0', 'an example table', columns=[foo_ind_ind, foo_ind, foo])

        self.assertIs(table['foo'], foo_ind_ind)
        self.assertIs(table.foo, foo)
        self.assertListEqual(table['foo'][0], [['a11', 'a12'], ['a21']])
        self.assertListEqual(table[0, 'foo'], [['a11', 'a12'], ['a21']])
        self.assertListEqual(table[1, 'foo'], [['b11']])

    def test_double_index_colnames(self):
        foo = VectorData(name='foo', description='foo column', data=['a11', 'a12', 'a21', 'b11'])
        foo_ind = VectorIndex(name='foo_index', target=foo, data=[2, 3, 4])
        foo_ind_ind = VectorIndex(name='foo_index_index', target=foo_ind, data=[2, 3])
        bar = VectorData(name='bar', description='bar column', data=[1, 2])

        table = DynamicTable('table0', 'an example table', columns=[foo, foo_ind, foo_ind_ind, bar],
                             colnames=['foo', 'bar'])

        self.assertTupleEqual(table.columns, (foo_ind_ind, foo_ind, foo, bar))

    def test_double_index_reverse_colnames(self):
        foo = VectorData(name='foo', description='foo column', data=['a11', 'a12', 'a21', 'b11'])
        foo_ind = VectorIndex(name='foo_index', target=foo, data=[2, 3, 4])
        foo_ind_ind = VectorIndex(name='foo_index_index', target=foo_ind, data=[2, 3])
        bar = VectorData(name='bar', description='bar column', data=[1, 2])

        table = DynamicTable('table0', 'an example table', columns=[foo_ind_ind, foo_ind, foo, bar],
                             colnames=['bar', 'foo'])

        self.assertTupleEqual(table.columns, (bar, foo_ind_ind, foo_ind, foo))


class TestDTDoubleIndexSkipMiddle(TestCase):

    def test_index(self):
        foo = VectorData(name='foo', description='foo column', data=['a11', 'a12', 'a21', 'b11'])
        foo_ind = VectorIndex(name='foo_index', target=foo, data=[2, 3, 4])
        foo_ind_ind = VectorIndex(name='foo_index_index', target=foo_ind, data=[2, 3])

        msg = "Found VectorIndex 'foo_index_index' but not its target 'foo_index'"
        with self.assertRaisesWith(ValueError, msg):
            DynamicTable('table0', 'an example table', columns=[foo_ind_ind, foo])


class TestDynamicTableAddIndexRoundTrip(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        table = DynamicTable('table0', 'an example table')
        table.add_column('foo', 'an int column', index=True)
        table.add_row(foo=[1, 2, 3])
        return table


class TestDynamicTableAddEnumRoundTrip(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        table = DynamicTable('table0', 'an example table')
        table.add_column('bar', 'an enumerable column', enum=True)
        table.add_row(bar='a')
        table.add_row(bar='b')
        table.add_row(bar='a')
        table.add_row(bar='c')
        return table


class TestDynamicTableAddEnum(TestCase):

    def test_enum(self):
        table = DynamicTable('table0', 'an example table')
        table.add_column('bar', 'an enumerable column', enum=True)
        table.add_row(bar='a')
        table.add_row(bar='b')
        table.add_row(bar='a')
        table.add_row(bar='c')
        rec = table.to_dataframe()
        exp = pd.DataFrame(data={'bar': ['a', 'b', 'a', 'c']}, index=pd.Series(name='id', data=[0, 1, 2, 3]))
        pd.testing.assert_frame_equal(exp, rec)

    def test_enum_index(self):
        table = DynamicTable('table0', 'an example table')
        table.add_column('bar', 'an indexed enumerable column', enum=True, index=True)
        table.add_row(bar=['a', 'a', 'a'])
        table.add_row(bar=['b', 'b', 'b', 'b'])
        table.add_row(bar=['c', 'c'])
        rec = table.to_dataframe()
        exp = pd.DataFrame(data={'bar': [['a', 'a', 'a'],
                                         ['b', 'b', 'b', 'b'],
                                         ['c', 'c']]},
                           index=pd.Series(name='id', data=[0, 1, 2]))
        pd.testing.assert_frame_equal(exp, rec)


class TestDynamicTableInitIndexRoundTrip(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        foo = VectorData(name='foo', description='foo column', data=['a', 'b', 'c'])
        foo_ind = VectorIndex(name='foo_index', target=foo, data=[2, 3])

        # NOTE: on construct, columns are ordered such that indices go before data, so create the table that way
        # for proper comparison of the columns list
        table = DynamicTable('table0', 'an example table', columns=[foo_ind, foo])
        return table


class TestDoubleIndexRoundtrip(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        foo = VectorData(name='foo', description='foo column', data=['a11', 'a12', 'a21', 'b11'])
        foo_ind = VectorIndex(name='foo_index', target=foo, data=[2, 3, 4])
        foo_ind_ind = VectorIndex(name='foo_index_index', target=foo_ind, data=[2, 3])

        # NOTE: on construct, columns are ordered such that indices go before data, so create the table that way
        # for proper comparison of the columns list
        table = DynamicTable('table0', 'an example table', columns=[foo_ind_ind, foo_ind, foo])
        return table


class TestDataIOColumns(H5RoundTripMixin, TestCase):
    def setUpContainer(self):
        self.chunked_data = H5DataIO(
            data=[i for i in range(10)],
            chunks=(3,),
            fillvalue=-1,
        )
        self.compressed_data = H5DataIO(
            data=np.arange(10),
            compression=1,
            shuffle=True,
            fletcher32=True,
            allow_plugin_filters=True,
        )
        foo = VectorData(name='foo', description='chunked column', data=self.chunked_data)
        bar = VectorData(name='bar', description='chunked column', data=self.compressed_data)

        # NOTE: on construct, columns are ordered such that indices go before data, so create the table that way
        # for proper comparison of the columns list
        table = DynamicTable('table0', 'an example table', columns=[foo, bar])
        table.add_row(foo=1, bar=1)
        return table

    def test_roundtrip(self):
        super().test_roundtrip()

        with h5py.File(self.filename, 'r') as f:
            chunked_dset = f['foo']
            self.assertTrue(np.all(chunked_dset[:] == self.chunked_data.data))
            self.assertEqual(chunked_dset.chunks, (3,))
            self.assertEqual(chunked_dset.fillvalue, -1)

            compressed_dset = f['bar']
            self.assertTrue(np.all(compressed_dset[:] == self.compressed_data.data))
            self.assertEqual(compressed_dset.compression, 'gzip')
            self.assertEqual(compressed_dset.shuffle, True)
            self.assertEqual(compressed_dset.fletcher32, True)


class TestDataIOIndexedColumns(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        self.chunked_data = H5DataIO(
            data=np.arange(30).reshape(5, 2, 3),
            chunks=(1, 1, 3),
            fillvalue=-1,
        )
        self.compressed_data = H5DataIO(
            data=np.arange(30).reshape(5, 2, 3),
            compression=1,
            shuffle=True,
            fletcher32=True,
            allow_plugin_filters=True,
        )
        foo = VectorData(name='foo', description='chunked column', data=self.chunked_data)
        foo_ind = VectorIndex(name='foo_index', target=foo, data=[2, 3, 4])
        bar = VectorData(name='bar', description='chunked column', data=self.compressed_data)
        bar_ind = VectorIndex(name='bar_index', target=bar, data=[2, 3, 4])

        # NOTE: on construct, columns are ordered such that indices go before data, so create the table that way
        # for proper comparison of the columns list
        table = DynamicTable('table0', 'an example table', columns=[foo_ind, foo, bar_ind, bar])

        # check for add_row
        table.add_row(foo=np.arange(30).reshape(5, 2, 3), bar=np.arange(30).reshape(5, 2, 3))

        return table

    def test_roundtrip(self):
        super().test_roundtrip()

        with h5py.File(self.filename, 'r') as f:
            chunked_dset = f['foo']
            self.assertTrue(np.all(chunked_dset[:] == self.chunked_data.data))
            self.assertEqual(chunked_dset.chunks, (1, 1, 3))
            self.assertEqual(chunked_dset.fillvalue, -1)

            compressed_dset = f['bar']
            self.assertTrue(np.all(compressed_dset[:] == self.compressed_data.data))
            self.assertEqual(compressed_dset.compression, 'gzip')
            self.assertEqual(compressed_dset.shuffle, True)
            self.assertEqual(compressed_dset.fletcher32, True)


class TestDataIOIndex(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        self.chunked_data = H5DataIO(
            data=np.arange(30).reshape(5, 2, 3),
            chunks=(1, 1, 3),
            fillvalue=-1,
            maxshape=(None, 2, 3)
        )
        self.chunked_index_data = H5DataIO(
            data=np.array([2, 3, 5], dtype=np.uint),
            chunks=(2, ),
            fillvalue=np.uint(10),
            maxshape=(None,)
        )
        self.compressed_data = H5DataIO(
            data=np.arange(30).reshape(5, 2, 3),
            compression=1,
            shuffle=True,
            fletcher32=True,
            allow_plugin_filters=True,
            maxshape=(None, 2, 3)
        )
        self.compressed_index_data = H5DataIO(
            data=np.array([2, 4, 5], dtype=np.uint),
            compression=1,
            shuffle=True,
            fletcher32=False,
            allow_plugin_filters=True,
            maxshape=(None,)
        )
        foo = VectorData(name='foo', description='chunked column', data=self.chunked_data)
        foo_ind = VectorIndex(name='foo_index', target=foo, data=self.chunked_index_data)
        bar = VectorData(name='bar', description='chunked column', data=self.compressed_data)
        bar_ind = VectorIndex(name='bar_index', target=bar, data=self.compressed_index_data)

        # NOTE: on construct, columns are ordered such that indices go before data, so create the table that way
        # for proper comparison of the columns list
        table = DynamicTable('table0', 'an example table', columns=[foo_ind, foo, bar_ind, bar],
                             id=H5DataIO(data=[0, 1, 2], chunks=True, maxshape=(None,)))

        # check for add_row
        table.add_row(foo=np.arange(30).reshape(5, 2, 3),
                      bar=np.arange(30).reshape(5, 2, 3))

        return table

    def test_append(self, cache_spec=False):
        """Write the container to an HDF5 file, read the container from the file, and append to it."""
        with HDF5IO(self.filename, manager=get_manager(), mode='w') as write_io:
            write_io.write(self.container, cache_spec=cache_spec)

        self.reader = HDF5IO(self.filename, manager=get_manager(), mode='a')
        read_table = self.reader.read()

        data = np.arange(30, 60).reshape(5, 2, 3)
        read_table.add_row(foo=data, bar=data)

        np.testing.assert_array_equal(read_table['foo'][-1], data)


class TestDTRReferences(TestCase):

    def setUp(self):
        self.filename = 'test_dtr_references.h5'

    def tearDown(self):
        remove_test_file(self.filename)

    def test_dtr_references(self):
        """Test roundtrip of a table with a ragged DTR to another table containing a column of references."""
        group1 = Container('group1')
        group2 = Container('group2')

        table1 = DynamicTable(
            name='table1',
            description='test table 1'
        )
        table1.add_column(
            name='x',
            description='test column of ints'
        )
        table1.add_column(
            name='y',
            description='test column of reference'
        )
        table1.add_row(id=101, x=1, y=group1)
        table1.add_row(id=102, x=2, y=group1)
        table1.add_row(id=103, x=3, y=group2)

        table2 = DynamicTable(
            name='table2',
            description='test table 2'
        )

        # create a ragged column that references table1
        # each row of table2 corresponds to one or more rows of table 1
        table2.add_column(
            name='electrodes',
            description='column description',
            index=True,
            table=table1
        )

        table2.add_row(id=10, electrodes=[1, 2])

        multi_container = SimpleMultiContainer('multi')
        multi_container.add_container(group1)
        multi_container.add_container(group2)
        multi_container.add_container(table1)
        multi_container.add_container(table2)

        with HDF5IO(self.filename, manager=get_manager(), mode='w') as io:
            io.write(multi_container)

        with HDF5IO(self.filename, manager=get_manager(), mode='r') as io:
            read_multi_container = io.read()
            self.assertContainerEqual(read_multi_container, multi_container, ignore_name=True)

            # test DTR access
            read_group1 = read_multi_container['group1']
            read_group2 = read_multi_container['group2']
            read_table = read_multi_container['table2']
            ret = read_table[0, 'electrodes']
            expected = pd.DataFrame({'x': np.array([2, 3]),
                                     'y': [read_group1, read_group2]},
                                    index=pd.Index(data=[102, 103], name='id'))
            pd.testing.assert_frame_equal(ret, expected)
