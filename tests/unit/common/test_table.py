from hdmf.common import DynamicTable, VectorData, VectorIndex, ElementIdentifiers, DynamicTableRegion
from hdmf.testing import TestCase, H5RoundTripMixin

import pandas as pd
import numpy as np
from collections import OrderedDict


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


class TestDynamicTableRoundTrip(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        table = DynamicTable('table0', 'an example table')
        table.add_column('foo', 'an int column')
        table.add_column('bar', 'a float column')
        table.add_column('baz', 'a string column')
        table.add_column('qux', 'a boolean column')
        table.add_row(foo=27, bar=28.0, baz="cat", qux=True)
        table.add_row(foo=37, bar=38.0, baz="dog", qux=False)
        return table


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
        res = dynamic_table_region.to_dataframe(exclude=set(['baz', 'foo']))
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
