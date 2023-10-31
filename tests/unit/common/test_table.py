from collections import OrderedDict
import h5py
import numpy as np
import os
import pandas as pd
import unittest

from hdmf import Container
from hdmf import TermSet, TermSetWrapper
from hdmf.backends.hdf5 import H5DataIO, HDF5IO
from hdmf.backends.hdf5.h5tools import H5_TEXT, H5PY_3
from hdmf.common import (DynamicTable, VectorData, VectorIndex, ElementIdentifiers, EnumData,
                         DynamicTableRegion, get_manager, SimpleMultiContainer)
from hdmf.testing import TestCase, H5RoundTripMixin, remove_test_file
from hdmf.utils import StrDataset
from hdmf.data_utils import DataChunkIterator

from tests.unit.helpers.utils import get_temp_filepath

try:
    import linkml_runtime  # noqa: F401
    LINKML_INSTALLED = True
except ImportError:
    LINKML_INSTALLED = False


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
        table = DynamicTable(name="with_table_columns", description='a test table', columns=cols)
        return table

    def with_columns_and_data(self):
        columns = [
            VectorData(name=s['name'], description=s['description'], data=d)
            for s, d in zip(self.spec, self.data)
        ]
        return DynamicTable(name="with_columns_and_data", description='a test table', columns=columns)

    def with_spec(self):
        table = DynamicTable(name="with_spec", description='a test table', columns=self.spec)
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
        table = DynamicTable(name="with_spec", description='a test table', columns=columns)
        self.check_table(table)

    def test_constructor_ids(self):
        columns = [VectorData(name=s['name'], description=s['description'], data=d)
                   for s, d in zip(self.spec, self.data)]
        table = DynamicTable(name="with_columns", description='a test table', id=[0, 1, 2, 3, 4], columns=columns)
        self.check_table(table)

    def test_constructor_ElementIdentifier_ids(self):
        columns = [VectorData(name=s['name'], description=s['description'], data=d)
                   for s, d in zip(self.spec, self.data)]
        ids = ElementIdentifiers(name='ids', data=[0, 1, 2, 3, 4])
        table = DynamicTable(name="with_columns", description='a test table', id=ids, columns=columns)
        self.check_table(table)

    def test_constructor_ids_bad_ids(self):
        columns = [VectorData(name=s['name'], description=s['description'], data=d)
                   for s, d in zip(self.spec, self.data)]
        msg = "Must provide same number of ids as length of columns"
        with self.assertRaisesWith(ValueError, msg):
            DynamicTable(name="with_columns", description='a test table', id=[0, 1], columns=columns)

    def test_constructor_all_columns_are_iterators(self):
        """
        All columns are specified via AbstractDataChunkIterator but no id's are given.
        Test that an error is being raised because we can't determine the id's.
        """
        data = np.array([1., 2., 3.])
        column = VectorData(name="TestColumn", description="", data=DataChunkIterator(data))
        msg = ("Cannot determine row id's for table. Must provide ids with same length "
               "as the columns when all columns are specified via DataChunkIterator objects.")
        with self.assertRaisesWith(ValueError, msg):
            _ = DynamicTable(name="TestTable", description="", columns=[column])
        # now test that when we supply id's that the error goes away
        _ = DynamicTable(name="TestTable", description="", columns=[column], id=list(range(3)))

    @unittest.skipIf(not LINKML_INSTALLED, "optional LinkML module is not installed")
    def test_add_col_validate(self):
        terms = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')
        col1 = VectorData(
            name='Species_1',
            description='...',
            data=TermSetWrapper(value=['Homo sapiens'], termset=terms)
        )
        species = DynamicTable(name='species', description='My species', columns=[col1])
        species.add_column(name='Species_2',
                           description='Species data',
                           data=TermSetWrapper(value=['Mus musculus'], termset=terms))
        expected_df_data = \
            {'Species_1': {0: 'Homo sapiens'},
             'Species_2': {0: 'Mus musculus'}}
        expected_df = pd.DataFrame.from_dict(expected_df_data)
        expected_df.index.name = 'id'
        pd.testing.assert_frame_equal(species.to_dataframe(), expected_df)

    @unittest.skipIf(not LINKML_INSTALLED, "optional LinkML module is not installed")
    def test_add_col_validate_bad_data(self):
        terms = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')
        col1 = VectorData(
            name='Species_1',
            description='...',
            data=TermSetWrapper(value=['Homo sapiens'], termset=terms)
        )
        species = DynamicTable(name='species', description='My species', columns=[col1])
        with self.assertRaises(ValueError):
            species.add_column(name='Species_2',
                               description='Species data',
                               data=TermSetWrapper(value=['bad data'],
                                                   termset=terms))

    @unittest.skipIf(not LINKML_INSTALLED, "optional LinkML module is not installed")
    def test_add_row_validate(self):
        terms = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')
        col1 = VectorData(
            name='Species_1',
            description='...',
            data=TermSetWrapper(value=['Homo sapiens'], termset=terms)
        )
        col2 = VectorData(
            name='Species_2',
            description='...',
            data=TermSetWrapper(value=['Mus musculus'], termset=terms)
        )
        species = DynamicTable(name='species', description='My species', columns=[col1,col2])
        species.add_row(Species_1='Myrmecophaga tridactyla', Species_2='Ursus arctos horribilis')
        expected_df_data = \
            {'Species_1': {0: 'Homo sapiens', 1: 'Myrmecophaga tridactyla'},
             'Species_2': {0: 'Mus musculus', 1: 'Ursus arctos horribilis'}}
        expected_df = pd.DataFrame.from_dict(expected_df_data)
        expected_df.index.name = 'id'
        pd.testing.assert_frame_equal(species.to_dataframe(), expected_df)

    @unittest.skipIf(not LINKML_INSTALLED, "optional LinkML module is not installed")
    def test_add_row_validate_bad_data_one_col(self):
        terms = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')
        col1 = VectorData(
            name='Species_1',
            description='...',
            data=TermSetWrapper(value=['Homo sapiens'], termset=terms)
        )
        col2 = VectorData(
            name='Species_2',
            description='...',
            data=TermSetWrapper(value=['Mus musculus'], termset=terms)
        )
        species = DynamicTable(name='species', description='My species', columns=[col1,col2])
        with self.assertRaises(ValueError):
            species.add_row(Species_1='bad', Species_2='Ursus arctos horribilis')

    @unittest.skipIf(not LINKML_INSTALLED, "optional LinkML module is not installed")
    def test_add_row_validate_bad_data_all_col(self):
        terms = TermSet(term_schema_path='tests/unit/example_test_term_set.yaml')
        col1 = VectorData(
            name='Species_1',
            description='...',
            data=TermSetWrapper(value=['Homo sapiens'], termset=terms)
        )
        col2 = VectorData(
            name='Species_2',
            description='...',
            data=TermSetWrapper(value=['Mus musculus'], termset=terms)
        )
        species = DynamicTable(name='species', description='My species', columns=[col1,col2])
        with self.assertRaises(ValueError):
            species.add_row(Species_1='bad data', Species_2='bad data')

    def test_constructor_bad_columns(self):
        columns = ['bad_column']
        msg = "'columns' must be a list of dict, VectorData, DynamicTableRegion, or VectorIndex"
        with self.assertRaisesWith(ValueError, msg):
            DynamicTable(name="with_columns", description='a test table', columns=columns)

    def test_constructor_unequal_length_columns(self):
        columns = [VectorData(name='col1', description='desc', data=[1, 2, 3]),
                   VectorData(name='col2', description='desc', data=[1, 2])]
        msg = "Columns must be the same length"
        with self.assertRaisesWith(ValueError, msg):
            DynamicTable(name="with_columns", description='a test table', columns=columns)

    def test_constructor_colnames(self):
        """Test that passing colnames correctly sets the order of the columns."""
        cols = [VectorData(**d) for d in self.spec]
        table = DynamicTable(name="with_columns", description='a test table',
                             columns=cols, colnames=['baz', 'bar', 'foo'])
        self.assertTupleEqual(table.columns, tuple(cols[::-1]))

    def test_constructor_colnames_no_columns(self):
        """Test that passing colnames without columns raises an error."""
        msg = "Must supply 'columns' if specifying 'colnames'"
        with self.assertRaisesWith(ValueError, msg):
            DynamicTable(name="with_columns", description='a test table',  colnames=['baz', 'bar', 'foo'])

    def test_constructor_colnames_vectorindex(self):
        """Test that passing colnames with a VectorIndex column puts the index in the right location in columns."""
        cols = [VectorData(**d) for d in self.spec]
        ind = VectorIndex(name='foo_index', data=list(), target=cols[0])
        cols.append(ind)
        table = DynamicTable(name="with_columns", description='a test table', columns=cols,
                             colnames=['baz', 'bar', 'foo'])
        self.assertTupleEqual(table.columns, (cols[2], cols[1], ind, cols[0]))

    def test_constructor_colnames_vectorindex_rev(self):
        """Test that passing colnames with a VectorIndex column puts the index in the right location in columns."""
        cols = [VectorData(**d) for d in self.spec]
        ind = VectorIndex(name='foo_index', data=list(), target=cols[0])
        cols.insert(0, ind)  # put index before its target
        table = DynamicTable(name="with_columns", description='a test table', columns=cols,
                             colnames=['baz', 'bar', 'foo'])
        self.assertTupleEqual(table.columns, (cols[3], cols[2], ind, cols[1]))

    def test_constructor_dup_index(self):
        """Test that passing two indices for the same column raises an error."""
        cols = [VectorData(**d) for d in self.spec]
        cols.append(VectorIndex(name='foo_index', data=list(), target=cols[0]))
        cols.append(VectorIndex(name='foo_index2', data=list(), target=cols[0]))
        msg = "'columns' contains index columns with the same target: ['foo', 'foo']"
        with self.assertRaisesWith(ValueError, msg):
            DynamicTable(name="with_columns", description='a test table', columns=cols)

    def test_constructor_index_missing_target(self):
        """Test that passing an index without its target raises an error."""
        cols = [VectorData(**d) for d in self.spec]
        missing_col = cols.pop(2)
        cols.append(VectorIndex(name='foo_index', data=list(), target=missing_col))
        msg = "Found VectorIndex 'foo_index' but not its target 'baz'"
        with self.assertRaisesWith(ValueError, msg):
            DynamicTable(name="with_columns", description='a test table', columns=cols)

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
        ind = VectorIndex(name='quux', data=list(), target=table['qux'])

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

    def test_add_column_auto_index_int(self):
        """
        Add a column as a list of lists after we have already added data so that we need to create a single VectorIndex
        with index=1 as parameter
        """
        table = self.with_spec()
        table.add_row(foo=5, bar=50.0, baz='lizard')
        table.add_row(foo=5, bar=50.0, baz='lizard')
        expected = [[1, 2, 3],
                    [1, 2, 3, 4]]
        table.add_column(name='qux',
                         description='qux column',
                         data=expected,
                         index=1)
        self.assertListEqual(table['qux'][:], expected)
        self.assertListEqual(table.qux_index.data, [3, 7])
        # Add more rows after we created the column
        table.add_row(foo=5, bar=50.0, baz='lizard', qux=[10, 11, 12])
        self.assertListEqual(table['qux'][:], expected + [[10, 11, 12], ])
        self.assertListEqual(table.qux_index.data, [3, 7, 10])

    def test_add_column_auto_index_bool(self):
        """
        Add a column as a list of lists after we have already added data so that we need to create a single VectorIndex
        with index=True as parameter
        """
        table = self.with_spec()
        table.add_row(foo=5, bar=50.0, baz='lizard')
        table.add_row(foo=5, bar=50.0, baz='lizard')
        expected = [[1, 2, 3],
                    [1, 2, 3, 4]]
        table.add_column(name='qux',
                         description='qux column',
                         data=expected,
                         index=True)
        self.assertListEqual(table['qux'][:], expected)
        self.assertListEqual(table.qux_index.data, [3, 7])
        # Add more rows after we created the column
        table.add_row(foo=5, bar=50.0, baz='lizard', qux=[10, 11, 12])
        self.assertListEqual(table['qux'][:], expected + [[10, 11, 12], ])
        self.assertListEqual(table.qux_index.data, [3, 7, 10])

    def test_add_column_auto_multi_index_int(self):
        """
        Add a column as a list of lists of lists after we have already added data so that we need to create a
        two VectorIndex for the column so we set index=2 as parameter
        """
        table = self.with_spec()
        table.add_row(foo=5, bar=50.0, baz='lizard')
        table.add_row(foo=5, bar=50.0, baz='lizard')
        expected = [[[1, 2, 3], [1]],
                    [[1, 2, 3, 4], [1, 2]]]
        table.add_column(name='qux',
                         description='qux column',
                         data=expected,
                         index=2)
        self.assertListEqual(table['qux'][:], expected)
        self.assertListEqual(table.qux_index_index.data, [2, 4])
        self.assertListEqual(table.qux_index.data, [3, 4, 8, 10])
        # Add more rows after we created the column
        table.add_row(foo=5, bar=50.0, baz='lizard', qux=[[10, 11, 12], ])
        self.assertListEqual(table['qux'][:], expected + [[[10, 11, 12], ]])
        self.assertListEqual(table.qux_index_index.data, [2, 4, 5])
        self.assertListEqual(table.qux_index.data, [3, 4, 8, 10, 13])

    def test_add_column_auto_multi_index_int_bad_index_levels(self):
        """
        Add a column as a list of lists if lists after we have already added data so that we need to create a
        a two-level index, but we ask for either too many or too few index levels.
        """
        table = self.with_spec()
        table.add_row(foo=5, bar=50.0, baz='lizard')
        table.add_row(foo=5, bar=50.0, baz='lizard')
        expected = [[[1, 2, 3], [1]],
                    [[1, 2, 3, 4], [1, 2]]]
        msg = "Cannot automatically construct VectorIndex for nested array. Invalid data array element found."
        with self.assertRaisesWith(ValueError, msg):
            table.add_column(name='qux',
                             description='qux column',
                             data=expected,
                             index=3)  # Too many index levels given
        # Asking for too few indexes will work here but should then later fail on write
        msg = ("Cannot automatically construct VectorIndex for nested array. "
               "Column data contains arrays as cell values. Please check the 'data' and 'index' parameters.")
        with self.assertRaisesWith(ValueError, msg + " 'index=1' may be too small for the given data."):
            table.add_column(name='qux',
                             description='qux column',
                             data=expected,
                             index=1)
        with self.assertRaisesWith(ValueError, msg + " 'index=True' may be too small for the given data."):
            table.add_column(name='qux',
                             description='qux column',
                             data=expected,
                             index=True)

    def test_add_column_auto_multi_index_int_with_empty_slots(self):
        """
        Add a column as a list of lists of lists after we have already added data so that we need to create 2
        VectorIndex levels so we set index=2 as parameter. For the data the first two rows have no entries in the
        multi-indexed column.
        """
        table = self.with_spec()
        table.add_row(foo=5, bar=50.0, baz='lizard')
        table.add_row(foo=5, bar=50.0, baz='lizard')
        expected = [[[], []],
                    [[], []]]
        table.add_column(name='qux',
                         description='qux column',
                         data=expected,
                         index=2)
        self.assertListEqual(table['qux'][:], expected)
        self.assertListEqual(table.qux_index_index.data, [2, 4])
        self.assertListEqual(table.qux_index.data, [0, 0, 0, 0])
        # Add more rows after we created the column
        table.add_row(foo=5, bar=50.0, baz='lizard', qux=[[10, 11, 12], ])
        self.assertListEqual(table['qux'][:], expected + [[[10, 11, 12], ]])
        self.assertListEqual(table.qux_index_index.data, [2, 4, 5])
        self.assertListEqual(table.qux_index.data, [0, 0, 0, 0, 3])

    def test_auto_multi_index_required(self):

        class TestTable(DynamicTable):
            __columns__ = (dict(name='qux', description='qux column', index=3, required=True),)

        table = TestTable(name='table_name', description='table_description')
        self.assertIsInstance(table.qux, VectorData)  # check that the attribute is set
        self.assertIsInstance(table.qux_index, VectorIndex)  # check that the attribute is set
        self.assertIsInstance(table.qux_index_index, VectorIndex)  # check that the attribute is set
        self.assertIsInstance(table.qux_index_index_index, VectorIndex)  # check that the attribute is set
        table.add_row(
            qux=[
                    [
                        [1, 2, 3],
                        [1, 2, 3, 4]
                    ]
                ]
        )
        table.add_row(
            qux=[
                    [
                        [1, 2]
                    ]
                ]
        )

        expected = [
            [
                [
                    [1, 2, 3],
                    [1, 2, 3, 4]
                ]
            ],
            [
                [
                    [1, 2]
                ]
            ]
        ]
        self.assertListEqual(table['qux'][:], expected)
        self.assertEqual(table.qux_index_index_index.data, [1, 2])

    def test_auto_multi_index(self):

        class TestTable(DynamicTable):
            __columns__ = (dict(name='qux', description='qux column', index=3),)  # this is optional

        table = TestTable(name='table_name', description='table_description')
        self.assertIsNone(table.qux)  # these are reserved as attributes but not yet initialized
        self.assertIsNone(table.qux_index)
        self.assertIsNone(table.qux_index_index)
        self.assertIsNone(table.qux_index_index_index)
        table.add_row(
            qux=[
                    [
                        [1, 2, 3],
                        [1, 2, 3, 4]
                    ]
                ]
        )
        table.add_row(
            qux=[
                    [
                        [1, 2]
                    ]
                ]
        )

        expected = [
            [
                [
                    [1, 2, 3],
                    [1, 2, 3, 4]
                ]
            ],
            [
                [
                    [1, 2]
                ]
            ]
        ]
        self.assertListEqual(table['qux'][:], expected)
        self.assertEqual(table.qux_index_index_index.data, [1, 2])

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
        self.assertEqual(rows.iloc[2].iloc[0], 5)
        self.assertEqual(rows.iloc[2].iloc[1], 50.0)
        self.assertEqual(rows.iloc[2].iloc[2], 'lizard')

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
        expected = DynamicTable(name='test_table', description='the expected table')
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
        df = DynamicTable(name='test', description='desc', id=np.arange(3, dtype='int'),
                          columns=(col, )).to_dataframe()
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
                    DynamicTable(name="test_table", description='a test table', columns=cols)

    def test_colnames_none(self):
        table = DynamicTable(name='table0', description='an example table')
        self.assertTupleEqual(table.colnames, tuple())
        self.assertTupleEqual(table.columns, tuple())

    def test_index_out_of_bounds(self):
        table = self.with_columns_and_data()
        msg = "Row index out of range for DynamicTable 'with_columns_and_data' (length 5)."
        with self.assertRaisesWith(IndexError, msg):
            table[5]

    def test_no_df_nested(self):
        table = self.with_columns_and_data()
        msg = 'DynamicTable.get() with df=False and index=False is not yet supported.'
        with self.assertRaisesWith(ValueError, msg):
            table.get(0, df=False, index=False)

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
        table = DynamicTable(name="with_columns_and_data", description='a test table', columns=columns)
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
        table = DynamicTable(name='test', description='desc', columns=(col, ))
        df = table.to_dataframe()
        df2 = pd.DataFrame({'data': [x for x in data]},
                           index=pd.Index(name='id', data=[0]))
        pd.testing.assert_frame_equal(df, df2)
        pd.testing.assert_frame_equal(table.get(0), df2)

    def test_multidim_col_one_elt_tuple(self):
        data = [(1, 2)]
        col = VectorData(name='data', description='desc', data=data)
        table = DynamicTable(name='test', description='desc', columns=(col, ))
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
        test_table = DynamicTable(name="with_columns_and_data", description='a test table', columns=columns)

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
        test_table = DynamicTable(name="with_columns_and_data", description='a test table', columns=columns)

        table = self.with_columns_and_data()
        self.assertFalse(table == test_table)

    def test_eq_diff_name(self):
        columns = [
            VectorData(name=s['name'], description=s['description'], data=d)
            for s, d in zip(self.spec, self.data)
        ]
        test_table = DynamicTable(name="wrong name", description='a test table', columns=columns)

        table = self.with_columns_and_data()
        self.assertFalse(table == test_table)

    def test_eq_diff_desc(self):
        columns = [
            VectorData(name=s['name'], description=s['description'], data=d)
            for s, d in zip(self.spec, self.data)
        ]
        test_table = DynamicTable(name="with_columns_and_data", description='wrong description', columns=columns)

        table = self.with_columns_and_data()
        self.assertFalse(table == test_table)

    def test_eq_bad_type(self):
        container = Container('test_container')
        table = self.with_columns_and_data()
        self.assertFalse(table == container)


class TestDynamicTableRoundTrip(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        table = DynamicTable(name='table0', description='an example table')
        table.add_column(name='foo', description='an int column')
        table.add_column(name='bar', description='a float column')
        table.add_column(name='baz', description='a string column')
        table.add_column(name='qux', description='a boolean column')
        table.add_column(name='corge', description='a doubly indexed int column', index=2)
        table.add_column(name='quux', description='an enum column', enum=True)
        table.add_row(foo=27, bar=28.0, baz="cat", corge=[[1, 2, 3], [4, 5, 6]], qux=True, quux='a')
        table.add_row(foo=37, bar=38.0, baz="dog", corge=[[11, 12, 13], [14, 15, 16]], qux=False, quux='b')
        table.add_column(name='agv', description='a column with autogenerated multi vector index',
                         data=[[[1, 2, 3], [4, 5]], [[6, 7], [8, 9, 10]]], index=2)
        return table

    def test_index_out_of_bounds(self):
        table = self.roundtripContainer()
        msg = "Row index 5 out of range for DynamicTable 'root' (length 2)."
        with self.assertRaisesWith(IndexError, msg):
            table[5]


class TestEmptyDynamicTableRoundTrip(H5RoundTripMixin, TestCase):
    """Test roundtripping a DynamicTable with no rows and no columns."""

    def setUpContainer(self):
        table = DynamicTable(name='table0', description='an example table')
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
        return DynamicTable(name="with_columns_and_data", description='a test table', columns=columns)

    def test_indexed_dynamic_table_region(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion(name='dtr', data=[1, 2, 2], description='desc', table=table)
        fetch_ids = dynamic_table_region[:3].index.values
        self.assertListEqual(fetch_ids.tolist(), [1, 2, 2])

    def test_dynamic_table_region_iteration(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion(name='dtr', data=[0, 1, 2, 3, 4], description='desc', table=table)
        for ii, item in enumerate(dynamic_table_region):
            self.assertTrue(table[ii].equals(item))

    def test_dynamic_table_region_shape(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion(name='dtr', data=[0, 1, 2, 3, 4], description='desc', table=table)
        self.assertTupleEqual(dynamic_table_region.shape, (5, 3))

    def test_dynamic_table_region_to_dataframe(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion(name='dtr', data=[0, 1, 2, 2], description='desc', table=table)
        res = dynamic_table_region.to_dataframe()
        self.assertListEqual(res.index.tolist(), [0, 1, 2, 2])
        self.assertListEqual(res['foo'].tolist(), [1, 2, 3, 3])
        self.assertListEqual(res['bar'].tolist(), [10.0, 20.0, 30.0, 30.0])
        self.assertListEqual(res['baz'].tolist(), ['cat', 'dog', 'bird', 'bird'])

    def test_dynamic_table_region_to_dataframe_exclude_cols(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion(name='dtr', data=[0, 1, 2, 2], description='desc', table=table)
        res = dynamic_table_region.to_dataframe(exclude={'baz', 'foo'})
        self.assertListEqual(res.index.tolist(), [0, 1, 2, 2])
        self.assertEqual(len(res.columns), 1)
        self.assertListEqual(res['bar'].tolist(), [10.0, 20.0, 30.0, 30.0])

    def test_dynamic_table_region_getitem_slice(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion(name='dtr', data=[0, 1, 2, 2], description='desc', table=table)
        res = dynamic_table_region[1:3]
        self.assertListEqual(res.index.tolist(), [1, 2])
        self.assertListEqual(res['foo'].tolist(), [2, 3])
        self.assertListEqual(res['bar'].tolist(), [20.0, 30.0])
        self.assertListEqual(res['baz'].tolist(), ['dog', 'bird'])

    def test_dynamic_table_region_getitem_single_row_by_index(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion(name='dtr', data=[0, 1, 2, 2], description='desc', table=table)
        res = dynamic_table_region[2]
        self.assertListEqual(res.index.tolist(), [2, ])
        self.assertListEqual(res['foo'].tolist(), [3, ])
        self.assertListEqual(res['bar'].tolist(), [30.0, ])
        self.assertListEqual(res['baz'].tolist(), ['bird', ])

    def test_dynamic_table_region_getitem_single_cell(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion(name='dtr', data=[0, 1, 2, 2], description='desc', table=table)
        res = dynamic_table_region[2, 'foo']
        self.assertEqual(res, 3)
        res = dynamic_table_region[1, 'baz']
        self.assertEqual(res, 'dog')

    def test_dynamic_table_region_getitem_slice_of_column(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion(name='dtr', data=[0, 1, 2, 2], description='desc', table=table)
        res = dynamic_table_region[0:3, 'foo']
        self.assertListEqual(res, [1, 2, 3])
        res = dynamic_table_region[1:3, 'baz']
        self.assertListEqual(res, ['dog', 'bird'])

    def test_dynamic_table_region_getitem_bad_index(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion(name='dtr', data=[0, 1, 2, 2], description='desc', table=table)
        with self.assertRaises(ValueError):
            _ = dynamic_table_region[True]

    def test_dynamic_table_region_table_prop(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion(name='dtr', data=[0, 1, 2, 2], description='desc', table=table)
        self.assertEqual(table, dynamic_table_region.table)

    def test_dynamic_table_region_set_table_prop(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion(name='dtr', data=[0, 1, 2, 2], description='desc')
        dynamic_table_region.table = table
        self.assertEqual(table, dynamic_table_region.table)

    def test_dynamic_table_region_set_table_prop_to_none(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion(name='dtr', data=[0, 1, 2, 2], description='desc', table=table)
        try:
            dynamic_table_region.table = None
        except AttributeError:
            self.fail("DynamicTableRegion table setter raised AttributeError unexpectedly!")

    @unittest.skip('we no longer check data contents for performance reasons')
    def test_dynamic_table_region_set_with_bad_data(self):
        table = self.with_columns_and_data()
        # index 5 is out of range
        dynamic_table_region = DynamicTableRegion(name='dtr', data=[5, 1], description='desc')
        with self.assertRaises(IndexError):
            dynamic_table_region.table = table
        self.assertIsNone(dynamic_table_region.table)

    def test_repr(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion(name='dtr', data=[1, 2, 2], description='desc', table=table)
        expected = """dtr hdmf.common.table.DynamicTableRegion at 0x%d
    Target table: with_columns_and_data hdmf.common.table.DynamicTable at 0x%d
"""
        expected = expected % (id(dynamic_table_region), id(table))
        self.assertEqual(str(dynamic_table_region), expected)

    def test_no_df_nested(self):
        table = self.with_columns_and_data()
        dynamic_table_region = DynamicTableRegion(name='dtr', data=[0, 1, 2, 2], description='desc', table=table)
        msg = 'DynamicTableRegion.get() with df=False and index=False is not yet supported.'
        with self.assertRaisesWith(ValueError, msg):
            dynamic_table_region.get(0, df=False, index=False)


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
        target_table = DynamicTable(name="target_table",
                                    description='example table to target with a DynamicTableRegion',
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
        table = DynamicTable(name="table_with_dtr",
                             description='a test table that has a DynamicTableRegion',
                             columns=columns)
        return table, target_table

    def setUp(self):
        self.table, self.target_table = self.make_tables()
        super().setUp()

    def setUpContainer(self):
        multi_container = SimpleMultiContainer(name='multi', containers=[self.target_table, self.table])
        return multi_container

    def _get(self, arg):
        mc = self.roundtripContainer()
        table = mc.containers['table_with_dtr']
        return table.get(arg)

    def _get_nested(self, arg):
        mc = self.roundtripContainer()
        table = mc.containers['table_with_dtr']
        return table.get(arg, index=False)

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
        with self.assertRaises(KeyError):
            self._getitem('boo')

    def _assert_two_elem_df(self, rec):
        columns = ['foo', 'bar', 'baz', 'dtr']
        data = [[1, 10.0, 'cat', 0],
                [2, 20.0, 'dog', 1]]
        exp = pd.DataFrame(data=data, columns=columns, index=pd.Series(name='id', data=[0, 1]))
        pd.testing.assert_frame_equal(rec, exp, check_dtype=False)

    def _assert_one_elem_df(self, rec):
        columns = ['foo', 'bar', 'baz', 'dtr']
        data = [[1, 10.0, 'cat', 0]]
        exp = pd.DataFrame(data=data, columns=columns, index=pd.Series(name='id', data=[0]))
        pd.testing.assert_frame_equal(rec, exp, check_dtype=False)

    def _assert_two_elem_df_nested(self, rec):
        nested_columns = ['qux', 'quz']
        nested_data = [['qux_1', 'quz_1'], ['qux_2', 'quz_2']]
        nested_df = pd.DataFrame(data=nested_data, columns=nested_columns, index=pd.Series(name='id', data=[0, 1]))

        columns = ['foo', 'bar', 'baz']
        data = [[1, 10.0, 'cat'],
                [2, 20.0, 'dog']]
        exp = pd.DataFrame(data=data, columns=columns, index=pd.Series(name='id', data=[0, 1]))

        # remove nested dataframe and test each df separately
        pd.testing.assert_frame_equal(rec['dtr'][0], nested_df.iloc[[0]])
        pd.testing.assert_frame_equal(rec['dtr'][1], nested_df.iloc[[1]])
        del rec['dtr']
        pd.testing.assert_frame_equal(rec, exp, check_dtype=False)

    def _assert_one_elem_df_nested(self, rec):
        nested_columns = ['qux', 'quz']
        nested_data = [['qux_1', 'quz_1'], ['qux_2', 'quz_2']]
        nested_df = pd.DataFrame(data=nested_data, columns=nested_columns, index=pd.Series(name='id', data=[0, 1]))

        columns = ['foo', 'bar', 'baz']
        data = [[1, 10.0, 'cat']]
        exp = pd.DataFrame(data=data, columns=columns, index=pd.Series(name='id', data=[0]))

        # remove nested dataframe and test each df separately
        pd.testing.assert_frame_equal(rec['dtr'][0], nested_df.iloc[[0]])
        del rec['dtr']
        pd.testing.assert_frame_equal(rec, exp, check_dtype=False)

    #####################
    # tests DynamicTableRegion.__getitem__
    def test_getitem_int(self):
        rec = self._getitem(0)
        self._assert_one_elem_df(rec)

    def test_getitem_list_single(self):
        rec = self._getitem([0])
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

    def test_get_list_single(self):
        rec = self._get([0])
        self._assert_one_elem_df(rec)

    def test_get_list(self):
        rec = self._get([0, 1])
        self._assert_two_elem_df(rec)

    def test_get_slice(self):
        rec = self._get(slice(0, 2, None))
        self._assert_two_elem_df(rec)

    #####################
    # tests DynamicTableRegion.get, return a DataFrame with nested DataFrame
    def test_get_nested_int(self):
        rec = self._get_nested(0)
        self._assert_one_elem_df_nested(rec)

    def test_get_nested_list_single(self):
        rec = self._get_nested([0])
        self._assert_one_elem_df_nested(rec)

    def test_get_nested_list(self):
        rec = self._get_nested([0, 1])
        self._assert_two_elem_df_nested(rec)

    def test_get_nested_slice(self):
        rec = self._get_nested(slice(0, 2, None))
        self._assert_two_elem_df_nested(rec)

    #####################
    # tests DynamicTableRegion.get, DO NOT return a DataFrame
    def test_get_nodf_int(self):
        rec = self._get_nodf(0)
        exp = [0, 1, 10.0, 'cat', 0]
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

    def test_get_nodf_list_single(self):
        rec = self._get_nodf([0])
        exp = [np.array([0]), np.array([1]), np.array([10.0]), np.array(['cat']), np.array([0])]
        self._assert_list_of_ndarray_equal(exp, rec)

    def test_get_nodf_list(self):
        rec = self._get_nodf([0, 1])
        exp = [np.array([0, 1]), np.array([1, 2]), np.array([10.0, 20.0]), np.array(['cat', 'dog']), np.array([0, 1])]
        self._assert_list_of_ndarray_equal(exp, rec)

    def test_get_nodf_slice(self):
        rec = self._get_nodf(slice(0, 2, None))
        exp = [np.array([0, 1]), np.array([1, 2]), np.array([10.0, 20.0]), np.array(['cat', 'dog']), np.array([0, 1])]
        self._assert_list_of_ndarray_equal(exp, rec)

    def test_getitem_int_str(self):
        """Test DynamicTableRegion.__getitem__ with (int, str)."""
        mc = self.roundtripContainer()
        table = mc.containers['table_with_dtr']
        rec = table['dtr'][0, 'qux']
        self.assertEqual(rec, 'qux_1')

    def test_getitem_str(self):
        """Test DynamicTableRegion.__getitem__ with str."""
        mc = self.roundtripContainer()
        table = mc.containers['table_with_dtr']
        rec = table['dtr']['qux']
        self.assertIs(rec, mc.containers['target_table']['qux'])


class TestElementIdentifiers(TestCase):

    def setUp(self):
        self.e = ElementIdentifiers(name='ids', data=[0, 1, 2, 3, 4])

    def test_identifier_search_single_list(self):
        a = (self.e == [1])
        np.testing.assert_array_equal(a, [1])

    def test_identifier_search_single_int(self):
        a = (self.e == 2)
        np.testing.assert_array_equal(a, [2])

    def test_identifier_search_single_list_not_found(self):
        a = (self.e == [10])
        np.testing.assert_array_equal(a, [])

    def test_identifier_search_single_int_not_found(self):
        a = (self.e == 10)
        np.testing.assert_array_equal(a, [])

    def test_identifier_search_single_list_all_match(self):
        a = (self.e == [1, 2, 3])
        np.testing.assert_array_equal(a, [1, 2, 3])

    def test_identifier_search_single_list_partial_match(self):
        a = (self.e == [1, 2, 10])
        np.testing.assert_array_equal(a, [1, 2])
        a = (self.e == [-1, 2, 10])
        np.testing.assert_array_equal(a, [2, ])

    def test_identifier_search_with_element_identifier(self):
        a = (self.e == ElementIdentifiers(name='ids', data=[1, 2, 10]))
        np.testing.assert_array_equal(a, [1, 2])

    def test_identifier_search_with_bad_ids(self):
        with self.assertRaises(TypeError):
            _ = (self.e == 0.1)
        with self.assertRaises(TypeError):
            _ = (self.e == 'test')


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
        with self.assertRaises(KeyError):
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
            table.add_column(name='col2', description='column #2',
                             table=DynamicTable(name='dummy', description='dummy'))
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

    def test_no_set_target_tables(self):
        """Test that the target table of a predefined DTR column is None."""
        table = SubTable(name='subtable', description='subtable description')
        self.assertIsNone(table.col5.table)

    def test_set_target_tables(self):
        """Test setting target tables for predefined DTR columns."""
        table1 = SubTable(name='subtable1', description='subtable description')
        table2 = SubTable(
            name='subtable2',
            description='subtable description',
            target_tables={
                'col5': table1,
                'col6': table1,
                'col7': table1,
                'col8': table1,
            },
        )
        self.assertIs(table2.col5.table, table1)
        self.assertIs(table2.col6.table, table1)
        self.assertIs(table2.col7.table, table1)
        self.assertIs(table2.col8.table, table1)

    def test_set_target_tables_unknown_col(self):
        """Test setting target tables for unknown columns."""
        table1 = SubTable(name='subtable1', description='subtable description')
        msg = r"'bad_col' is not the name of a predefined column of table subtable2 .*"
        with self.assertRaisesRegex(ValueError, msg):
            SubTable(
                name='subtable2',
                description='subtable description',
                target_tables={
                    'bad_col': table1,
                },
            )

    def test_set_target_tables_bad_init_col(self):
        """Test setting target tables for predefined, required non-DTR columns."""
        table1 = SubTable(name='subtable1', description='subtable description')
        msg = "Column 'col1' must be a DynamicTableRegion to have a target table."
        with self.assertRaisesWith(ValueError, msg):
            SubTable(
                name='subtable2',
                description='subtable description',
                target_tables={
                    'col1': table1,
                },
            )

    def test_set_target_tables_bad_opt_col(self):
        """Test setting target tables for predefined, optional non-DTR columns."""
        table1 = SubTable(name='subtable1', description='subtable description')
        msg = "Column 'col2' must be a DynamicTableRegion to have a target table."
        with self.assertRaisesWith(ValueError, msg):
            SubTable(
                name='subtable2',
                description='subtable description',
                target_tables={
                    'col2': table1,
                },
            )

    def test_set_target_tables_existing_col_mismatch(self):
        """Test setting target tables for an existing DTR column with a mismatched, existing target table."""
        table1 = SubTable(name='subtable1', description='subtable description')
        table2 = SubTable(name='subtable2', description='subtable description')
        dtr = DynamicTableRegion(name='dtr', data=[], description='desc', table=table1)
        msg = "Column 'dtr' already has a target table that is not the passed table."
        with self.assertRaisesWith(ValueError, msg):
            SubTable(
                name='subtable3',
                description='subtable description',
                columns=[dtr],
                target_tables={
                    'dtr': table2,
                },
            )

    def test_set_target_tables_existing_col_match(self):
        """Test setting target tables for an existing DTR column with a matching, existing target table."""
        table1 = SubTable(name='subtable1', description='subtable description')
        dtr = DynamicTableRegion(name='dtr', data=[], description='desc', table=table1)
        SubTable(
            name='subtable2',
            description='subtable description',
            columns=[dtr],
            target_tables={
                'dtr': table1,
            },
        )


class TestEnumData(TestCase):

    def test_init(self):
        ed = EnumData(name='cv_data', description='a test EnumData', elements=['a', 'b', 'c'],
                      data=np.array([0, 0, 1, 1, 2, 2]))
        self.assertIsInstance(ed.elements, VectorData)

    def test_get(self):
        ed = EnumData(name='cv_data', description='a test EnumData', elements=['a', 'b', 'c'],
                      data=np.array([0, 0, 1, 1, 2, 2]))
        dat = ed[2]
        self.assertEqual(dat, 'b')
        dat = ed[-1]
        self.assertEqual(dat, 'c')
        dat = ed[0]
        self.assertEqual(dat, 'a')

    def test_get_list(self):
        ed = EnumData(name='cv_data', description='a test EnumData', elements=['a', 'b', 'c'],
                      data=np.array([0, 0, 1, 1, 2, 2]))
        dat = ed[[0, 1, 2]]
        np.testing.assert_array_equal(dat, ['a', 'a', 'b'])

    def test_get_list_join(self):
        ed = EnumData(name='cv_data', description='a test EnumData', elements=['a', 'b', 'c'],
                      data=np.array([0, 0, 1, 1, 2, 2]))
        dat = ed.get([0, 1, 2], join=True)
        self.assertEqual(dat, 'aab')

    def test_get_list_indices(self):
        ed = EnumData(name='cv_data', description='a test EnumData', elements=['a', 'b', 'c'],
                      data=np.array([0, 0, 1, 1, 2, 2]))
        dat = ed.get([0, 1, 2], index=True)
        np.testing.assert_array_equal(dat, [0, 0, 1])

    def test_get_2d(self):
        ed = EnumData(name='cv_data', description='a test EnumData',
                      elements=['a', 'b', 'c'],
                      data=np.array([[0, 0], [1, 1], [2, 2]]))
        dat = ed[0]
        np.testing.assert_array_equal(dat, ['a', 'a'])

    def test_get_2d_w_2d(self):
        ed = EnumData(name='cv_data', description='a test EnumData',
                      elements=['a', 'b', 'c'],
                      data=np.array([[0, 0], [1, 1], [2, 2]]))
        dat = ed[[0, 1]]
        np.testing.assert_array_equal(dat, [['a', 'a'], ['b', 'b']])

    def test_add_row(self):
        ed = EnumData(name='cv_data', description='a test EnumData', elements=['a', 'b', 'c'])
        ed.add_row('b')
        ed.add_row('a')
        ed.add_row('c')
        np.testing.assert_array_equal(ed.data, np.array([1, 0, 2], dtype=np.uint8))

    def test_add_row_index(self):
        ed = EnumData(name='cv_data', description='a test EnumData', elements=['a', 'b', 'c'])
        ed.add_row(1, index=True)
        ed.add_row(0, index=True)
        ed.add_row(2, index=True)
        np.testing.assert_array_equal(ed.data, np.array([1, 0, 2], dtype=np.uint8))


class TestIndexedEnumData(TestCase):

    def test_init(self):
        ed = EnumData(name='cv_data', description='a test EnumData',
                      elements=['a', 'b', 'c'], data=np.array([0, 0, 1, 1, 2, 2]))
        idx = VectorIndex(name='enum_index', data=[2, 4, 6], target=ed)
        self.assertIsInstance(ed.elements, VectorData)
        self.assertIsInstance(idx.target, EnumData)

    def test_add_row(self):
        ed = EnumData(name='cv_data', description='a test EnumData', elements=['a', 'b', 'c'])
        idx = VectorIndex(name='enum_index', data=list(), target=ed)
        idx.add_row(['a', 'a', 'a'])
        idx.add_row(['b', 'b'])
        idx.add_row(['c', 'c', 'c', 'c'])
        np.testing.assert_array_equal(idx[0], ['a', 'a', 'a'])
        np.testing.assert_array_equal(idx[1], ['b', 'b'])
        np.testing.assert_array_equal(idx[2], ['c', 'c', 'c', 'c'])

    def test_add_row_index(self):
        ed = EnumData(name='cv_data', description='a test EnumData', elements=['a', 'b', 'c'])
        idx = VectorIndex(name='enum_index', data=list(), target=ed)
        idx.add_row([0, 0, 0], index=True)
        idx.add_row([1, 1], index=True)
        idx.add_row([2, 2, 2, 2], index=True)
        np.testing.assert_array_equal(idx[0], ['a', 'a', 'a'])
        np.testing.assert_array_equal(idx[1], ['b', 'b'])
        np.testing.assert_array_equal(idx[2], ['c', 'c', 'c', 'c'])

    @unittest.skip("feature is not yet supported")
    def test_add_2d_row_index(self):
        ed = EnumData(name='cv_data', description='a test EnumData', elements=['a', 'b', 'c'])
        idx = VectorIndex(name='enum_index', data=list(), target=ed)
        idx.add_row([['a', 'a'], ['a', 'a'], ['a', 'a']])
        idx.add_row([['b', 'b'], ['b', 'b']])
        idx.add_row([['c', 'c'], ['c', 'c'], ['c', 'c'], ['c', 'c']])
        np.testing.assert_array_equal(idx[0], [['a', 'a'], ['a', 'a'], ['a', 'a']])
        np.testing.assert_array_equal(idx[1], [['b', 'b'], ['b', 'b']])
        np.testing.assert_array_equal(idx[2], [['c', 'c'], ['c', 'c'], ['c', 'c'], ['c', 'c']])


class SelectionTestMixin:

    def setUp(self):
        # table1 contains a non-ragged DTR and a ragged DTR, both of which point to table2
        # table2 contains a non-ragged DTR and a ragged DTR, both of which point to table3
        self.table3 = DynamicTable(
            name='table3',
            description='a test table',
            id=[20, 21, 22]
        )
        self.table3.add_column('foo', 'scalar column', data=self._wrap([20.0, 21.0, 22.0]))
        self.table3.add_column('bar', 'ragged column', index=self._wrap([2, 3, 6]),
                               data=self._wrap(['t11', 't12', 't21', 't31', 't32', 't33']))
        self.table3.add_column('baz', 'multi-dimension column',
                               data=self._wrap([[210.0, 211.0, 212.0],
                                                [220.0, 221.0, 222.0],
                                                [230.0, 231.0, 232.0]]))
        # generate expected dataframe for table3
        data = OrderedDict()
        data['foo'] = [20.0, 21.0, 22.0]
        data['bar'] = [['t11', 't12'], ['t21'], ['t31', 't32', 't33']]
        data['baz'] = [[210.0, 211.0, 212.0], [220.0, 221.0, 222.0], [230.0, 231.0, 232.0]]
        idx = [20, 21, 22]
        self.table3_df = pd.DataFrame(data=data, index=pd.Index(name='id', data=idx))

        self.table2 = DynamicTable(
            name='table2',
            description='a test table',
            id=[10, 11, 12]
        )
        self.table2.add_column('foo', 'scalar column', data=self._wrap([10.0, 11.0, 12.0]))
        self.table2.add_column('bar', 'ragged column', index=self._wrap([2, 3, 6]),
                               data=self._wrap(['s11', 's12', 's21', 's31', 's32', 's33']))
        self.table2.add_column('baz', 'multi-dimension column',
                               data=self._wrap([[110.0, 111.0, 112.0],
                                                [120.0, 121.0, 122.0],
                                                [130.0, 131.0, 132.0]]))
        self.table2.add_column('qux', 'DTR column', table=self.table3, data=self._wrap([0, 1, 0]))
        self.table2.add_column('corge', 'ragged DTR column', index=self._wrap([2, 3, 6]), table=self.table3,
                               data=self._wrap([0, 1, 2, 0, 1, 2]))
        # TODO test when ragged DTR indices are not in ascending order

        # generate expected dataframe for table2 *without DTR*
        data = OrderedDict()
        data['foo'] = [10.0, 11.0, 12.0]
        data['bar'] = [['s11', 's12'], ['s21'], ['s31', 's32', 's33']]
        data['baz'] = [[110.0, 111.0, 112.0], [120.0, 121.0, 122.0], [130.0, 131.0, 132.0]]
        idx = [10, 11, 12]
        self.table2_df = pd.DataFrame(data=data, index=pd.Index(name='id', data=idx))

        self.table1 = DynamicTable(
            name='table1',
            description='a table to test slicing',
            id=[0, 1]
        )
        self.table1.add_column('foo', 'scalar column', data=self._wrap([0.0, 1.0]))
        self.table1.add_column('bar', 'ragged column', index=self._wrap([2, 3]),
                               data=self._wrap(['r11', 'r12', 'r21']))
        self.table1.add_column('baz', 'multi-dimension column',
                               data=self._wrap([[10.0, 11.0, 12.0],
                                                [20.0, 21.0, 22.0]]))
        self.table1.add_column('qux', 'DTR column', table=self.table2, data=self._wrap([0, 1]))
        self.table1.add_column('corge', 'ragged DTR column', index=self._wrap([2, 3]), table=self.table2,
                               data=self._wrap([0, 1, 2]))
        self.table1.add_column('barz', 'ragged column of tuples (cpd type)', index=self._wrap([2, 3]),
                               data=self._wrap([(1.0, 11), (2.0, 12), (3.0, 21)]))

        # generate expected dataframe for table1 *without DTR*
        data = OrderedDict()
        data['foo'] = self._wrap_check([0.0, 1.0])
        data['bar'] = [self._wrap_check(['r11', 'r12']), self._wrap_check(['r21'])]
        data['baz'] = [self._wrap_check([10.0, 11.0, 12.0]),
                       self._wrap_check([20.0, 21.0, 22.0])]
        data['barz'] = [self._wrap_check([(1.0, 11), (2.0, 12)]), self._wrap_check([(3.0, 21)])]
        idx = [0, 1]
        self.table1_df = pd.DataFrame(data=data, index=pd.Index(name='id', data=idx))

    def _check_two_rows_df(self, rec):
        data = OrderedDict()
        data['foo'] = self._wrap_check([0.0, 1.0])
        data['bar'] = [self._wrap_check(['r11', 'r12']), self._wrap_check(['r21'])]
        data['baz'] = [self._wrap_check([10.0, 11.0, 12.0]),
                       self._wrap_check([20.0, 21.0, 22.0])]
        data['qux'] = self._wrap_check([0, 1])
        data['corge'] = [self._wrap_check([0, 1]), self._wrap_check([2])]
        data['barz'] = [self._wrap_check([(1.0, 11), (2.0, 12)]), self._wrap_check([(3.0, 21)])]
        idx = [0, 1]
        exp = pd.DataFrame(data=data, index=pd.Index(name='id', data=idx))
        pd.testing.assert_frame_equal(rec, exp)

    def _check_two_rows_df_nested(self, rec):
        # first level: cache nested df cols and remove them before calling pd.testing.assert_frame_equal
        qux_series = rec['qux']
        corge_series = rec['corge']
        del rec['qux']
        del rec['corge']

        idx = [0, 1]
        pd.testing.assert_frame_equal(rec, self.table1_df.loc[idx])

        # second level: compare the nested columns separately
        self.assertEqual(len(qux_series), 2)
        rec_qux1 = qux_series[0]
        rec_qux2 = qux_series[1]
        self._check_table2_first_row_qux(rec_qux1)
        self._check_table2_second_row_qux(rec_qux2)

        self.assertEqual(len(corge_series), 2)
        rec_corge1 = corge_series[0]
        rec_corge2 = corge_series[1]
        self._check_table2_first_row_corge(rec_corge1)
        self._check_table2_second_row_corge(rec_corge2)

    def _check_one_row_df(self, rec):
        data = OrderedDict()
        data['foo'] = self._wrap_check([0.0])
        data['bar'] = [self._wrap_check(['r11', 'r12'])]
        data['baz'] = [self._wrap_check([10.0, 11.0, 12.0])]
        data['qux'] = self._wrap_check([0])
        data['corge'] = [self._wrap_check([0, 1])]
        data['barz'] = [self._wrap_check([(1.0, 11), (2.0, 12)])]
        idx = [0]
        exp = pd.DataFrame(data=data, index=pd.Index(name='id', data=idx))
        pd.testing.assert_frame_equal(rec, exp)

    def _check_one_row_df_nested(self, rec):
        # first level: cache nested df cols and remove them before calling pd.testing.assert_frame_equal
        qux_series = rec['qux']
        corge_series = rec['corge']
        del rec['qux']
        del rec['corge']

        idx = [0]
        pd.testing.assert_frame_equal(rec, self.table1_df.loc[idx])

        # second level: compare the nested columns separately
        self.assertEqual(len(qux_series), 1)
        rec_qux = qux_series[0]
        self._check_table2_first_row_qux(rec_qux)

        self.assertEqual(len(corge_series), 1)
        rec_corge = corge_series[0]
        self._check_table2_first_row_corge(rec_corge)

    def _check_table2_first_row_qux(self, rec_qux):
        # second level: cache nested df cols and remove them before calling pd.testing.assert_frame_equal
        qux_qux_series = rec_qux['qux']
        qux_corge_series = rec_qux['corge']
        del rec_qux['qux']
        del rec_qux['corge']

        qux_idx = [10]
        pd.testing.assert_frame_equal(rec_qux, self.table2_df.loc[qux_idx])

        # third level: compare the nested columns separately
        self.assertEqual(len(qux_qux_series), 1)
        pd.testing.assert_frame_equal(qux_qux_series[qux_idx[0]], self.table3_df.iloc[[0]])
        self.assertEqual(len(qux_corge_series), 1)
        pd.testing.assert_frame_equal(qux_corge_series[qux_idx[0]], self.table3_df.iloc[[0, 1]])

    def _check_table2_second_row_qux(self, rec_qux):
        # second level: cache nested df cols and remove them before calling pd.testing.assert_frame_equal
        qux_qux_series = rec_qux['qux']
        qux_corge_series = rec_qux['corge']
        del rec_qux['qux']
        del rec_qux['corge']

        qux_idx = [11]
        pd.testing.assert_frame_equal(rec_qux, self.table2_df.loc[qux_idx])

        # third level: compare the nested columns separately
        self.assertEqual(len(qux_qux_series), 1)
        pd.testing.assert_frame_equal(qux_qux_series[qux_idx[0]], self.table3_df.iloc[[1]])
        self.assertEqual(len(qux_corge_series), 1)
        pd.testing.assert_frame_equal(qux_corge_series[qux_idx[0]], self.table3_df.iloc[[2]])

    def _check_table2_first_row_corge(self, rec_corge):
        # second level: cache nested df cols and remove them before calling pd.testing.assert_frame_equal
        corge_qux_series = rec_corge['qux']
        corge_corge_series = rec_corge['corge']
        del rec_corge['qux']
        del rec_corge['corge']

        corge_idx = [10, 11]
        pd.testing.assert_frame_equal(rec_corge, self.table2_df.loc[corge_idx])

        # third level: compare the nested columns separately
        self.assertEqual(len(corge_qux_series), 2)
        pd.testing.assert_frame_equal(corge_qux_series[corge_idx[0]], self.table3_df.iloc[[0]])
        pd.testing.assert_frame_equal(corge_qux_series[corge_idx[1]], self.table3_df.iloc[[1]])
        self.assertEqual(len(corge_corge_series), 2)
        pd.testing.assert_frame_equal(corge_corge_series[corge_idx[0]], self.table3_df.iloc[[0, 1]])
        pd.testing.assert_frame_equal(corge_corge_series[corge_idx[1]], self.table3_df.iloc[[2]])

    def _check_table2_second_row_corge(self, rec_corge):
        # second level: cache nested df cols and remove them before calling pd.testing.assert_frame_equal
        corge_qux_series = rec_corge['qux']
        corge_corge_series = rec_corge['corge']
        del rec_corge['qux']
        del rec_corge['corge']

        corge_idx = [12]
        pd.testing.assert_frame_equal(rec_corge, self.table2_df.loc[corge_idx])

        # third level: compare the nested columns separately
        self.assertEqual(len(corge_qux_series), 1)
        pd.testing.assert_frame_equal(corge_qux_series[corge_idx[0]], self.table3_df.iloc[[0]])
        self.assertEqual(len(corge_corge_series), 1)
        pd.testing.assert_frame_equal(corge_corge_series[corge_idx[0]], self.table3_df.iloc[[0, 1, 2]])

    def _check_two_rows_no_df(self, rec):
        self.assertEqual(rec[0], [0, 1])
        np.testing.assert_array_equal(rec[1], self._wrap_check([0.0, 1.0]))
        expected = [self._wrap_check(['r11', 'r12']), self._wrap_check(['r21'])]
        self._assertNestedRaggedArrayEqual(rec[2], expected)
        np.testing.assert_array_equal(rec[3], self._wrap_check([[10.0, 11.0, 12.0], [20.0, 21.0, 22.0]]))
        np.testing.assert_array_equal(rec[4], self._wrap_check([0, 1]))
        expected = [self._wrap_check([0, 1]), self._wrap_check([2])]
        for i, j in zip(rec[5], expected):
            np.testing.assert_array_equal(i, j)

    def _check_one_row_no_df(self, rec):
        self.assertEqual(rec[0], 0)
        self.assertEqual(rec[1], 0.0)
        np.testing.assert_array_equal(rec[2], self._wrap_check(['r11', 'r12']))
        np.testing.assert_array_equal(rec[3], self._wrap_check([10.0, 11.0, 12.0]))
        self.assertEqual(rec[4], 0)
        np.testing.assert_array_equal(rec[5], self._wrap_check([0, 1]))
        np.testing.assert_array_equal(rec[6], self._wrap_check([(1.0, 11), (2.0, 12)]))

    def _check_one_row_multiselect_no_df(self, rec):
        # difference from _check_one_row_no_df is that everything is wrapped in a list
        self.assertEqual(rec[0], [0])
        self.assertEqual(rec[1], [0.0])
        np.testing.assert_array_equal(rec[2], [self._wrap_check(['r11', 'r12'])])
        np.testing.assert_array_equal(rec[3], [self._wrap_check([10.0, 11.0, 12.0])])
        self.assertEqual(rec[4], [0])
        np.testing.assert_array_equal(rec[5], [self._wrap_check([0, 1])])
        np.testing.assert_array_equal(rec[6], [self._wrap_check([(1.0, 11), (2.0, 12)])])

    def _assertNestedRaggedArrayEqual(self, arr1, arr2):
        """
        This is a helper function for _check_two_rows_no_df.
        It compares arrays or lists containing numpy arrays that may be ragged
        """
        self.assertEqual(type(arr1), type(arr2))
        self.assertEqual(len(arr1), len(arr2))
        if isinstance(arr1, np.ndarray):
            if arr1.dtype == object:  # both are arrays containing arrays, lists, or h5py.Dataset strings
                for i, j in zip(arr1, arr2):
                    self._assertNestedRaggedArrayEqual(i, j)
            elif np.issubdtype(arr1.dtype, np.number):
                np.testing.assert_allclose(arr1, arr2)
            else:
                np.testing.assert_array_equal(arr1, arr2)
        elif isinstance(arr1, list):
            for i, j in zip(arr1, arr2):
                self._assertNestedRaggedArrayEqual(i, j)
        else:  # scalar
            self.assertEqual(arr1, arr2)

    def test_single_item(self):
        rec = self.table1[0]
        self._check_one_row_df(rec)

    def test_single_item_nested(self):
        rec = self.table1.get(0, index=False)
        self._check_one_row_df_nested(rec)

    def test_single_item_no_df(self):
        rec = self.table1.get(0, df=False)
        self._check_one_row_no_df(rec)

    def test_slice(self):
        rec = self.table1[0:2]
        self._check_two_rows_df(rec)

    def test_slice_nested(self):
        rec = self.table1.get(slice(0, 2), index=False)
        self._check_two_rows_df_nested(rec)

    def test_slice_no_df(self):
        rec = self.table1.get(slice(0, 2), df=False)
        self._check_two_rows_no_df(rec)

    def test_slice_single(self):
        rec = self.table1[0:1]
        self._check_one_row_df(rec)

    def test_slice_single_nested(self):
        rec = self.table1.get(slice(0, 1), index=False)
        self._check_one_row_df_nested(rec)

    def test_slice_single_no_df(self):
        rec = self.table1.get(slice(0, 1), df=False)
        self._check_one_row_multiselect_no_df(rec)

    def test_list(self):
        rec = self.table1[[0, 1]]
        self._check_two_rows_df(rec)

    def test_list_nested(self):
        rec = self.table1.get([0, 1], index=False)
        self._check_two_rows_df_nested(rec)

    def test_list_no_df(self):
        rec = self.table1.get([0, 1], df=False)
        self._check_two_rows_no_df(rec)

    def test_list_single(self):
        rec = self.table1[[0]]
        self._check_one_row_df(rec)

    def test_list_single_nested(self):
        rec = self.table1.get([0], index=False)
        self._check_one_row_df_nested(rec)

    def test_list_single_no_df(self):
        rec = self.table1.get([0], df=False)
        self._check_one_row_multiselect_no_df(rec)

    def test_array(self):
        rec = self.table1[np.array([0, 1])]
        self._check_two_rows_df(rec)

    def test_array_nested(self):
        rec = self.table1.get(np.array([0, 1]), index=False)
        self._check_two_rows_df_nested(rec)

    def test_array_no_df(self):
        rec = self.table1.get(np.array([0, 1]), df=False)
        self._check_two_rows_no_df(rec)

    def test_array_single(self):
        rec = self.table1[np.array([0])]
        self._check_one_row_df(rec)

    def test_array_single_nested(self):
        rec = self.table1.get(np.array([0]), index=False)
        self._check_one_row_df_nested(rec)

    def test_array_single_no_df(self):
        rec = self.table1.get(np.array([0]), df=False)
        self._check_one_row_multiselect_no_df(rec)

    def test_to_dataframe_nested(self):
        rec = self.table1.to_dataframe()
        self._check_two_rows_df_nested(rec)

    def test_to_dataframe(self):
        rec = self.table1.to_dataframe(index=True)
        self._check_two_rows_df(rec)


class TestSelectionArray(SelectionTestMixin, TestCase):

    def _wrap(self, my_list):
        return np.array(my_list)

    def _wrap_check(self, my_list):
        return self._wrap(my_list)


class TestSelectionList(SelectionTestMixin, TestCase):

    def _wrap(self, my_list):
        return my_list

    def _wrap_check(self, my_list):
        return self._wrap(my_list)


class TestSelectionH5Dataset(SelectionTestMixin, TestCase):

    def setUp(self):
        self.path = get_temp_filepath()
        self.file = h5py.File(self.path, 'w')
        self.dset_counter = 0
        super().setUp()

    def tearDown(self):
        super().tearDown()
        self.file.close()
        if os.path.exists(self.path):
            os.remove(self.path)

    def _wrap(self, my_list):
        self.dset_counter = self.dset_counter + 1
        kwargs = dict()
        if isinstance(my_list[0], str):
            kwargs['dtype'] = H5_TEXT
        elif isinstance(my_list[0], tuple):  # compound dtype
            # normally for cpd dtype, __resolve_dtype__ takes a list of DtypeSpec objects
            cpd_type = [dict(name='cpd_float', dtype=np.dtype('float64')),
                        dict(name='cpd_int', dtype=np.dtype('int32'))]
            kwargs['dtype'] = HDF5IO.__resolve_dtype__(cpd_type, my_list[0])
        dset = self.file.create_dataset('dset%d' % self.dset_counter, data=np.array(my_list, **kwargs))
        if H5PY_3 and isinstance(my_list[0], str):
            return StrDataset(dset, None)  # return a wrapper to read data as str instead of bytes
        else:
            # NOTE: h5py.Dataset with compound dtype are read as numpy arrays with compound dtype, not tuples
            return dset

    def _wrap_check(self, my_list):
        # getitem on h5dataset backed data will return np.array
        kwargs = dict()
        if isinstance(my_list[0], str):
            kwargs['dtype'] = H5_TEXT
        elif isinstance(my_list[0], tuple):
            cpd_type = [dict(name='cpd_float', dtype=np.dtype('float64')),
                        dict(name='cpd_int', dtype=np.dtype('int32'))]
            kwargs['dtype'] = np.dtype([(x['name'], x['dtype']) for x in cpd_type])
            # compound dtypes with str are read as bytes, see https://github.com/h5py/h5py/issues/1751
        return np.array(my_list, **kwargs)


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

        table = DynamicTable(name='table0', description='an example table', columns=[foo, foo_ind, foo_ind_ind])

        self.assertIs(table['foo'], foo_ind_ind)
        self.assertIs(table.foo, foo)
        self.assertListEqual(table['foo'][0], [['a11', 'a12'], ['a21']])
        self.assertListEqual(table[0, 'foo'], [['a11', 'a12'], ['a21']])
        self.assertListEqual(table[1, 'foo'], [['b11']])

    def test_double_index_reverse(self):
        foo = VectorData(name='foo', description='foo column', data=['a11', 'a12', 'a21', 'b11'])
        foo_ind = VectorIndex(name='foo_index', target=foo, data=[2, 3, 4])
        foo_ind_ind = VectorIndex(name='foo_index_index', target=foo_ind, data=[2, 3])

        table = DynamicTable(name='table0', description='an example table', columns=[foo_ind_ind, foo_ind, foo])

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

        table = DynamicTable(name='table0', description='an example table', columns=[foo, foo_ind, foo_ind_ind, bar],
                             colnames=['foo', 'bar'])

        self.assertTupleEqual(table.columns, (foo_ind_ind, foo_ind, foo, bar))

    def test_double_index_reverse_colnames(self):
        foo = VectorData(name='foo', description='foo column', data=['a11', 'a12', 'a21', 'b11'])
        foo_ind = VectorIndex(name='foo_index', target=foo, data=[2, 3, 4])
        foo_ind_ind = VectorIndex(name='foo_index_index', target=foo_ind, data=[2, 3])
        bar = VectorData(name='bar', description='bar column', data=[1, 2])

        table = DynamicTable(name='table0', description='an example table', columns=[foo_ind_ind, foo_ind, foo, bar],
                             colnames=['bar', 'foo'])

        self.assertTupleEqual(table.columns, (bar, foo_ind_ind, foo_ind, foo))


class TestDTDoubleIndexSkipMiddle(TestCase):

    def test_index(self):
        foo = VectorData(name='foo', description='foo column', data=['a11', 'a12', 'a21', 'b11'])
        foo_ind = VectorIndex(name='foo_index', target=foo, data=[2, 3, 4])
        foo_ind_ind = VectorIndex(name='foo_index_index', target=foo_ind, data=[2, 3])

        msg = "Found VectorIndex 'foo_index_index' but not its target 'foo_index'"
        with self.assertRaisesWith(ValueError, msg):
            DynamicTable(name='table0', description='an example table', columns=[foo_ind_ind, foo])


class TestDynamicTableAddIndexRoundTrip(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        table = DynamicTable(name='table0', description='an example table')
        table.add_column('foo', 'an int column', index=True)
        table.add_row(foo=[1, 2, 3])
        return table


class TestDynamicTableAddEnumRoundTrip(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        table = DynamicTable(name='table0', description='an example table')
        table.add_column('bar', 'an enumerable column', enum=True)
        table.add_row(bar='a')
        table.add_row(bar='b')
        table.add_row(bar='a')
        table.add_row(bar='c')
        return table


class TestDynamicTableAddEnum(TestCase):

    def test_enum(self):
        table = DynamicTable(name='table0', description='an example table')
        table.add_column('bar', 'an enumerable column', enum=True)
        table.add_row(bar='a')
        table.add_row(bar='b')
        table.add_row(bar='a')
        table.add_row(bar='c')
        rec = table.to_dataframe()
        exp = pd.DataFrame(data={'bar': ['a', 'b', 'a', 'c']}, index=pd.Series(name='id', data=[0, 1, 2, 3]))
        pd.testing.assert_frame_equal(exp, rec)

    def test_enum_index(self):
        table = DynamicTable(name='table0', description='an example table')
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
        table = DynamicTable(name='table0', description='an example table', columns=[foo_ind, foo])
        return table


class TestDoubleIndexRoundtrip(H5RoundTripMixin, TestCase):

    def setUpContainer(self):
        foo = VectorData(name='foo', description='foo column', data=['a11', 'a12', 'a21', 'b11'])
        foo_ind = VectorIndex(name='foo_index', target=foo, data=[2, 3, 4])
        foo_ind_ind = VectorIndex(name='foo_index_index', target=foo_ind, data=[2, 3])

        # NOTE: on construct, columns are ordered such that indices go before data, so create the table that way
        # for proper comparison of the columns list
        table = DynamicTable(name='table0', description='an example table', columns=[foo_ind_ind, foo_ind, foo])
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
        table = DynamicTable(name='table0', description='an example table', columns=[foo, bar])
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
        table = DynamicTable(name='table0', description='an example table', columns=[foo_ind, foo, bar_ind, bar])

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
        table = DynamicTable(name='table0', description='an example table', columns=[foo_ind, foo, bar_ind, bar],
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

        multi_container = SimpleMultiContainer(name='multi')
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


class TestVectorIndexDtype(TestCase):

    def set_up_array_index(self):
        data = VectorData(name='data', description='desc')
        index = VectorIndex(name='index', data=np.array([]), target=data)
        return index

    def set_up_list_index(self):
        data = VectorData(name='data', description='desc')
        index = VectorIndex(name='index', data=[], target=data)
        return index

    def test_array_inc_precision(self):
        index = self.set_up_array_index()
        index.add_vector(np.empty((255, )))
        self.assertEqual(index.data[0], 255)
        self.assertEqual(index.data.dtype, np.uint8)

    def test_array_inc_precision_1step(self):
        index = self.set_up_array_index()
        index.add_vector(np.empty((65535, )))
        self.assertEqual(index.data[0], 65535)
        self.assertEqual(index.data.dtype, np.uint16)

    def test_array_inc_precision_2steps(self):
        index = self.set_up_array_index()
        index.add_vector(np.empty((65536, )))
        self.assertEqual(index.data[0], 65536)
        self.assertEqual(index.data.dtype, np.uint32)

    def test_array_prev_data_inc_precision_2steps(self):
        index = self.set_up_array_index()
        index.add_vector(np.empty((255, )))  # dtype is still uint8
        index.add_vector(np.empty((65536, )))
        self.assertEqual(index.data[0], 255)  # make sure the 255 is upgraded
        self.assertEqual(index.data.dtype, np.uint32)

    def test_list_inc_precision(self):
        index = self.set_up_list_index()
        index.add_vector(list(range(255)))
        self.assertEqual(index.data[0], 255)
        self.assertEqual(type(index.data[0]), np.uint8)

    def test_list_inc_precision_1step(self):
        index = self.set_up_list_index()
        index.add_vector(list(range(65535)))
        self.assertEqual(index.data[0], 65535)
        self.assertEqual(type(index.data[0]), np.uint16)

    def test_list_inc_precision_2steps(self):
        index = self.set_up_list_index()
        index.add_vector(list(range(65536)))
        self.assertEqual(index.data[0], 65536)
        self.assertEqual(type(index.data[0]), np.uint32)

    def test_list_prev_data_inc_precision_2steps(self):
        index = self.set_up_list_index()
        index.add_vector(list(range(255)))
        index.add_vector(list(range(65536 - 255)))
        self.assertEqual(index.data[0], 255)  # make sure the 255 is upgraded
        self.assertEqual(type(index.data[0]), np.uint32)
