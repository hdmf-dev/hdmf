"""
Module for testing functions specific to tables containing DynamicTableRegion columns
"""

import numpy as np
from hdmf.common import DynamicTable, AlignedDynamicTable, VectorData, DynamicTableRegion, VectorIndex
from hdmf.testing import TestCase
from hdmf.utils import docval, popargs, get_docval
from hdmf.common.hierarchicaltable import to_hierarchical_dataframe, drop_id_columns, flatten_column_index
from pandas.testing import assert_frame_equal


class DynamicTableSingleDTR(DynamicTable):
    """Test table class that references a single foreign table"""
    __columns__ = (
        {'name': 'child_table_ref1',
         'description': 'Column with a references to the next level in the hierarchy',
         'required': True,
         'index': True,
         'table': True},
    )

    @docval({'name': 'name', 'type': str, 'doc': 'The name of the table'},
            {'name': 'child_table1',
             'type': DynamicTable,
             'doc': 'the child DynamicTable this DynamicTableSingleDTR point to.'},
            *get_docval(DynamicTable.__init__, 'id', 'columns', 'colnames'))
    def __init__(self, **kwargs):
        # Define default name and description settings
        kwargs['description'] = (kwargs['name'] + " DynamicTableSingleDTR")
        child_table1 = popargs('child_table1', kwargs)
        # Initialize the DynamicTable
        super().__init__(**kwargs)
        if self['child_table_ref1'].target.table is None:
            self['child_table_ref1'].target.table = child_table1


class DynamicTableMultiDTR(DynamicTable):
    """Test table class that references multiple related tables"""
    __columns__ = (
        {'name': 'child_table_ref1',
         'description': 'Column with a references to the next level in the hierarchy',
         'required': True,
         'index': True,
         'table': True},
        {'name': 'child_table_ref2',
         'description': 'Column with a references to the next level in the hierarchy',
         'required': True,
         'index': True,
         'table': True},
    )

    @docval({'name': 'name', 'type': str, 'doc': 'The name of the table'},
            {'name': 'child_table1',
             'type': DynamicTable,
             'doc': 'the child DynamicTable this DynamicTableSingleDTR point to.'},
            {'name': 'child_table2',
             'type': DynamicTable,
             'doc': 'the child DynamicTable this DynamicTableSingleDTR point to.'},
            *get_docval(DynamicTable.__init__, 'id', 'columns', 'colnames'))
    def __init__(self, **kwargs):
        # Define default name and description settings
        kwargs['description'] = (kwargs['name'] + " DynamicTableSingleDTR")
        child_table1 = popargs('child_table1', kwargs)
        child_table2 = popargs('child_table2', kwargs)
        # Initialize the DynamicTable
        super().__init__(**kwargs)
        if self['child_table_ref1'].target.table is None:
            self['child_table_ref1'].target.table = child_table1
        if self['child_table_ref2'].target.table is None:
            self['child_table_ref2'].target.table = child_table2


class TestLinkedAlignedDynamicTables(TestCase):
    """
    Test functionality specific to AlignedDynamicTables containing DynamicTableRegion columns.

    Since these functions only implements front-end convenient functions for DynamicTable
    we do not need to worry about I/O here (that is tested elsewere), but it is sufficient if
    we test with container class. The only time I/O becomes relevant is on read in case that, e.g., a
    h5py.Dataset may behave differently than a numpy array.
    """
    def setUp(self):
        """
        Create basic set of linked tables consisting of

        aligned_table
           |
           +--> category0 ---> table_level_0_0
           |
           +--> category1 ---> table_level_0_1
        """
        # Level 0 0 table. I.e., first table on level 0
        self.table_level0_0 = DynamicTable(name='level0_0', description="level0_0 DynamicTable")
        self.table_level0_0.add_row(id=10)
        self.table_level0_0.add_row(id=11)
        self.table_level0_0.add_row(id=12)
        self.table_level0_0.add_row(id=13)
        self.table_level0_0.add_column(data=['tag1', 'tag2', 'tag2', 'tag1', 'tag3', 'tag4', 'tag5'],
                                       name='tags',
                                       description='custom tags',
                                       index=[1, 2, 4, 7])
        self.table_level0_0.add_column(data=np.arange(4),
                                       name='myid',
                                       description='custom ids',
                                       index=False)
        # Level 0 1 table. I.e., second table on level 0
        self.table_level0_1 = DynamicTable(name='level0_1', description="level0_1 DynamicTable")
        self.table_level0_1.add_row(id=14)
        self.table_level0_1.add_row(id=15)
        self.table_level0_1.add_row(id=16)
        self.table_level0_1.add_row(id=17)
        self.table_level0_1.add_column(data=['tag1', 'tag1', 'tag2', 'tag2', 'tag3', 'tag3', 'tag4'],
                                       name='tags',
                                       description='custom tags',
                                       index=[2, 4, 6, 7])
        self.table_level0_1.add_column(data=np.arange(4),
                                       name='myid',
                                       description='custom ids',
                                       index=False)

        # category 0 table
        self.category0 = DynamicTableSingleDTR(name='category0', child_table1=self.table_level0_0)
        self.category0.add_row(id=0, child_table_ref1=[0, ])
        self.category0.add_row(id=1, child_table_ref1=[1, 2])
        self.category0.add_row(id=1, child_table_ref1=[3, ])
        self.category0.add_column(data=[10, 11, 12],
                                  name='filter',
                                  description='filter value',
                                  index=False)

        # category 1 table
        self.category1 = DynamicTableSingleDTR(name='category1', child_table1=self.table_level0_1)
        self.category1.add_row(id=0, child_table_ref1=[0, 1])
        self.category1.add_row(id=1, child_table_ref1=[2, 3])
        self.category1.add_row(id=1, child_table_ref1=[1, 3])
        self.category1.add_column(data=[1, 2, 3],
                                  name='filter',
                                  description='filter value',
                                  index=False)
        # Aligned table
        self.aligned_table = AlignedDynamicTable(name='my_aligned_table',
                                                 description='my test table',
                                                 columns=[VectorData(name='a1', description='a1', data=np.arange(3)), ],
                                                 colnames=['a1', ],
                                                 category_tables=[self.category0, self.category1])

    def tearDown(self):
        del self.table_level0_0
        del self.table_level0_1
        del self.category0
        del self.category1
        del self.aligned_table

    def test_to_hierarchical_dataframe(self):
        """Test that converting an AlignedDynamicTable with links works"""
        hier_df = to_hierarchical_dataframe(self.aligned_table)
        self.assertListEqual(hier_df.columns.to_list(),
                             [('level0_0', 'id'), ('level0_0', 'tags'), ('level0_0', 'myid')])
        self.assertListEqual(hier_df.index.names,
                             [('my_aligned_table', 'id'), ('my_aligned_table', ('my_aligned_table', 'a1')),
                              ('my_aligned_table', ('category0', 'id')), ('my_aligned_table', ('category0', 'filter')),
                              ('my_aligned_table', ('category1', 'id')),
                              ('my_aligned_table', ('category1', 'child_table_ref1')),
                              ('my_aligned_table', ('category1', 'filter'))])
        self.assertListEqual(hier_df.index.to_list(),
                             [(0, 0, 0, 10, 0, (0, 1), 1),
                              (1, 1, 1, 11, 1, (2, 3), 2),
                              (1, 1, 1, 11, 1, (2, 3), 2),
                              (2, 2, 1, 12, 1, (1, 3), 3)])
        self.assertListEqual(hier_df[('level0_0', 'tags')].values.tolist(),
                             [['tag1'], ['tag2'], ['tag2', 'tag1'], ['tag3', 'tag4', 'tag5']])

    def test_has_foreign_columns_in_category_tables(self):
        """Test confirming working order for DynamicTableRegions in subtables"""
        self.assertTrue(self.aligned_table.has_foreign_columns())
        self.assertFalse(self.aligned_table.has_foreign_columns(ignore_category_tables=True))

    def test_has_foreign_columns_false(self):
        """Test false if there are no DynamicTableRegionColumns"""
        temp_table = DynamicTable(name='t1', description='t1',
                                  colnames=['c1', 'c2'],
                                  columns=[VectorData(name='c1', description='c1', data=np.arange(4)),
                                           VectorData(name='c2', description='c2', data=np.arange(4))])
        temp_aligned_table = AlignedDynamicTable(name='my_aligned_table',
                                                 description='my test table',
                                                 category_tables=[temp_table],
                                                 colnames=['a1', 'a2'],
                                                 columns=[VectorData(name='a1', description='c1', data=np.arange(4)),
                                                          VectorData(name='a2', description='c2', data=np.arange(4))])
        self.assertFalse(temp_aligned_table.has_foreign_columns())
        self.assertFalse(temp_aligned_table.has_foreign_columns(ignore_category_tables=True))

    def test_has_foreign_column_in_main_table(self):
        temp_table = DynamicTable(name='t1', description='t1',
                                  colnames=['c1', 'c2'],
                                  columns=[VectorData(name='c1', description='c1', data=np.arange(4)),
                                           VectorData(name='c2', description='c2', data=np.arange(4))])
        temp_aligned_table = AlignedDynamicTable(name='my_aligned_table',
                                                 description='my test table',
                                                 category_tables=[temp_table],
                                                 colnames=['a1', 'a2'],
                                                 columns=[VectorData(name='a1', description='c1', data=np.arange(4)),
                                                          DynamicTableRegion(name='a2', description='c2',
                                                                             data=np.arange(4), table=temp_table)])
        self.assertTrue(temp_aligned_table.has_foreign_columns())
        self.assertTrue(temp_aligned_table.has_foreign_columns(ignore_category_tables=True))

    def test_get_foreign_columns(self):
        # check without subcateogries
        foreign_cols = self.aligned_table.get_foreign_columns(ignore_category_tables=True)
        self.assertListEqual(foreign_cols, [])
        # check with subcateogries
        foreign_cols = self.aligned_table.get_foreign_columns()
        self.assertEqual(len(foreign_cols), 2)
        for i, v in enumerate([('category0', 'child_table_ref1'), ('category1', 'child_table_ref1')]):
            self.assertTupleEqual(foreign_cols[i], v)

    def test_get_foreign_columns_none(self):
        """Test false if there are no DynamicTableRegionColumns"""
        temp_table = DynamicTable(name='t1', description='t1',
                                  colnames=['c1', 'c2'],
                                  columns=[VectorData(name='c1', description='c1', data=np.arange(4)),
                                           VectorData(name='c2', description='c2', data=np.arange(4))])
        temp_aligned_table = AlignedDynamicTable(name='my_aligned_table',
                                                 description='my test table',
                                                 category_tables=[temp_table],
                                                 colnames=['a1', 'a2'],
                                                 columns=[VectorData(name='a1', description='c1', data=np.arange(4)),
                                                          VectorData(name='a2', description='c2', data=np.arange(4))])
        self.assertListEqual(temp_aligned_table.get_foreign_columns(), [])
        self.assertListEqual(temp_aligned_table.get_foreign_columns(ignore_category_tables=True), [])

    def test_get_foreign_column_in_main_and_category_table(self):
        temp_table0 = DynamicTable(name='t0', description='t1',
                                   colnames=['c1', 'c2'],
                                   columns=[VectorData(name='c1', description='c1', data=np.arange(4)),
                                            VectorData(name='c2', description='c2', data=np.arange(4))])
        temp_table = DynamicTable(name='t1', description='t1',
                                  colnames=['c1', 'c2'],
                                  columns=[VectorData(name='c1', description='c1', data=np.arange(4)),
                                           DynamicTableRegion(name='c2', description='c2',
                                                              data=np.arange(4), table=temp_table0)])
        temp_aligned_table = AlignedDynamicTable(name='my_aligned_table',
                                                 description='my test table',
                                                 category_tables=[temp_table],
                                                 colnames=['a1', 'a2'],
                                                 columns=[VectorData(name='a1', description='c1', data=np.arange(4)),
                                                          DynamicTableRegion(name='a2', description='c2',
                                                                             data=np.arange(4), table=temp_table)])
        # We should get both the DynamicTableRegion from the main table and the category 't1'
        self.assertListEqual(temp_aligned_table.get_foreign_columns(), [(None, 'a2'), ('t1', 'c2')])
        # We should only get the column from the main table
        self.assertListEqual(temp_aligned_table.get_foreign_columns(ignore_category_tables=True), [(None, 'a2')])

    def test_get_linked_tables(self):
        # check without subcateogries
        linked_table = self.aligned_table.get_linked_tables(ignore_category_tables=True)
        self.assertListEqual(linked_table, [])
        # check with subcateogries
        linked_tables = self.aligned_table.get_linked_tables()
        self.assertEqual(len(linked_tables), 2)
        self.assertTupleEqual((linked_tables[0].source_table.name,
                               linked_tables[0].source_column.name,
                               linked_tables[0].target_table.name),
                              ('category0', 'child_table_ref1', 'level0_0'))
        self.assertTupleEqual((linked_tables[1].source_table.name,
                               linked_tables[1].source_column.name,
                               linked_tables[1].target_table.name),
                              ('category1', 'child_table_ref1', 'level0_1'))

    def test_get_linked_tables_none(self):
        """Test false if there are no DynamicTableRegionColumns"""
        temp_table = DynamicTable(name='t1', description='t1',
                                  colnames=['c1', 'c2'],
                                  columns=[VectorData(name='c1', description='c1', data=np.arange(4)),
                                           VectorData(name='c2', description='c2', data=np.arange(4))])
        temp_aligned_table = AlignedDynamicTable(name='my_aligned_table',
                                                 description='my test table',
                                                 category_tables=[temp_table],
                                                 colnames=['a1', 'a2'],
                                                 columns=[VectorData(name='a1', description='c1', data=np.arange(4)),
                                                          VectorData(name='a2', description='c2', data=np.arange(4))])
        self.assertListEqual(temp_aligned_table.get_linked_tables(), [])
        self.assertListEqual(temp_aligned_table.get_linked_tables(ignore_category_tables=True), [])

    def test_get_linked_tables_complex_link(self):
        temp_table0 = DynamicTable(name='t0', description='t1',
                                   colnames=['c1', 'c2'],
                                   columns=[VectorData(name='c1', description='c1', data=np.arange(4)),
                                            VectorData(name='c2', description='c2', data=np.arange(4))])
        temp_table = DynamicTable(name='t1', description='t1',
                                  colnames=['c1', 'c2'],
                                  columns=[VectorData(name='c1', description='c1', data=np.arange(4)),
                                           DynamicTableRegion(name='c2', description='c2',
                                                              data=np.arange(4), table=temp_table0)])
        temp_aligned_table = AlignedDynamicTable(name='my_aligned_table',
                                                 description='my test table',
                                                 category_tables=[temp_table],
                                                 colnames=['a1', 'a2'],
                                                 columns=[VectorData(name='a1', description='c1', data=np.arange(4)),
                                                          DynamicTableRegion(name='a2', description='c2',
                                                                             data=np.arange(4), table=temp_table)])
        # NOTE: in this example templ_aligned_table both points to temp_table and at the
        #       same time contains temp_table as a category. This could lead to temp_table
        #       visited multiple times and we want to make sure this doesn't happen
        # We should get both the DynamicTableRegion from the main table and the category 't1'
        linked_tables = temp_aligned_table.get_linked_tables()
        self.assertEqual(len(linked_tables), 2)
        for i, v in enumerate([('my_aligned_table', 'a2', 't1'), ('t1', 'c2', 't0')]):
            self.assertTupleEqual((linked_tables[i].source_table.name,
                                   linked_tables[i].source_column.name,
                                   linked_tables[i].target_table.name), v)
        # Now, since our main table links to the category table the result should remain the same
        # even if we ignore the category table
        linked_tables = temp_aligned_table.get_linked_tables(ignore_category_tables=True)
        self.assertEqual(len(linked_tables), 2)
        for i, v in enumerate([('my_aligned_table', 'a2', 't1'), ('t1', 'c2', 't0')]):
            self.assertTupleEqual((linked_tables[i].source_table.name,
                                   linked_tables[i].source_column.name,
                                   linked_tables[i].target_table.name), v)

    def test_get_linked_tables_simple_link(self):
        temp_table0 = DynamicTable(name='t0', description='t1',
                                   colnames=['c1', 'c2'],
                                   columns=[VectorData(name='c1', description='c1', data=np.arange(4)),
                                            VectorData(name='c2', description='c2', data=np.arange(4))])
        temp_table = DynamicTable(name='t1', description='t1',
                                  colnames=['c1', 'c2'],
                                  columns=[VectorData(name='c1', description='c1', data=np.arange(4)),
                                           DynamicTableRegion(name='c2', description='c2',
                                                              data=np.arange(4), table=temp_table0)])
        temp_aligned_table = AlignedDynamicTable(name='my_aligned_table',
                                                 description='my test table',
                                                 category_tables=[temp_table],
                                                 colnames=['a1', 'a2'],
                                                 columns=[VectorData(name='a1', description='c1', data=np.arange(4)),
                                                          DynamicTableRegion(name='a2', description='c2',
                                                                             data=np.arange(4), table=temp_table0)])
        # NOTE: in this example temp_aligned_table and temp_table both point to temp_table0
        # We should get both the DynamicTableRegion from the main table and the category 't1'
        linked_tables = temp_aligned_table.get_linked_tables()
        self.assertEqual(len(linked_tables), 2)
        for i, v in enumerate([('my_aligned_table', 'a2', 't0'), ('t1', 'c2', 't0')]):
            self.assertTupleEqual((linked_tables[i].source_table.name,
                                   linked_tables[i].source_column.name,
                                   linked_tables[i].target_table.name), v)
        # Since no table ever link to our category temp_table we should only get the link from our
        # main table here, in contrast to what happens in the test_get_linked_tables_complex_link case
        linked_tables = temp_aligned_table.get_linked_tables()
        self.assertEqual(len(linked_tables), 2)
        for i, v in enumerate([('my_aligned_table', 'a2', 't0'), ]):
            self.assertTupleEqual((linked_tables[i].source_table.name,
                                   linked_tables[i].source_column.name,
                                   linked_tables[i].target_table.name), v)


class TestHierarchicalTable(TestCase):

    def setUp(self):
        """
        Create basic set of linked tables consisting of

        super_parent_table --->  parent_table ---> aligned_table
                                                        |
                                                        +--> category0
        """
        # Level 0 0 table. I.e., first table on level 0
        self.category0 = DynamicTable(name='level0_0', description="level0_0 DynamicTable")
        self.category0.add_row(id=10)
        self.category0.add_row(id=11)
        self.category0.add_row(id=12)
        self.category0.add_row(id=13)
        self.category0.add_column(data=['tag1', 'tag2', 'tag2', 'tag1', 'tag3', 'tag4', 'tag5'],
                                  name='tags',
                                  description='custom tags',
                                  index=[1, 2, 4, 7])
        self.category0.add_column(data=np.arange(4),
                                  name='myid',
                                  description='custom ids',
                                  index=False)

        # Aligned table
        self.aligned_table = AlignedDynamicTable(name='aligned_table',
                                                 description='parent_table',
                                                 columns=[VectorData(name='a1', description='a1', data=np.arange(4)), ],
                                                 colnames=['a1', ],
                                                 category_tables=[self.category0, ])

        # Parent table
        self.parent_table = DynamicTable(name='parent_table',
                                         description='parent_table',
                                         columns=[VectorData(name='p1', description='p1', data=np.arange(4)),
                                                  DynamicTableRegion(name='l1', description='l1',
                                                                     data=np.arange(4), table=self.aligned_table)])
        # Super-parent table
        dtr_sp = DynamicTableRegion(name='sl1', description='sl1', data=np.arange(4), table=self.parent_table)
        vi_dtr_sp = VectorIndex(name='sl1_index', data=[1, 2, 3], target=dtr_sp)
        self.super_parent_table = DynamicTable(name='super_parent_table',
                                               description='super_parent_table',
                                               columns=[VectorData(name='sp1', description='sp1', data=np.arange(3)),
                                                        dtr_sp, vi_dtr_sp])

    def tearDown(self):
        del self.category0
        del self.aligned_table
        del self.parent_table

    def test_to_hierarchical_dataframe_no_dtr_on_top_level(self):
        # Cover the case where our top dtr is flat (i.e., without a VectorIndex)
        dtr_sp = DynamicTableRegion(name='sl1', description='sl1', data=np.arange(4), table=self.parent_table)
        spttable = DynamicTable(name='super_parent_table',
                                description='super_parent_table',
                                columns=[VectorData(name='sp1', description='sp1', data=np.arange(4)), dtr_sp])
        hier_df = to_hierarchical_dataframe(spttable).reset_index()
        expected_columns = [('super_parent_table', 'id'), ('super_parent_table', 'sp1'),
                            ('parent_table', 'id'), ('parent_table', 'p1'),
                            ('aligned_table', 'id'),
                            ('aligned_table', ('aligned_table', 'a1')), ('aligned_table', ('level0_0', 'id')),
                            ('aligned_table', ('level0_0', 'tags')), ('aligned_table', ('level0_0', 'myid'))]
        self.assertListEqual(hier_df.columns.to_list(), expected_columns)

    def test_to_hierarchical_dataframe_indexed_dtr_on_last_level(self):
        # Parent table
        dtr_p1 = DynamicTableRegion(name='l1', description='l1', data=np.arange(4), table=self.aligned_table)
        vi_dtr_p1 = VectorIndex(name='sl1_index', data=[1, 2, 3], target=dtr_p1)
        p1 = DynamicTable(name='parent_table', description='parent_table',
                          columns=[VectorData(name='p1', description='p1', data=np.arange(3)), dtr_p1, vi_dtr_p1])
        # Super-parent table
        dtr_sp = DynamicTableRegion(name='sl1', description='sl1', data=np.arange(4), table=p1)
        vi_dtr_sp = VectorIndex(name='sl1_index', data=[1, 2, 3], target=dtr_sp)
        spt = DynamicTable(name='super_parent_table', description='super_parent_table',
                           columns=[VectorData(name='sp1', description='sp1', data=np.arange(3)), dtr_sp, vi_dtr_sp])
        hier_df = to_hierarchical_dataframe(spt).reset_index()
        expected_columns = [('super_parent_table', 'id'), ('super_parent_table', 'sp1'),
                            ('parent_table', 'id'), ('parent_table', 'p1'),
                            ('aligned_table', 'id'),
                            ('aligned_table', ('aligned_table', 'a1')), ('aligned_table', ('level0_0', 'id')),
                            ('aligned_table', ('level0_0', 'tags')), ('aligned_table', ('level0_0', 'myid'))]
        self.assertListEqual(hier_df.columns.to_list(), expected_columns)  # make sure we have the right columns
        self.assertListEqual(hier_df[('aligned_table', ('level0_0', 'tags'))].to_list(),
                             [['tag1'], ['tag2'], ['tag2', 'tag1']])

    def test_to_hierarchical_dataframe_indexed_data_nparray(self):
        # Test that we can convert a table that contains a VectorIndex column as regular data,
        # i.e., it is not our DynamicTableRegion column that is index but a regular data column.
        # In this test the data is defined as an numpy nd.array so that an nd.array is injected
        # into the MultiIndex of the table. As a numpy array is not hashable this would normally
        # create an error when creating the MultiIndex
        # Parent table
        dtr_p1 = DynamicTableRegion(name='l1', description='l1', data=np.arange(4), table=self.aligned_table)
        vi_dtr_p1 = VectorIndex(name='sl1_index', data=[1, 2, 3], target=dtr_p1)
        p1 = DynamicTable(name='parent_table', description='parent_table',
                          columns=[VectorData(name='p1', description='p1', data=np.arange(3)), dtr_p1, vi_dtr_p1])
        # Super-parent table
        dtr_sp = DynamicTableRegion(name='sl1', description='sl1', data=np.arange(3), table=p1)
        spt = DynamicTable(name='super_parent_table', description='super_parent_table',
                           columns=[VectorData(name='sp1', description='sp1', data=np.arange(3)), dtr_sp])
        spt.add_column(name='vic', description='vic', data=np.arange(9), index=[2, 4, 6])
        hier_df = to_hierarchical_dataframe(spt).reset_index()
        expected_columns = [('super_parent_table', 'id'), ('super_parent_table', 'sp1'), ('super_parent_table', 'vic'),
                            ('parent_table', 'id'), ('parent_table', 'p1'),
                            ('aligned_table', 'id'),
                            ('aligned_table', ('aligned_table', 'a1')), ('aligned_table', ('level0_0', 'id')),
                            ('aligned_table', ('level0_0', 'tags')), ('aligned_table', ('level0_0', 'myid'))]
        self.assertListEqual(hier_df.columns.to_list(), expected_columns)  # make sure we have the right columns
        self.assertListEqual(hier_df[('aligned_table', ('level0_0', 'tags'))].to_list(),
                             [['tag1'], ['tag2'], ['tag2', 'tag1']])

    def test_to_hierarchical_dataframe_indexed_data_list(self):
        # Test that we can convert a table that contains a VectorIndex column as regular data,
        # i.e., it is not our DynamicTableRegion column that is index but a regular data column.
        # In this test the data is defined as an list  so that a list is injected
        # into the MultiIndex of the table. As a list  is not hashable this would normally
        # create an error when creating the MultiIndex
        # Parent table
        dtr_p1 = DynamicTableRegion(name='l1', description='l1', data=np.arange(4), table=self.aligned_table)
        vi_dtr_p1 = VectorIndex(name='sl1_index', data=[1, 2, 3], target=dtr_p1)
        p1 = DynamicTable(name='parent_table', description='parent_table',
                          columns=[VectorData(name='p1', description='p1', data=np.arange(3)), dtr_p1, vi_dtr_p1])
        # Super-parent table
        dtr_sp = DynamicTableRegion(name='sl1', description='sl1', data=np.arange(3), table=p1)
        spt = DynamicTable(name='super_parent_table', description='super_parent_table',
                           columns=[VectorData(name='sp1', description='sp1', data=np.arange(3)), dtr_sp])
        spt.add_column(name='vic', description='vic', data=list(range(9)), index=list([2, 4, 6]))
        hier_df = to_hierarchical_dataframe(spt).reset_index()
        expected_columns = [('super_parent_table', 'id'), ('super_parent_table', 'sp1'), ('super_parent_table', 'vic'),
                            ('parent_table', 'id'), ('parent_table', 'p1'),
                            ('aligned_table', 'id'),
                            ('aligned_table', ('aligned_table', 'a1')), ('aligned_table', ('level0_0', 'id')),
                            ('aligned_table', ('level0_0', 'tags')), ('aligned_table', ('level0_0', 'myid'))]
        self.assertListEqual(hier_df.columns.to_list(), expected_columns)  # make sure we have the right columns
        self.assertListEqual(hier_df[('aligned_table', ('level0_0', 'tags'))].to_list(),
                             [['tag1'], ['tag2'], ['tag2', 'tag1']])

    def test_to_hierarchical_dataframe_empty_tables(self):
        # Setup empty tables with the following hierarchy
        # super_parent_table --->  parent_table --->  child_table
        a1 = DynamicTable(name='level0_0', description="level0_0 DynamicTable",
                          columns=[VectorData(name='l0', description='l0', data=[])])
        p1 = DynamicTable(name='parent_table', description='parent_table',
                          columns=[DynamicTableRegion(name='l1', description='l1', data=[], table=a1),
                                   VectorData(name='p1c', description='l0', data=[])])
        dtr_sp = DynamicTableRegion(name='sl1', description='sl1', data=np.arange(4), table=p1)
        vi_dtr_sp = VectorIndex(name='sl1_index', data=[], target=dtr_sp)
        spt = DynamicTable(name='super_parent_table', description='super_parent_table',
                           columns=[dtr_sp, vi_dtr_sp, VectorData(name='sptc', description='l0', data=[])])
        # Convert to hierarchical dataframe and make sure we get the right columns
        hier_df = to_hierarchical_dataframe(spt).reset_index()
        expected_columns = [('super_parent_table', 'id'), ('super_parent_table', 'sptc'),
                            ('parent_table', 'id'), ('parent_table', 'p1c'),
                            ('level0_0', 'id'), ('level0_0', 'l0')]
        self.assertListEqual(hier_df.columns.to_list(), expected_columns)

    def test_to_hierarchical_dataframe_multilevel(self):
        hier_df = to_hierarchical_dataframe(self.super_parent_table).reset_index()
        expected_cols = [('super_parent_table', 'id'), ('super_parent_table', 'sp1'),
                         ('parent_table', 'id'), ('parent_table', 'p1'),
                         ('aligned_table', 'id'),
                         ('aligned_table', ('aligned_table', 'a1')),
                         ('aligned_table', ('level0_0', 'id')),
                         ('aligned_table', ('level0_0', 'tags')),
                         ('aligned_table', ('level0_0', 'myid'))]
        # Check that we have all the columns
        self.assertListEqual(hier_df.columns.to_list(), expected_cols)
        # Spot-check the data in two columns
        self.assertListEqual(hier_df[('aligned_table', ('level0_0', 'tags'))].to_list(),
                             [['tag1'], ['tag2'], ['tag2', 'tag1']])
        self.assertListEqual(hier_df[('aligned_table', ('aligned_table', 'a1'))].to_list(), list(range(3)))

    def test_to_hierarchical_dataframe(self):
        hier_df = to_hierarchical_dataframe(self.parent_table)
        self.assertEqual(len(hier_df), 4)
        self.assertEqual(len(hier_df.columns), 5)
        self.assertEqual(len(hier_df.index.names), 2)
        columns = [('aligned_table',                    'id'),
                   ('aligned_table', ('aligned_table', 'a1')),
                   ('aligned_table',      ('level0_0', 'id')),
                   ('aligned_table',    ('level0_0', 'tags')),
                   ('aligned_table',    ('level0_0', 'myid'))]
        for i, c in enumerate(hier_df.columns):
            self.assertTupleEqual(c, columns[i])
        index_names = [('parent_table', 'id'), ('parent_table', 'p1')]
        self.assertListEqual(hier_df.index.names, index_names)
        self.assertListEqual(hier_df.index.to_list(), [(i, i) for i in range(4)])
        self.assertListEqual(hier_df[('aligned_table', ('aligned_table', 'a1'))].to_list(), list(range(4)))
        self.assertListEqual(hier_df[('aligned_table', ('level0_0', 'id'))].to_list(), list(range(10, 14)))
        self.assertListEqual(hier_df[('aligned_table', ('level0_0', 'myid'))].to_list(), list(range(4)))
        tags = [['tag1'], ['tag2'], ['tag2', 'tag1'], ['tag3', 'tag4', 'tag5']]
        for i, v in enumerate(hier_df[('aligned_table', ('level0_0', 'tags'))].to_list()):
            self.assertListEqual(v, tags[i])

    def test_to_hierarchical_dataframe_flat_table(self):
        hier_df = to_hierarchical_dataframe(self.category0)
        assert_frame_equal(hier_df, self.category0.to_dataframe())
        hier_df = to_hierarchical_dataframe(self.aligned_table)
        assert_frame_equal(hier_df, self.aligned_table.to_dataframe())

    def test_drop_id_columns(self):
        hier_df = to_hierarchical_dataframe(self.parent_table)
        cols = hier_df.columns.to_list()
        mod_df = drop_id_columns(hier_df, inplace=False)
        expected_cols = [('aligned_table', ('aligned_table', 'a1')),
                         ('aligned_table', ('level0_0', 'tags')),
                         ('aligned_table', ('level0_0', 'myid'))]
        self.assertListEqual(hier_df.columns.to_list(), cols)  # Test that no columns are dropped with inplace=False
        self.assertListEqual(mod_df.columns.to_list(), expected_cols)   # Assert that we got back a modified dataframe
        drop_id_columns(hier_df, inplace=True)
        self.assertListEqual(hier_df.columns.to_list(),
                             expected_cols)
        flat_df = to_hierarchical_dataframe(self.parent_table).reset_index(inplace=False)
        drop_id_columns(flat_df, inplace=True)
        self.assertListEqual(flat_df.columns.to_list(),
                             [('parent_table',                    'p1'),
                              ('aligned_table', ('aligned_table', 'a1')),
                              ('aligned_table',    ('level0_0', 'tags')),
                              ('aligned_table',    ('level0_0', 'myid'))])

    def test_flatten_column_index(self):
        hier_df = to_hierarchical_dataframe(self.parent_table).reset_index()
        cols = hier_df.columns.to_list()
        expexted_cols = [('parent_table', 'id'),
                         ('parent_table', 'p1'),
                         ('aligned_table', 'id'),
                         ('aligned_table', 'aligned_table', 'a1'),
                         ('aligned_table', 'level0_0', 'id'),
                         ('aligned_table', 'level0_0', 'tags'),
                         ('aligned_table', 'level0_0', 'myid')]
        df = flatten_column_index(hier_df, inplace=False)
        # Test that our columns have not changed with inplace=False
        self.assertListEqual(hier_df.columns.to_list(), cols)
        self.assertListEqual(df.columns.to_list(), expexted_cols)  # make sure we got back a modified dataframe
        flatten_column_index(hier_df, inplace=True)  # make sure we can also directly flatten inplace
        self.assertListEqual(hier_df.columns.to_list(), expexted_cols)
        # Test that we can apply flatten_column_index again on our already modified dataframe to reduce the levels
        flatten_column_index(hier_df, inplace=True, max_levels=2)
        expexted_cols = [('parent_table', 'id'), ('parent_table', 'p1'), ('aligned_table', 'id'),
                         ('aligned_table', 'a1'), ('level0_0', 'id'), ('level0_0', 'tags'), ('level0_0', 'myid')]
        self.assertListEqual(hier_df.columns.to_list(), expexted_cols)
        # Test that we can directly reduce the max_levels to just 1
        hier_df = to_hierarchical_dataframe(self.parent_table).reset_index()
        flatten_column_index(hier_df, inplace=True, max_levels=1)
        expexted_cols = ['id', 'p1', 'id', 'a1', 'id', 'tags', 'myid']
        self.assertListEqual(hier_df.columns.to_list(), expexted_cols)

    def test_flatten_column_index_already_flat_index(self):
        hier_df = to_hierarchical_dataframe(self.parent_table).reset_index()
        flatten_column_index(hier_df, inplace=True, max_levels=1)
        expexted_cols = ['id', 'p1', 'id', 'a1', 'id', 'tags', 'myid']
        self.assertListEqual(hier_df.columns.to_list(), expexted_cols)
        # Now try to flatten the already flat columns again to make sure nothing changes
        flatten_column_index(hier_df, inplace=True, max_levels=1)
        self.assertListEqual(hier_df.columns.to_list(), expexted_cols)

    def test_flatten_column_index_bad_maxlevels(self):
        hier_df = to_hierarchical_dataframe(self.parent_table)
        with self.assertRaisesWith(ValueError, 'max_levels must be greater than 0'):
            flatten_column_index(dataframe=hier_df, inplace=True, max_levels=-1)
        with self.assertRaisesWith(ValueError, 'max_levels must be greater than 0'):
            flatten_column_index(dataframe=hier_df, inplace=True, max_levels=0)


class TestLinkedDynamicTables(TestCase):
    """
    Test functionality specific to DynamicTables containing DynamicTableRegion columns.

    Since these functions only implements front-end convenient functions for DynamicTable
    we do not need to worry about I/O here (that is tested elsewere), ut it is sufficient if
    we test with container class. The only time I/O becomes relevant is on read in case that, e.g., a
    h5py.Dataset may behave differently than a numpy array.
    """
    def setUp(self):
        """
        Create basic set of linked tables consisting of

        table_level2 ---> table_level1 ---->  table_level_0_0
                                   \
                                    ------>  table_level_0_1

        """
        self.table_level0_0 = DynamicTable(name='level0_0', description="level0_0 DynamicTable")
        self.table_level0_1 = DynamicTable(name='level0_1', description="level0_1 DynamicTable")
        self.table_level1 = DynamicTableMultiDTR(name='level1',
                                                 child_table1=self.table_level0_0,
                                                 child_table2=self.table_level0_1)
        self.table_level2 = DynamicTableSingleDTR(name='level2', child_table1=self.table_level1)

    def tearDown(self):
        del self.table_level0_0
        del self.table_level0_1
        del self.table_level1
        del self.table_level2

    def popolate_tables(self):
        """Helper function to populate our tables generate in setUp with some simple data"""
        # Level 0 0 table. I.e., first table on level 0
        self.table_level0_0.add_row(id=10)
        self.table_level0_0.add_row(id=11)
        self.table_level0_0.add_row(id=12)
        self.table_level0_0.add_row(id=13)
        self.table_level0_0.add_column(data=['tag1', 'tag2', 'tag2', 'tag1', 'tag3', 'tag4', 'tag5'],
                                       name='tags',
                                       description='custom tags',
                                       index=[1, 2, 4, 7])
        self.table_level0_0.add_column(data=np.arange(4),
                                       name='myid',
                                       description='custom ids',
                                       index=False)
        # Level 0 1 table. I.e., second table on level 0
        self.table_level0_1.add_row(id=14)
        self.table_level0_1.add_row(id=15)
        self.table_level0_1.add_row(id=16)
        self.table_level0_1.add_row(id=17)
        self.table_level0_1.add_column(data=['tag1', 'tag1', 'tag2', 'tag2', 'tag3', 'tag3', 'tag4'],
                                       name='tags',
                                       description='custom tags',
                                       index=[2, 4, 6, 7])
        self.table_level0_1.add_column(data=np.arange(4),
                                       name='myid',
                                       description='custom ids',
                                       index=False)
        # Level 1 table
        self.table_level1.add_row(id=0, child_table_ref1=[0, 1], child_table_ref2=[0])
        self.table_level1.add_row(id=1, child_table_ref1=[2], child_table_ref2=[1, 2])
        self.table_level1.add_row(id=2, child_table_ref1=[3], child_table_ref2=[3])
        self.table_level1.add_column(data=['tag1', 'tag2', 'tag2'],
                                     name='tag',
                                     description='custom tag',
                                     index=False)
        self.table_level1.add_column(data=['tag1', 'tag2', 'tag2', 'tag3', 'tag3', 'tag4', 'tag5'],
                                     name='tags',
                                     description='custom tags',
                                     index=[2, 4, 7])
        # Level 2 data
        self.table_level2.add_row(id=0, child_table_ref1=[0, ])
        self.table_level2.add_row(id=1, child_table_ref1=[1, 2])
        self.table_level2.add_column(data=[10, 12],
                                     name='filter',
                                     description='filter value',
                                     index=False)

    def test_populate_table_hierarchy(self):
        """Test that just checks that populating the tables with data works correctly"""
        self.popolate_tables()
        # Check level0 0 data
        self.assertListEqual(self.table_level0_0.id[:], np.arange(10, 14, 1).tolist())
        self.assertListEqual(self.table_level0_0['tags'][:],
                             [['tag1'], ['tag2'], ['tag2', 'tag1'], ['tag3', 'tag4', 'tag5']])
        self.assertListEqual(self.table_level0_0['myid'][:].tolist(), np.arange(0, 4, 1).tolist())
        # Check level0 1 data
        self.assertListEqual(self.table_level0_1.id[:], np.arange(14, 18, 1).tolist())
        self.assertListEqual(self.table_level0_1['tags'][:],
                             [['tag1', 'tag1'], ['tag2', 'tag2'], ['tag3', 'tag3'], ['tag4']])
        self.assertListEqual(self.table_level0_1['myid'][:].tolist(), np.arange(0, 4, 1).tolist())
        # Check level1 data
        self.assertListEqual(self.table_level1.id[:], np.arange(0, 3, 1).tolist())
        self.assertListEqual(self.table_level1['tag'][:], ['tag1', 'tag2', 'tag2'])
        self.assertTrue(self.table_level1['child_table_ref1'].target.table is self.table_level0_0)
        self.assertTrue(self.table_level1['child_table_ref2'].target.table is self.table_level0_1)
        self.assertEqual(len(self.table_level1['child_table_ref1'].target.table), 4)
        self.assertEqual(len(self.table_level1['child_table_ref2'].target.table), 4)
        # Check level2 data
        self.assertListEqual(self.table_level2.id[:], np.arange(0, 2, 1).tolist())
        self.assertListEqual(self.table_level2['filter'][:], [10, 12])
        self.assertTrue(self.table_level2['child_table_ref1'].target.table is self.table_level1)
        self.assertEqual(len(self.table_level2['child_table_ref1'].target.table), 3)

    def test_get_foreign_columns(self):
        """Test DynamicTable.get_foreign_columns"""
        self.popolate_tables()
        self.assertListEqual(self.table_level0_0.get_foreign_columns(), [])
        self.assertListEqual(self.table_level0_1.get_foreign_columns(), [])
        self.assertListEqual(self.table_level1.get_foreign_columns(), ['child_table_ref1', 'child_table_ref2'])
        self.assertListEqual(self.table_level2.get_foreign_columns(), ['child_table_ref1'])

    def test_has_foreign_columns(self):
        """Test DynamicTable.get_foreign_columns"""
        self.popolate_tables()
        self.assertFalse(self.table_level0_0.has_foreign_columns())
        self.assertFalse(self.table_level0_1.has_foreign_columns())
        self.assertTrue(self.table_level1.has_foreign_columns())
        self.assertTrue(self.table_level2.has_foreign_columns())

    def test_get_linked_tables(self):
        """Test DynamicTable.get_linked_tables"""
        self.popolate_tables()
        # check level0_0
        self.assertListEqual(self.table_level0_0.get_linked_tables(), [])
        # check level0_0
        self.assertListEqual(self.table_level0_1.get_linked_tables(), [])
        # check level1
        temp = self.table_level1.get_linked_tables()
        self.assertEqual(len(temp), 2)
        self.assertEqual(temp[0].source_table.name, self.table_level1.name)
        self.assertEqual(temp[0].source_column.name, 'child_table_ref1')
        self.assertEqual(temp[0].target_table.name, self.table_level0_0.name)
        self.assertEqual(temp[1].source_table.name, self.table_level1.name)
        self.assertEqual(temp[1].source_column.name, 'child_table_ref2')
        self.assertEqual(temp[1].target_table.name, self.table_level0_1.name)
        # check level2
        temp = self.table_level2.get_linked_tables()
        self.assertEqual(len(temp), 3)
        self.assertEqual(temp[0].source_table.name, self.table_level2.name)
        self.assertEqual(temp[0].source_column.name, 'child_table_ref1')
        self.assertEqual(temp[0].target_table.name, self.table_level1.name)
        self.assertEqual(temp[1].source_table.name, self.table_level1.name)
        self.assertEqual(temp[1].source_column.name, 'child_table_ref1')
        self.assertEqual(temp[1].target_table.name, self.table_level0_0.name)
        self.assertEqual(temp[2].source_table.name, self.table_level1.name)
        self.assertEqual(temp[2].source_column.name, 'child_table_ref2')
        self.assertEqual(temp[2].target_table.name, self.table_level0_1.name)
