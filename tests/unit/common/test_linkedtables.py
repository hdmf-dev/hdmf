import numpy as np
from hdmf.common import DynamicTable, AlignedDynamicTable, VectorData, DynamicTableRegion
from hdmf.testing import TestCase
from hdmf.utils import docval, popargs, get_docval, call_docval_func


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
        # Initialize the DynamicTable
        call_docval_func(super(DynamicTableSingleDTR, self).__init__, kwargs)
        if self['child_table_ref1'].target.table is None:
            self['child_table_ref1'].target.table = popargs('child_table1', kwargs)


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
        # Initialize the DynamicTable
        call_docval_func(super(DynamicTableMultiDTR, self).__init__, kwargs)
        if self['child_table_ref1'].target.table is None:
            self['child_table_ref1'].target.table = popargs('child_table1', kwargs)
        if self['child_table_ref2'].target.table is None:
            self['child_table_ref2'].target.table = popargs('child_table2', kwargs)


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
        self.table_level0_0 = DynamicTable(name='level0_0', description="level0_0 DynamicTable")
        self.table_level0_1 = DynamicTable(name='level0_1', description="level0_1 DynamicTable")
        self.category0 = DynamicTableSingleDTR(name='category0', child_table1=self.table_level0_0)
        self.category1 = DynamicTableSingleDTR(name='category1', child_table1=self.table_level0_1)
        self.aligned_table = AlignedDynamicTable(name='my_aligned_table',
                                                 description='my test table',
                                                 category_tables=[self.category0, self.category1])

    def tearDown(self):
        del self.table_level0_0
        del self.table_level0_1
        del self.category0
        del self.category1
        del self.aligned_table

    def popuplate_tables(self):
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
        # category 0 table
        self.category0.add_row(id=0, child_table_ref1=[0, 1], child_table_ref2=[0])
        self.category0.add_row(id=1, child_table_ref1=[2], child_table_ref2=[1, 2])
        self.category0.add_row(id=2, child_table_ref1=[3], child_table_ref2=[3])
        self.category0.add_column(data=['tag1', 'tag2', 'tag2'],
                                  name='tag',
                                  description='custom tag',
                                  index=False)
        self.category0.add_column(data=['tag1', 'tag2', 'tag2', 'tag3', 'tag3', 'tag4', 'tag5'],
                                  name='tags',
                                  description='custom tags',
                                  index=[2, 4, 7])

        # category 1 table
        self.category1.add_row(id=0, child_table_ref1=[0, 1], child_table_ref2=[0])
        self.category1.add_row(id=1, child_table_ref1=[2], child_table_ref2=[1, 2])
        self.category1.add_row(id=2, child_table_ref1=[3], child_table_ref2=[3])
        self.category1.add_column(data=['tag1', 'tag2', 'tag2'],
                                  name='tag',
                                  description='custom tag',
                                  index=False)
        self.category1.add_column(data=['tag1', 'tag2', 'tag2', 'tag3', 'tag3', 'tag4', 'tag5'],
                                  name='tags',
                                  description='custom tags',
                                  index=[2, 4, 7])
        # aligned table
        self.aligned_table.column(data=['at1', 'at2', 'at3'],
                                  name='at_col',
                                  description='custom at column',
                                  index=False)

    def test_has_foreign_columns_in_category_tables(self):
        """Test confirming working order for DynamicTableRegions in subtables"""
        self.assertTrue(self.aligned_table.has_foreign_columns())

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


class TestLinkedDynamicTables(TestCase):
    """
    Test functionality specific to DynamicTables containing DynamicTableRegion columns.

    Since these functions only implements front-end convenient functions for DynamicTable
    we do not need to worry about I/O here (that is tested elsewere), but it is sufficient if
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
        self.assertEqual(temp[0]['source_table'].name, self.table_level1.name)
        self.assertEqual(temp[0]['source_column'].name, 'child_table_ref1')
        self.assertEqual(temp[0]['target_table'].name, self.table_level0_0.name)
        self.assertEqual(temp[1]['source_table'].name, self.table_level1.name)
        self.assertEqual(temp[1]['source_column'].name, 'child_table_ref2')
        self.assertEqual(temp[1]['target_table'].name, self.table_level0_1.name)
        # check level2
        temp = self.table_level2.get_linked_tables()
        self.assertEqual(len(temp), 3)
        self.assertEqual(temp[0]['source_table'].name, self.table_level2.name)
        self.assertEqual(temp[0]['source_column'].name, 'child_table_ref1')
        self.assertEqual(temp[0]['target_table'].name, self.table_level1.name)
        self.assertEqual(temp[1]['source_table'].name, self.table_level1.name)
        self.assertEqual(temp[1]['source_column'].name, 'child_table_ref1')
        self.assertEqual(temp[1]['target_table'].name, self.table_level0_0.name)
        self.assertEqual(temp[2]['source_table'].name, self.table_level1.name)
        self.assertEqual(temp[2]['source_column'].name, 'child_table_ref2')
        self.assertEqual(temp[2]['target_table'].name, self.table_level0_1.name)

    # def test_to_denormalized_dataframe(self):
    #     """
    #     Test to_denormalized_dataframe(flat_column_index=False)
    #     for self.table_level1 and self.table_level2
    #     """
    #     ref_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),
    #                                 'referencedata_test_hierarchical_dynamic_table_mixin.h5')
    #     if not os.path.exists(ref_filename):
    #         self.skipTest("Reference data file not found required for test. %s" & ref_filename)
    #     self.popolate_tables()
    #     # level 1
    #     curr = self.table_level1.to_denormalized_dataframe(flat_column_index=False)
    #     ref = pandas.read_hdf(path_or_buf=ref_filename,
    #                           key='test_to_denormalized_dataframe_table_level1')
    #     pandas.testing.assert_frame_equal(curr, ref)
    #     # level 2
    #     curr = self.table_level2.to_denormalized_dataframe(flat_column_index=False)
    #     ref = pandas.read_hdf(path_or_buf=ref_filename,
    #                           key='test_to_denormalized_dataframe_table_level2')
    #     pandas.testing.assert_frame_equal(curr, ref)
    #
    # def test_to_denormalized_dataframe_flat_column_index(self):
    #     """
    #     Test to_denormalized_dataframe(flat_column_index=True)
    #     for self.table_level1 and self.table_level2
    #     """
    #     ref_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),
    #                                 'referencedata_test_hierarchical_dynamic_table_mixin.h5')
    #     if not os.path.exists(ref_filename):
    #         self.skipTest("Reference data file not found required for test. %s" & ref_filename)
    #     self.popolate_tables()
    #     # test level 1
    #     curr = self.table_level1.to_denormalized_dataframe(flat_column_index=True)
    #     ref = pandas.read_hdf(path_or_buf=ref_filename,
    #                           key='test_to_denormalized_dataframe_flat_column_index_table_level1')
    #     pandas.testing.assert_frame_equal(curr, ref)
    #     # test level 2
    #     curr = self.table_level2.to_denormalized_dataframe(flat_column_index=True)
    #     ref = pandas.read_hdf(path_or_buf=ref_filename,
    #                           key='test_to_denormalized_dataframe_flat_column_index_table_level2')
    #     pandas.testing.assert_frame_equal(curr, ref)
    #
    # def test_to_hierarchical_dataframe_empty_table(self):
    #     """
    #     Test creating the hierarchical table that is empty
    #     """
    #     # Do not populate with data, just straight convert to dataframe
    #     tab = self.table_level1.to_hierarchical_dataframe()
    #     self.assertEqual(len(tab), 0)
    #     self.assertListEqual(tab.columns.to_list(), [('level0', 'id')])
    #     self.assertListEqual(tab.index.names, [('level1', 'id')])
    #     tab = self.table_level2.to_hierarchical_dataframe()
    #     self.assertEqual(len(tab), 0)
    #     self.assertListEqual(tab.columns.to_list(), [('level0', 'id')])
    #     self.assertListEqual(tab.index.names, [('level2', 'id'), ('level1', 'id')])
    #     tab = self.table_level1.to_hierarchical_dataframe(flat_column_index=True)
    #     self.assertEqual(len(tab), 0)
    #     self.assertListEqual(tab.columns.to_list(), [('level0', 'id')])
    #     tab = self.table_level2.to_hierarchical_dataframe(flat_column_index=True)
    #     self.assertListEqual(tab.columns.to_list(), [('level0', 'id')])
    #     self.assertListEqual(tab.index.names, [('level2', 'id'), ('level1', 'id')])
    #
    # def test_to_hierarchical_dataframe(self):
    #     """
    #     Test to_hierarchical_dataframe(flat_column_index=False)
    #     for self.table_level1 and self.table_level2
    #     """
    #     ref_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),
    #                                 'referencedata_test_hierarchical_dynamic_table_mixin.h5')
    #     if not os.path.exists(ref_filename):
    #         self.skipTest("Reference data file not found required for test. %s" & ref_filename)
    #     self.popolate_tables()
    #     # test level 1
    #     curr = self.table_level1.to_hierarchical_dataframe(flat_column_index=False)
    #     ref = pandas.read_hdf(path_or_buf=ref_filename,
    #                           key='test_to_hierarchical_dataframe_table_level1')
    #     pandas.testing.assert_frame_equal(curr, ref)
    #     # test level 2
    #     curr = self.table_level2.to_hierarchical_dataframe(flat_column_index=False)
    #     ref = pandas.read_hdf(path_or_buf=ref_filename,
    #                           key='test_to_hierarchical_dataframe_table_level2')
    #     pandas.testing.assert_frame_equal(curr, ref)
    #
    # def test_to_hierarchical_dataframe_flat_column_index(self):
    #     """
    #     Test to_hierarchical_dataframe(flat_column_index=True)
    #     for self.table_level1 and self.table_level2
    #     """
    #     ref_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),
    #                                 'referencedata_test_hierarchical_dynamic_table_mixin.h5')
    #     if not os.path.exists(ref_filename):
    #         self.skipTest("Reference data file not found required for test. %s" & ref_filename)
    #     self.popolate_tables()
    #     # test level 1
    #     curr = self.table_level1.to_hierarchical_dataframe(flat_column_index=True)
    #     ref = pandas.read_hdf(path_or_buf=ref_filename,
    #                           key='test_to_hierarchical_dataframe_flat_column_index_table_level1')
    #     pandas.testing.assert_frame_equal(curr, ref)
    #     # test level 2
    #     curr = self.table_level2.to_hierarchical_dataframe(flat_column_index=True)
    #     ref = pandas.read_hdf(path_or_buf=ref_filename,
    #                           key='test_to_hierarchical_dataframe_flat_column_index_table_level2')
    #     pandas.testing.assert_frame_equal(curr, ref)
    #
    #
    # @unittest.skip("Enable this test if you want to generate a new reference test data for comparison")
    # def test_generate_reference_testdata(self):
    #     """
    #     Save reference results for the tests evaluating that the to_denormalized_dataframe
    #     and to_hierarchical_dataframe functions are working. This test should be enabled
    #     to regenerate the reference results. CAUTION: We should confirm first that the
    #     functions produce the correct results.
    #     """
    #     self.popolate_tables()
    #     ref_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)),
    #                                 'referencedata_test_hierarchical_dynamic_table_mixin.h5')
    #     if os.path.exists(ref_filename):
    #         os.remove(ref_filename)
    #     print("\n Generating reference test data file %s" % ref_filename)  # noqa: T001
    #     temp = self.table_level1.to_denormalized_dataframe(flat_column_index=False)
    #     temp.to_hdf(path_or_buf=ref_filename,
    #                 key='test_to_denormalized_dataframe_table_level1')
    #     temp = self.table_level2.to_denormalized_dataframe(flat_column_index=False)
    #     temp.to_hdf(path_or_buf=ref_filename,
    #                 key='test_to_denormalized_dataframe_table_level2')
    #     temp = self.table_level1.to_denormalized_dataframe(flat_column_index=True)
    #     temp.to_hdf(path_or_buf=ref_filename,
    #                 key='test_to_denormalized_dataframe_flat_column_index_table_level1')
    #     temp = self.table_level2.to_denormalized_dataframe(flat_column_index=True)
    #     temp.to_hdf(path_or_buf=ref_filename,
    #                 key='test_to_denormalized_dataframe_flat_column_index_table_level2')
    #     temp = self.table_level1.to_hierarchical_dataframe(flat_column_index=False)
    #     temp.to_hdf(path_or_buf=ref_filename,
    #                 key='test_to_hierarchical_dataframe_table_level1')
    #     temp = self.table_level2.to_hierarchical_dataframe(flat_column_index=False)
    #     temp.to_hdf(path_or_buf=ref_filename,
    #                 key='test_to_hierarchical_dataframe_table_level2')
    #     temp = self.table_level1.to_hierarchical_dataframe(flat_column_index=True)
    #     temp.to_hdf(path_or_buf=ref_filename,
    #                 key='test_to_hierarchical_dataframe_flat_column_index_table_level1')
    #     temp = self.table_level2.to_hierarchical_dataframe(flat_column_index=True)
    #     temp.to_hdf(path_or_buf=ref_filename,
    #                 key='test_to_hierarchical_dataframe_flat_column_index_table_level2')
