import numpy as np
from hdmf.common.table import DynamicTable, DynamicTableRegion, VectorData
from hdmf.data_utils import ShapeValidatorResult, DataChunkIterator, assertEqualShape
from hdmf.testing import TestCase


class ShapeValidatorTests(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_array_all_dimensions_match(self):
        # Test match
        d1 = np.arange(10).reshape(2, 5)
        d2 = np.arange(10).reshape(2, 5)
        res = assertEqualShape(d1, d2)
        self.assertTrue(res.result)
        self.assertIsNone(res.error)
        self.assertTupleEqual(res.ignored, ())
        self.assertTupleEqual(res.unmatched, ())
        self.assertTupleEqual(res.shape1, (2, 5))
        self.assertTupleEqual(res.shape2, (2, 5))
        self.assertTupleEqual(res.axes1, (0, 1))
        self.assertTupleEqual(res.axes2, (0, 1))

    def test_array_dimensions_mismatch(self):
        # Test unmatched
        d1 = np.arange(10).reshape(2, 5)
        d2 = np.arange(10).reshape(5, 2)
        res = assertEqualShape(d1, d2)
        self.assertFalse(res.result)
        self.assertEqual(res.error, 'AXIS_LEN_ERROR')
        self.assertTupleEqual(res.ignored, ())
        self.assertTupleEqual(res.unmatched, ((0, 0), (1, 1)))
        self.assertTupleEqual(res.shape1, (2, 5))
        self.assertTupleEqual(res.shape2, (5, 2))
        self.assertTupleEqual(res.axes1, (0, 1))
        self.assertTupleEqual(res.axes2, (0, 1))

    def test_array_unequal_number_of_dimensions(self):
        # Test unequal num dims
        d1 = np.arange(10).reshape(2, 5)
        d2 = np.arange(20).reshape(5, 2, 2)
        res = assertEqualShape(d1, d2)
        self.assertFalse(res.result)
        self.assertEqual(res.error, 'NUM_AXES_ERROR')
        self.assertTupleEqual(res.ignored, ())
        self.assertTupleEqual(res.unmatched, ())
        self.assertTupleEqual(res.shape1, (2, 5))
        self.assertTupleEqual(res.shape2, (5, 2, 2))
        self.assertTupleEqual(res.axes1, (0, 1))
        self.assertTupleEqual(res.axes2, (0, 1, 2))

    def test_array_unequal_number_of_dimensions_check_one_axis_only(self):
        # Test unequal num dims compare one axis
        d1 = np.arange(10).reshape(2, 5)
        d2 = np.arange(20).reshape(2, 5, 2)
        res = assertEqualShape(d1, d2, 0, 0)
        self.assertTrue(res.result)
        self.assertIsNone(res.error)
        self.assertTupleEqual(res.ignored, ())
        self.assertTupleEqual(res.unmatched, ())
        self.assertTupleEqual(res.shape1, (2, 5))
        self.assertTupleEqual(res.shape2, (2, 5, 2))
        self.assertTupleEqual(res.axes1, (0,))
        self.assertTupleEqual(res.axes2, (0,))

    def test_array_unequal_number_of_dimensions_check_multiple_axesy(self):
        # Test unequal num dims compare multiple axes
        d1 = np.arange(10).reshape(2, 5)
        d2 = np.arange(20).reshape(5, 2, 2)
        res = assertEqualShape(d1, d2, [0, 1], [1, 0])
        self.assertTrue(res.result)
        self.assertIsNone(res.error)
        self.assertTupleEqual(res.ignored, ())
        self.assertTupleEqual(res.unmatched, ())
        self.assertTupleEqual(res.shape1, (2, 5))
        self.assertTupleEqual(res.shape2, (5, 2, 2))
        self.assertTupleEqual(res.axes1, (0, 1))
        self.assertTupleEqual(res.axes2, (1, 0))

    def test_array_unequal_number_of_axes_for_comparison(self):
        # Test unequal num axes for comparison
        d1 = np.arange(10).reshape(2, 5)
        d2 = np.arange(20).reshape(5, 2, 2)
        res = assertEqualShape(d1, d2, [0, 1], 1)
        self.assertFalse(res.result)
        self.assertEqual(res.error, "NUM_AXES_ERROR")
        self.assertTupleEqual(res.ignored, ())
        self.assertTupleEqual(res.unmatched, ())
        self.assertTupleEqual(res.shape1, (2, 5))
        self.assertTupleEqual(res.shape2, (5, 2, 2))
        self.assertTupleEqual(res.axes1, (0, 1))
        self.assertTupleEqual(res.axes2, (1,))

    def test_array_axis_index_out_of_bounds_single_axis(self):
        # Test too large frist axis
        d1 = np.arange(10).reshape(2, 5)
        d2 = np.arange(20).reshape(5, 2, 2)
        res = assertEqualShape(d1, d2, 4, 1)
        self.assertFalse(res.result)
        self.assertEqual(res.error, 'AXIS_OUT_OF_BOUNDS')
        self.assertTupleEqual(res.ignored, ())
        self.assertTupleEqual(res.unmatched, ())
        self.assertTupleEqual(res.shape1, (2, 5))
        self.assertTupleEqual(res.shape2, (5, 2, 2))
        self.assertTupleEqual(res.axes1, (4,))
        self.assertTupleEqual(res.axes2, (1,))

    def test_array_axis_index_out_of_bounds_mutilple_axis(self):
        # Test too large second axis
        d1 = np.arange(10).reshape(2, 5)
        d2 = np.arange(20).reshape(5, 2, 2)
        res = assertEqualShape(d1, d2, [0, 1], [5, 0])
        self.assertFalse(res.result)
        self.assertEqual(res.error, 'AXIS_OUT_OF_BOUNDS')
        self.assertTupleEqual(res.ignored, ())
        self.assertTupleEqual(res.unmatched, ())
        self.assertTupleEqual(res.shape1, (2, 5))
        self.assertTupleEqual(res.shape2, (5, 2, 2))
        self.assertTupleEqual(res.axes1, (0, 1))
        self.assertTupleEqual(res.axes2, (5, 0))

    def test_DataChunkIterators_match(self):
        # Compare data chunk iterators
        d1 = DataChunkIterator(data=np.arange(10).reshape(2, 5))
        d2 = DataChunkIterator(data=np.arange(10).reshape(2, 5))
        res = assertEqualShape(d1, d2)
        self.assertTrue(res.result)
        self.assertIsNone(res.error)
        self.assertTupleEqual(res.ignored, ())
        self.assertTupleEqual(res.unmatched, ())
        self.assertTupleEqual(res.shape1, (2, 5))
        self.assertTupleEqual(res.shape2, (2, 5))
        self.assertTupleEqual(res.axes1, (0, 1))
        self.assertTupleEqual(res.axes2, (0, 1))

    def test_DataChunkIterator_ignore_undetermined_axis(self):
        # Compare data chunk iterators with undetermined axis (ignore axis)
        d1 = DataChunkIterator(data=np.arange(10).reshape(2, 5), maxshape=(None, 5))
        d2 = DataChunkIterator(data=np.arange(10).reshape(2, 5))
        res = assertEqualShape(d1, d2, ignore_undetermined=True)
        self.assertTrue(res.result)
        self.assertIsNone(res.error)
        self.assertTupleEqual(res.ignored, ((0, 0),))
        self.assertTupleEqual(res.unmatched, ())
        self.assertTupleEqual(res.shape1, (None, 5))
        self.assertTupleEqual(res.shape2, (2, 5))
        self.assertTupleEqual(res.axes1, (0, 1))
        self.assertTupleEqual(res.axes2, (0, 1))

    def test_DataChunkIterator_error_on_undetermined_axis(self):
        # Compare data chunk iterators with undetermined axis (error on undetermined axis)
        d1 = DataChunkIterator(data=np.arange(10).reshape(2, 5), maxshape=(None, 5))
        d2 = DataChunkIterator(data=np.arange(10).reshape(2, 5))
        res = assertEqualShape(d1, d2, ignore_undetermined=False)
        self.assertFalse(res.result)
        self.assertEqual(res.error, 'AXIS_LEN_ERROR')
        self.assertTupleEqual(res.ignored, ())
        self.assertTupleEqual(res.unmatched, ((0, 0),))
        self.assertTupleEqual(res.shape1, (None, 5))
        self.assertTupleEqual(res.shape2, (2, 5))
        self.assertTupleEqual(res.axes1, (0, 1))
        self.assertTupleEqual(res.axes2, (0, 1))

    def test_DynamicTableRegion_shape_validation(self):
        # Create a test DynamicTable
        dt_spec = [
            {'name': 'foo', 'description': 'foo column'},
            {'name': 'bar', 'description': 'bar column'},
            {'name': 'baz', 'description': 'baz column'},
        ]
        dt_data = [
            [1, 2, 3, 4, 5],
            [10.0, 20.0, 30.0, 40.0, 50.0],
            ['cat', 'dog', 'bird', 'fish', 'lizard']
        ]
        columns = [
            VectorData(name=s['name'], description=s['description'], data=d)
            for s, d in zip(dt_spec, dt_data)
        ]
        dt = DynamicTable("with_columns_and_data",
                          "a test table", columns=columns)
        # Create test DynamicTableRegion
        dtr = DynamicTableRegion('dtr', [1, 2, 2], 'desc', table=dt)
        # Confirm that the shapes match
        res = assertEqualShape(dtr, np.arange(9).reshape(3, 3))
        self.assertTrue(res.result)

    def with_table_columns(self):
        cols = [VectorData(**d) for d in self.spec]
        table = DynamicTable("with_table_columns", 'a test table', columns=cols)
        return table

    def with_columns_and_data(self):

        return


class ShapeValidatorResultTests(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_default_message(self):
        temp = ShapeValidatorResult()
        temp.error = 'AXIS_LEN_ERROR'
        self.assertEqual(temp.default_message, ShapeValidatorResult.SHAPE_ERROR[temp.error])

    def test_set_error_to_illegal_type(self):
        temp = ShapeValidatorResult()
        with self.assertRaises(ValueError):
            temp.error = 'MY_ILLEGAL_ERROR_TYPE'

    def test_ensure_use_of_tuples_during_asignment(self):
        temp = ShapeValidatorResult()
        temp_d = [1, 2]
        temp_cases = ['shape1', 'shape2', 'axes1', 'axes2', 'ignored', 'unmatched']
        for var in temp_cases:
            setattr(temp, var, temp_d)
            self.assertIsInstance(getattr(temp, var), tuple,  var)
