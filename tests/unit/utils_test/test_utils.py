import os

import h5py
import numpy as np
from hdmf.data_utils import DataChunkIterator, DataIO
from hdmf.testing import TestCase
from hdmf.utils import get_data_shape, to_uint_array


class TestGetDataShape(TestCase):

    def test_h5dataset(self):
        """Test get_data_shape on h5py.Datasets of various shapes and maxshape."""
        path = 'test_get_data_shape.h5'
        with h5py.File(path, 'w') as f:
            dset = f.create_dataset('data', data=((1, 2), (3, 4), (5, 6)))
            res = get_data_shape(dset)
            self.assertTupleEqual(res, (3, 2))

            dset = f.create_dataset('shape', shape=(3, 2))
            res = get_data_shape(dset)
            self.assertTupleEqual(res, (3, 2))

            # test that maxshape takes priority
            dset = f.create_dataset('shape_maxshape', shape=(3, 2), maxshape=(None, 100))
            res = get_data_shape(dset)
            self.assertTupleEqual(res, (None, 100))

        os.remove(path)

    def test_dci(self):
        """Test get_data_shape on DataChunkIterators of various shapes and maxshape."""
        dci = DataChunkIterator(dtype=np.dtype(int))
        res = get_data_shape(dci)
        self.assertIsNone(res)

        dci = DataChunkIterator(data=[1, 2])
        res = get_data_shape(dci)
        self.assertTupleEqual(res, (2, ))

        dci = DataChunkIterator(data=[[1, 2], [3, 4], [5, 6]])
        res = get_data_shape(dci)
        self.assertTupleEqual(res, (3, 2))

        # test that maxshape takes priority
        dci = DataChunkIterator(data=[[1, 2], [3, 4], [5, 6]], maxshape=(None, 100))
        res = get_data_shape(dci)
        self.assertTupleEqual(res, (None, 100))

    def test_dataio(self):
        """Test get_data_shape on DataIO of various shapes and maxshape."""
        dio = DataIO(data=[1, 2])
        res = get_data_shape(dio)
        self.assertTupleEqual(res, (2, ))

        dio = DataIO(data=[[1, 2], [3, 4], [5, 6]])
        res = get_data_shape(dio)
        self.assertTupleEqual(res, (3, 2))

        dio = DataIO(data=np.array([[1, 2], [3, 4], [5, 6]]))
        res = get_data_shape(dio)
        self.assertTupleEqual(res, (3, 2))

    def test_list(self):
        """Test get_data_shape on lists of various shapes."""
        res = get_data_shape(list())
        self.assertTupleEqual(res, (0, ))

        res = get_data_shape([1, 2])
        self.assertTupleEqual(res, (2, ))

        res = get_data_shape([[1, 2], [3, 4], [5, 6]])
        self.assertTupleEqual(res, (3, 2))

    def test_tuple(self):
        """Test get_data_shape on tuples of various shapes."""
        res = get_data_shape(tuple())
        self.assertTupleEqual(res, (0, ))

        res = get_data_shape((1, 2))
        self.assertTupleEqual(res, (2, ))

        res = get_data_shape(((1, 2), (3, 4), (5, 6)))
        self.assertTupleEqual(res, (3, 2))

    def test_nparray(self):
        """Test get_data_shape on numpy arrays of various shapes."""
        res = get_data_shape(np.empty([]))
        self.assertTupleEqual(res, tuple())

        res = get_data_shape(np.array([]))
        self.assertTupleEqual(res, (0, ))

        res = get_data_shape(np.array([1, 2]))
        self.assertTupleEqual(res, (2, ))

        res = get_data_shape(np.array([[1, 2], [3, 4], [5, 6]]))
        self.assertTupleEqual(res, (3, 2))

    def test_other(self):
        """Test get_data_shape on miscellaneous edge cases."""
        res = get_data_shape(dict())
        self.assertIsNone(res)

        res = get_data_shape(None)
        self.assertIsNone(res)

        res = get_data_shape([None, None])
        self.assertTupleEqual(res, (2, ))

        res = get_data_shape(object())
        self.assertIsNone(res)

        res = get_data_shape([object(), object()])
        self.assertTupleEqual(res, (2, ))

    def test_string(self):
        """Test get_data_shape on strings and collections of strings."""
        res = get_data_shape('abc')
        self.assertIsNone(res)

        res = get_data_shape(('a', 'b'))
        self.assertTupleEqual(res, (2, ))

        res = get_data_shape((('a', 'b'), ('c', 'd'), ('e', 'f')))
        self.assertTupleEqual(res, (3, 2))

    def test_set(self):
        """Test get_data_shape on sets, which have __len__ but are not subscriptable."""
        res = get_data_shape(set())
        self.assertTupleEqual(res, (0, ))

        res = get_data_shape({1, 2})
        self.assertTupleEqual(res, (2, ))

    def test_arbitrary_iterable_with_len(self):
        """Test get_data_shape with strict_no_data_load=True on an arbitrary iterable object with __len__."""

        class MyIterable:
            """Iterable class without shape or maxshape, where loading the first element raises an error."""

            def __len__(self):
                return 10

            def __iter__(self):
                return self

            def __next__(self):
                raise DataLoadedError()

        class DataLoadedError(Exception):
            pass

        data = MyIterable()
        with self.assertRaises(DataLoadedError):
            get_data_shape(data)  # test that data is loaded

        res = get_data_shape(data, strict_no_data_load=True)  # no error raised means data was not loaded
        self.assertIsNone(res)

    def test_strict_no_data_load(self):
        """Test get_data_shape with strict_no_data_load=True on nested lists/tuples is the same as when it is False."""
        res = get_data_shape([[1, 2], [3, 4], [5, 6]], strict_no_data_load=True)
        self.assertTupleEqual(res, (3, 2))

        res = get_data_shape(((1, 2), (3, 4), (5, 6)), strict_no_data_load=True)
        self.assertTupleEqual(res, (3, 2))


class TestToUintArray(TestCase):

    def test_ndarray_uint(self):
        arr = np.array([0, 1, 2], dtype=np.uint)
        res = to_uint_array(arr)
        np.testing.assert_array_equal(res, arr)

    def test_ndarray_int(self):
        arr = np.array([0, 1, 2], dtype=np.int)
        res = to_uint_array(arr)
        np.testing.assert_array_equal(res, arr)

    def test_ndarray_int_neg(self):
        arr = np.array([0, -1, 2], dtype=np.int)
        with self.assertRaisesWith(ValueError, 'Cannot convert negative integer values to uint.'):
            to_uint_array(arr)

    def test_ndarray_float(self):
        arr = np.array([0, 1, 2], dtype=np.float)
        with self.assertRaisesWith(ValueError, 'Cannot convert array of dtype float64 to uint.'):
            to_uint_array(arr)

    def test_list_int(self):
        arr = [0, 1, 2]
        res = to_uint_array(arr)
        expected = np.array([0, 1, 2], dtype=np.uint)
        np.testing.assert_array_equal(res, expected)

    def test_list_int_neg(self):
        arr = [0, -1, 2]
        with self.assertRaisesWith(ValueError, 'Cannot convert negative integer values to uint.'):
            to_uint_array(arr)

    def test_list_float(self):
        arr = [0., 1., 2.]
        with self.assertRaisesWith(ValueError, 'Cannot convert array of dtype float64 to uint.'):
            to_uint_array(arr)
