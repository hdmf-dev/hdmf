import os

import h5py
import numpy as np
import pandas as pd
from hdmf.container import Data
from hdmf.data_utils import DataChunkIterator, DataIO
from hdmf.testing import TestCase
from hdmf.utils import (get_data_shape, to_uint_array, is_newer_version, _is_collection, _get_length, _unwrap_scalar,
                        coerce_pandas_data)
from tests.unit.helpers.utils import get_temp_filepath


class TestIsCollection(TestCase):
    """Tests for _is_collection helper that detects collections vs scalars."""

    def test_numpy_1d_array(self):
        self.assertTrue(_is_collection(np.array([1, 2, 3])))

    def test_numpy_empty_array(self):
        self.assertTrue(_is_collection(np.array([])))

    def test_numpy_2d_array(self):
        self.assertTrue(_is_collection(np.array([[1, 2], [3, 4]])))

    def test_numpy_0d_array(self):
        self.assertFalse(_is_collection(np.array(5)))

    def test_numpy_scalar(self):
        self.assertFalse(_is_collection(np.float64(3.14)))

    def test_list(self):
        self.assertTrue(_is_collection([1, 2, 3]))

    def test_empty_list(self):
        self.assertTrue(_is_collection([]))

    def test_tuple(self):
        self.assertTrue(_is_collection((1, 2, 3)))

    def test_set(self):
        self.assertTrue(_is_collection({1, 2, 3}))

    def test_int(self):
        self.assertFalse(_is_collection(42))

    def test_float(self):
        self.assertFalse(_is_collection(3.14))

    def test_string(self):
        self.assertFalse(_is_collection("hello"))

    def test_bytes(self):
        self.assertFalse(_is_collection(b"hello"))

    def test_none(self):
        self.assertFalse(_is_collection(None))

    def test_bool(self):
        self.assertFalse(_is_collection(True))

    def test_ndim_without_len(self):
        """Simulate zarr v3 array: has ndim and shape but no __len__."""
        class FakeArray:
            ndim = 2
            shape = (10, 5)
        self.assertTrue(_is_collection(FakeArray()))

    def test_ndim_zero_without_len(self):
        """Simulate 0-d array-like: has ndim=0 but no __len__."""
        class FakeScalar:
            ndim = 0
            shape = ()
        self.assertFalse(_is_collection(FakeScalar()))

    def test_ndim_raises_runtime_error(self):
        """Simulate closed h5py dataset where accessing ndim raises RuntimeError."""
        class ClosedDataset:
            @property
            def ndim(self):
                raise RuntimeError("File is closed")
        self.assertFalse(_is_collection(ClosedDataset()))

    def test_closed_h5py_dataset(self):
        """Test that a real closed h5py dataset is handled gracefully."""
        path = get_temp_filepath()
        try:
            hfile = h5py.File(path, "w")
            ds = hfile.create_dataset("data", data=[1, 2, 3])
            hfile.close()
            self.assertFalse(_is_collection(ds))
        finally:
            if os.path.exists(path):
                os.remove(path)


class TestGetLength(TestCase):
    """Tests for _get_length helper that gets first-dimension length."""

    def test_list(self):
        self.assertEqual(_get_length([1, 2, 3]), 3)

    def test_empty_list(self):
        self.assertEqual(_get_length([]), 0)

    def test_tuple(self):
        self.assertEqual(_get_length((1, 2)), 2)

    def test_numpy_array(self):
        self.assertEqual(_get_length(np.array([1, 2, 3, 4])), 4)

    def test_numpy_2d_array(self):
        self.assertEqual(_get_length(np.array([[1, 2], [3, 4], [5, 6]])), 3)

    def test_shape_without_len(self):
        """Simulate zarr v3 array: has shape but no __len__."""
        class FakeArray:
            shape = (10, 5)
        self.assertEqual(_get_length(FakeArray()), 10)


class TestUnwrapScalar(TestCase):
    """Tests for _unwrap_scalar helper that extracts numpy scalars from 0-d ndarrays.

    The 0-d ndarray tests are the critical ones: numpy scalar indexing (array[0])
    returns numpy scalars (e.g., numpy.float64), but array-API-conforming libraries
    like zarr v3 return 0-d ndarrays instead. A 0-d ndarray is an ndarray with
    shape=() and ndim=0 — it fails isinstance checks against Python scalar types
    (int, float, bool) and type() returns numpy.ndarray rather than the element
    dtype. _unwrap_scalar converts these to proper numpy scalars via .item().
    """

    def test_0_dimensional_float64_array(self):
        result = _unwrap_scalar(np.asarray(3.14))
        self.assertNotIsInstance(result, np.ndarray)
        self.assertIsInstance(result, float)
        self.assertEqual(result, 3.14)

    def test_0_dimensional_int32_array(self):
        result = _unwrap_scalar(np.asarray(42, dtype=np.int32))
        self.assertNotIsInstance(result, np.ndarray)
        self.assertIsInstance(result, int)
        self.assertEqual(result, 42)

    def test_0_dimensional_bool_array(self):
        result = _unwrap_scalar(np.asarray(True))
        self.assertNotIsInstance(result, np.ndarray)
        self.assertIsInstance(result, bool)
        self.assertTrue(result)

    def test_python_int_passthrough(self):
        self.assertEqual(_unwrap_scalar(5), 5)

    def test_python_float_passthrough(self):
        self.assertEqual(_unwrap_scalar(3.14), 3.14)

    def test_python_str_passthrough(self):
        self.assertEqual(_unwrap_scalar("hello"), "hello")

    def test_1d_array_passthrough(self):
        arr = np.array([1, 2, 3])
        result = _unwrap_scalar(arr)
        self.assertIs(result, arr)


class TestGetDataShape(TestCase):

    def test_h5dataset(self):
        """Test get_data_shape on h5py.Datasets of various shapes and maxshape."""
        path = 'test_get_data_shape.h5'
        with h5py.File(path, 'w') as f:
            dset = f.create_dataset('data', data=((1, 2), (3, 4), (5, 6)))
            res = get_data_shape(dset)
            self.assertTupleEqual(res, (3, 2))

            dset = f.create_dataset('shape', shape=(3, 2), dtype='f4')
            res = get_data_shape(dset)
            self.assertTupleEqual(res, (3, 2))

            # test that shape takes priority over maxshape for objects that have both
            dset = f.create_dataset('shape_maxshape', shape=(3, 2), maxshape=(None, 100), dtype='f4')
            res = get_data_shape(dset)
            self.assertTupleEqual(res, (3, 2))

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

    def test_list_with_Data_objects(self):
        # list of Data objects
        res = get_data_shape([Data(name="a", data=[1, 2]), Data(name="b", data=[3, 4])])
        self.assertTupleEqual(res, (2, ))

        # list of list of Data objects
        res = get_data_shape(
            [
                [Data(name="a", data=[1, 2, 3]), Data(name="b", data=[3, 4, 5]), Data(name="c", data=[3, 4, 5])],
                [Data(name="d", data=[1, 2, 3]), Data(name="e", data=[3, 4, 5]), Data(name="f", data=[3, 4, 5])],
            ]
        )
        self.assertTupleEqual(res, (2, 3))


    def test_strict_no_data_load(self):
        """Test get_data_shape with strict_no_data_load=True on nested lists/tuples is the same as when it is False."""
        res = get_data_shape([[1, 2], [3, 4], [5, 6]], strict_no_data_load=True)
        self.assertTupleEqual(res, (3, 2))

        res = get_data_shape(((1, 2), (3, 4), (5, 6)), strict_no_data_load=True)
        self.assertTupleEqual(res, (3, 2))


class TestToUintArray(TestCase):

    def test_ndarray_uint(self):
        arr = np.array([0, 1, 2], dtype=np.uint32)
        res = to_uint_array(arr)
        np.testing.assert_array_equal(res, arr)

    def test_ndarray_int(self):
        arr = np.array([0, 1, 2], dtype=np.int32)
        res = to_uint_array(arr)
        np.testing.assert_array_equal(res, arr)

    def test_ndarray_int_neg(self):
        arr = np.array([0, -1, 2], dtype=np.int32)
        with self.assertRaisesWith(ValueError, 'Cannot convert negative integer values to uint.'):
            to_uint_array(arr)

    def test_ndarray_float(self):
        arr = np.array([0, 1, 2], dtype=np.float64)
        with self.assertRaisesWith(ValueError, 'Cannot convert array of dtype float64 to uint.'):
            to_uint_array(arr)

    def test_list_int(self):
        arr = [0, 1, 2]
        res = to_uint_array(arr)
        expected = np.array([0, 1, 2], dtype=np.uint32)
        np.testing.assert_array_equal(res, expected)

    def test_list_int_neg(self):
        arr = [0, -1, 2]
        with self.assertRaisesWith(ValueError, 'Cannot convert negative integer values to uint.'):
            to_uint_array(arr)

    def test_list_float(self):
        arr = [0., 1., 2.]
        with self.assertRaisesWith(ValueError, 'Cannot convert array of dtype float64 to uint.'):
            to_uint_array(arr)

class TestCoercePandasData(TestCase):
    """Tests for coerce_pandas_data, which normalizes pandas Series/ExtensionArray to numpy."""

    def test_passthrough_non_pandas(self):
        arr = np.array([1, 2, 3])
        self.assertIs(coerce_pandas_data(arr), arr)
        lst = [1, 2, 3]
        self.assertIs(coerce_pandas_data(lst), lst)

    def test_string_array(self):
        sa = pd.array(['a', 'b', 'c'], dtype='string')
        out = coerce_pandas_data(sa)
        self.assertIsInstance(out, np.ndarray)
        self.assertEqual(list(out), ['a', 'b', 'c'])

    def test_arrow_string_array(self):
        try:
            asa = pd.array(['a', 'b', 'c'], dtype='string[pyarrow]')
        except ImportError:
            self.skipTest('pyarrow not installed')
        out = coerce_pandas_data(asa)
        self.assertIsInstance(out, np.ndarray)
        self.assertEqual(list(out), ['a', 'b', 'c'])

    def test_series_string(self):
        s = pd.Series(['a', 'b', 'c'], dtype='string')
        out = coerce_pandas_data(s)
        self.assertIsInstance(out, np.ndarray)
        self.assertEqual(list(out), ['a', 'b', 'c'])

    def test_series_numeric_lossless(self):
        s = pd.Series([1, 2, 3])
        out = coerce_pandas_data(s)
        self.assertIsInstance(out, np.ndarray)
        self.assertEqual(out.dtype, np.int64)
        np.testing.assert_array_equal(out, [1, 2, 3])

    def test_categorical(self):
        cat = pd.Categorical(['x', 'y', 'x'])
        out = coerce_pandas_data(cat)
        self.assertIsInstance(out, np.ndarray)
        self.assertEqual(list(out), ['x', 'y', 'x'])

    def test_string_array_with_na_raises(self):
        sa = pd.array(['a', None, 'c'], dtype='string')
        with self.assertRaisesRegex(TypeError, 'missing values'):
            coerce_pandas_data(sa)

    def test_series_object_with_nan_raises(self):
        s = pd.Series(['a', np.nan, 'c'])
        with self.assertRaisesRegex(TypeError, 'missing values'):
            coerce_pandas_data(s)

    def test_integer_array_lossless(self):
        ia = pd.array([1, 2, 3], dtype='Int64')
        out = coerce_pandas_data(ia)
        self.assertIsInstance(out, np.ndarray)
        self.assertEqual(out.dtype, np.int64)
        np.testing.assert_array_equal(out, [1, 2, 3])

    def test_boolean_array_lossless(self):
        ba = pd.array([True, False, True], dtype='boolean')
        out = coerce_pandas_data(ba)
        self.assertIsInstance(out, np.ndarray)
        self.assertEqual(out.dtype, np.bool_)
        np.testing.assert_array_equal(out, [True, False, True])

    def test_integer_array_with_na_raises(self):
        ia = pd.array([1, None, 3], dtype='Int64')
        with self.assertRaisesRegex(TypeError, 'missing values'):
            coerce_pandas_data(ia)


class TestDataAcceptsPandas(TestCase):
    """Verify pandas Series/ExtensionArray flow through Data construction."""

    def test_vector_data_from_arrow_string_values(self):
        from hdmf.common import VectorData
        df = pd.DataFrame({'animal': ['cat', 'dog', 'bird']})
        vd = VectorData(name='animal', description='', data=df['animal'].values)
        self.assertIsInstance(vd.data, np.ndarray)
        self.assertEqual(list(vd.data), ['cat', 'dog', 'bird'])

    def test_vector_data_from_series(self):
        from hdmf.common import VectorData
        s = pd.Series(['a', 'b', 'c'])
        vd = VectorData(name='s', description='', data=s)
        self.assertIsInstance(vd.data, np.ndarray)
        self.assertEqual(list(vd.data), ['a', 'b', 'c'])


class TestVersionComparison(TestCase):
    """Test the version comparison functionality in NamespaceCatalog."""

    def test_is_newer_version(self):
        """Test basic version comparison scenarios."""
        # test when first version is newer
        self.assertTrue(is_newer_version("10.0.0", "2.0.0"))
        self.assertTrue(is_newer_version("1.1.0", "1.0.0"))
        self.assertTrue(is_newer_version("1.0.1", "1.0.0"))

        # test when second version is newer
        self.assertFalse(is_newer_version("2.0.0", "10.0.0"))
        self.assertFalse(is_newer_version("1.0.0", "1.1.0"))
        self.assertFalse(is_newer_version("1.0.0", "1.0.1"))

        # test when versions are equal
        self.assertFalse(is_newer_version("1.0.0", "1.0.0"))
