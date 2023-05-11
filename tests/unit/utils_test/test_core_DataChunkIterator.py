import numpy as np

from hdmf.data_utils import DataChunkIterator, DataChunk
from hdmf.testing import TestCase


class DataChunkIteratorTests(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_none_iter(self):
        """Test that DataChunkIterator __init__ sets defaults correctly and all chunks and recommended shapes are None.
        """
        dci = DataChunkIterator(dtype=np.dtype('int'))
        self.assertIsNone(dci.maxshape)
        self.assertEqual(dci.dtype, np.dtype('int'))
        self.assertEqual(dci.buffer_size, 1)
        self.assertEqual(dci.iter_axis, 0)
        count = 0
        for chunk in dci:
            pass
        self.assertEqual(count, 0)
        self.assertIsNone(dci.recommended_data_shape())
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_list_none(self):
        """Test that DataChunkIterator has no dtype or chunks when given a list of None.
        """
        a = [None, None, None]
        with self.assertRaisesWith(Exception, 'Data type could not be determined. Please specify dtype in '
                                              'DataChunkIterator init.'):
            DataChunkIterator(a)

    def test_list_none_dtype(self):
        """Test that DataChunkIterator has the passed-in dtype and no chunks when given a list of None.
        """
        a = [None, None, None]
        dci = DataChunkIterator(a, dtype=np.dtype('int'))
        self.assertTupleEqual(dci.maxshape, (3,))
        self.assertEqual(dci.dtype, np.dtype('int'))
        count = 0
        for chunk in dci:
            pass
        self.assertEqual(count, 0)
        self.assertTupleEqual(dci.recommended_data_shape(), (3,))
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_numpy_iter_unbuffered_first_axis(self):
        """Test DataChunkIterator with numpy data, no buffering, and iterating on the first dimension.
        """
        a = np.arange(30).reshape(5, 2, 3)
        dci = DataChunkIterator(data=a, buffer_size=1)
        count = 0
        for chunk in dci:
            self.assertTupleEqual(chunk.shape, (1, 2, 3))
            count += 1
        self.assertEqual(count, 5)
        self.assertTupleEqual(dci.recommended_data_shape(), a.shape)
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_numpy_iter_unbuffered_middle_axis(self):
        """Test DataChunkIterator with numpy data, no buffering, and iterating on a middle dimension.
        """
        a = np.arange(30).reshape(5, 2, 3)
        dci = DataChunkIterator(data=a, buffer_size=1, iter_axis=1)
        count = 0
        for chunk in dci:
            self.assertTupleEqual(chunk.shape, (5, 1, 3))
            count += 1
        self.assertEqual(count, 2)
        self.assertTupleEqual(dci.recommended_data_shape(), a.shape)
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_numpy_iter_unbuffered_last_axis(self):
        """Test DataChunkIterator with numpy data, no buffering, and iterating on the last dimension.
        """
        a = np.arange(30).reshape(5, 2, 3)
        dci = DataChunkIterator(data=a, buffer_size=1, iter_axis=2)
        count = 0
        for chunk in dci:
            self.assertTupleEqual(chunk.shape, (5, 2, 1))
            count += 1
        self.assertEqual(count, 3)
        self.assertTupleEqual(dci.recommended_data_shape(), a.shape)
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_numpy_iter_buffered_first_axis(self):
        """Test DataChunkIterator with numpy data, buffering, and iterating on the first dimension.
        """
        a = np.arange(30).reshape(5, 2, 3)
        dci = DataChunkIterator(data=a, buffer_size=2)
        count = 0
        for chunk in dci:
            if count < 2:
                self.assertTupleEqual(chunk.shape, (2, 2, 3))
            else:
                self.assertTupleEqual(chunk.shape, (1, 2, 3))
            count += 1
        self.assertEqual(count, 3)
        self.assertTupleEqual(dci.recommended_data_shape(), a.shape)
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_numpy_iter_buffered_middle_axis(self):
        """Test DataChunkIterator with numpy data, buffering, and iterating on a middle dimension.
        """
        a = np.arange(45).reshape(5, 3, 3)
        dci = DataChunkIterator(data=a, buffer_size=2, iter_axis=1)
        count = 0
        for chunk in dci:
            if count < 1:
                self.assertTupleEqual(chunk.shape, (5, 2, 3))
            else:
                self.assertTupleEqual(chunk.shape, (5, 1, 3))
            count += 1
        self.assertEqual(count, 2)
        self.assertTupleEqual(dci.recommended_data_shape(), a.shape)
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_numpy_iter_buffered_last_axis(self):
        """Test DataChunkIterator with numpy data, buffering, and iterating on the last dimension.
        """
        a = np.arange(30).reshape(5, 2, 3)
        dci = DataChunkIterator(data=a, buffer_size=2, iter_axis=2)
        count = 0
        for chunk in dci:
            if count < 1:
                self.assertTupleEqual(chunk.shape, (5, 2, 2))
            else:
                self.assertTupleEqual(chunk.shape, (5, 2, 1))
            count += 1
        self.assertEqual(count, 2)
        self.assertTupleEqual(dci.recommended_data_shape(), a.shape)
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_numpy_iter_unmatched_buffer_size(self):
        a = np.arange(10)
        dci = DataChunkIterator(data=a, buffer_size=3)
        self.assertTupleEqual(dci.maxshape, a.shape)
        self.assertEqual(dci.dtype, a.dtype)
        count = 0
        for chunk in dci:
            if count < 3:
                self.assertTupleEqual(chunk.data.shape, (3,))
            else:
                self.assertTupleEqual(chunk.data.shape, (1,))
            count += 1
        self.assertEqual(count, 4)
        self.assertTupleEqual(dci.recommended_data_shape(), a.shape)
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_standard_iterator_unbuffered(self):
        dci = DataChunkIterator(data=range(10), buffer_size=1)
        self.assertEqual(dci.dtype, np.dtype(int))
        self.assertTupleEqual(dci.maxshape, (10,))
        self.assertTupleEqual(dci.recommended_data_shape(), (10,))  # Test before and after iteration
        count = 0
        for chunk in dci:
            self.assertTupleEqual(chunk.data.shape, (1,))
            count += 1
        self.assertEqual(count, 10)
        self.assertTupleEqual(dci.recommended_data_shape(), (10,))  # Test before and after iteration
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_standard_iterator_unmatched_buffersized(self):
        dci = DataChunkIterator(data=range(10), buffer_size=3)
        self.assertEqual(dci.dtype, np.dtype(int))
        self.assertTupleEqual(dci.maxshape, (10,))
        self.assertIsNone(dci.recommended_chunk_shape())
        self.assertTupleEqual(dci.recommended_data_shape(), (10,))  # Test before and after iteration
        count = 0
        for chunk in dci:
            if count < 3:
                self.assertTupleEqual(chunk.data.shape, (3,))
            else:
                self.assertTupleEqual(chunk.data.shape, (1,))
            count += 1
        self.assertEqual(count, 4)
        self.assertTupleEqual(dci.recommended_data_shape(), (10,))  # Test before and after iteration

    def test_multidimensional_list_first_axis(self):
        """Test DataChunkIterator with multidimensional list data, no buffering, and iterating on the first dimension.
        """
        a = np.arange(30).reshape(5, 2, 3).tolist()
        dci = DataChunkIterator(a)
        self.assertTupleEqual(dci.maxshape, (5, 2, 3))
        self.assertEqual(dci.dtype, np.dtype(int))
        count = 0
        for chunk in dci:
            self.assertTupleEqual(chunk.data.shape, (1, 2, 3))
            count += 1
        self.assertEqual(count, 5)
        self.assertTupleEqual(dci.recommended_data_shape(), (5, 2, 3))
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_multidimensional_list_middle_axis(self):
        """Test DataChunkIterator with multidimensional list data, no buffering, and iterating on a middle dimension.
        """
        a = np.arange(30).reshape(5, 2, 3).tolist()
        warn_msg = ('Iterating over an axis other than the first dimension of list or tuple data '
                    'involves converting the data object to a numpy ndarray, which may incur a computational '
                    'cost.')
        with self.assertWarnsWith(UserWarning, warn_msg):
            dci = DataChunkIterator(a, iter_axis=1)
        self.assertTupleEqual(dci.maxshape, (5, 2, 3))
        self.assertEqual(dci.dtype, np.dtype(int))
        count = 0
        for chunk in dci:
            self.assertTupleEqual(chunk.data.shape, (5, 1, 3))
            count += 1
        self.assertEqual(count, 2)
        self.assertTupleEqual(dci.recommended_data_shape(), (5, 2, 3))
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_multidimensional_list_last_axis(self):
        """Test DataChunkIterator with multidimensional list data, no buffering, and iterating on the last dimension.
        """
        a = np.arange(30).reshape(5, 2, 3).tolist()
        warn_msg = ('Iterating over an axis other than the first dimension of list or tuple data '
                    'involves converting the data object to a numpy ndarray, which may incur a computational '
                    'cost.')
        with self.assertWarnsWith(UserWarning, warn_msg):
            dci = DataChunkIterator(a, iter_axis=2)
        self.assertTupleEqual(dci.maxshape, (5, 2, 3))
        self.assertEqual(dci.dtype, np.dtype(int))
        count = 0
        for chunk in dci:
            self.assertTupleEqual(chunk.data.shape, (5, 2, 1))
            count += 1
        self.assertEqual(count, 3)
        self.assertTupleEqual(dci.recommended_data_shape(), (5, 2, 3))
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_maxshape(self):
        a = np.arange(30).reshape(5, 2, 3)
        aiter = iter(a)
        daiter = DataChunkIterator.from_iterable(aiter, buffer_size=2)
        self.assertEqual(daiter.maxshape, (None, 2, 3))

    def test_dtype(self):
        a = np.arange(30, dtype='int32').reshape(5, 2, 3)
        aiter = iter(a)
        daiter = DataChunkIterator.from_iterable(aiter, buffer_size=2)
        self.assertEqual(daiter.dtype, a.dtype)

    def test_sparse_data_buffer_aligned(self):
        a = [1, 2, 3, 4, None, None, 7, 8, None, None]
        dci = DataChunkIterator(a, buffer_size=2)
        self.assertTupleEqual(dci.maxshape, (10,))
        self.assertEqual(dci.dtype, np.dtype(int))
        count = 0
        for chunk in dci:
            self.assertTupleEqual(chunk.data.shape, (2,))
            self.assertEqual(len(chunk.selection), 1)
            self.assertEqual(chunk.selection[0], slice(chunk.data[0] - 1, chunk.data[1]))
            count += 1
        self.assertEqual(count, 3)
        self.assertTupleEqual(dci.recommended_data_shape(), (10,))
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_sparse_data_buffer_notaligned(self):
        a = [1, 2, 3, None, None, None, None, 8, 9, 10]
        dci = DataChunkIterator(a, buffer_size=2)
        self.assertTupleEqual(dci.maxshape, (10,))
        self.assertEqual(dci.dtype, np.dtype(int))
        count = 0
        for chunk in dci:
            self.assertEqual(len(chunk.selection), 1)
            if count == 0:  # [1, 2]
                self.assertListEqual(chunk.data.tolist(), [1, 2])
                self.assertEqual(chunk.selection[0], slice(chunk.data[0] - 1, chunk.data[1]))
            elif count == 1:  # [3, None]
                self.assertListEqual(chunk.data.tolist(), [3, ])
                self.assertEqual(chunk.selection[0], slice(chunk.data[0] - 1, chunk.data[0]))
            elif count == 2:  # [8, 9]
                self.assertListEqual(chunk.data.tolist(), [8, 9])
                self.assertEqual(chunk.selection[0], slice(chunk.data[0] - 1, chunk.data[1]))
            else:  # count == 3, [10]
                self.assertListEqual(chunk.data.tolist(), [10, ])
                self.assertEqual(chunk.selection[0], slice(chunk.data[0] - 1, chunk.data[0]))
            count += 1
        self.assertEqual(count, 4)
        self.assertTupleEqual(dci.recommended_data_shape(), (10,))
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_start_with_none(self):
        a = [None, None, 3]
        dci = DataChunkIterator(a, buffer_size=2)
        self.assertTupleEqual(dci.maxshape, (3,))
        self.assertEqual(dci.dtype, np.dtype(int))
        count = 0
        for chunk in dci:
            self.assertListEqual(chunk.data.tolist(), [3])
            self.assertEqual(len(chunk.selection), 1)
            self.assertEqual(chunk.selection[0], slice(2, 3))
            count += 1
        self.assertEqual(count, 1)
        self.assertTupleEqual(dci.recommended_data_shape(), (3,))
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_list_scalar(self):
        a = [3]
        dci = DataChunkIterator(a, buffer_size=2)
        self.assertTupleEqual(dci.maxshape, (1,))
        self.assertEqual(dci.dtype, np.dtype(int))
        count = 0
        for chunk in dci:
            self.assertListEqual(chunk.data.tolist(), [3])
            self.assertEqual(len(chunk.selection), 1)
            self.assertEqual(chunk.selection[0], slice(0, 1))
            count += 1
        self.assertEqual(count, 1)
        self.assertTupleEqual(dci.recommended_data_shape(), (1,))
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_list_numpy_scalar(self):
        a = np.array([3])
        dci = DataChunkIterator(a, buffer_size=2)
        self.assertTupleEqual(dci.maxshape, (1,))
        self.assertEqual(dci.dtype, np.dtype(int))
        count = 0
        for chunk in dci:
            self.assertListEqual(chunk.data.tolist(), [3])
            self.assertEqual(len(chunk.selection), 1)
            self.assertEqual(chunk.selection[0], slice(0, 1))
            count += 1
        self.assertEqual(count, 1)
        self.assertTupleEqual(dci.recommended_data_shape(), (1,))
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_set_maxshape(self):
        a = np.array([3])
        dci = DataChunkIterator(a, maxshape=(5, 2, 3), buffer_size=2)
        self.assertTupleEqual(dci.maxshape, (5, 2, 3))
        self.assertEqual(dci.dtype, np.dtype(int))
        count = 0
        for chunk in dci:
            self.assertListEqual(chunk.data.tolist(), [3])
            self.assertTupleEqual(chunk.selection, (slice(0, 1), slice(None), slice(None)))
            count += 1
        self.assertEqual(count, 1)
        self.assertTupleEqual(dci.recommended_data_shape(), (5, 2, 3))
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_custom_iter_first_axis(self):
        def my_iter():
            count = 0
            a = np.arange(30).reshape(5, 2, 3)
            while count < a.shape[0]:
                val = a[count, :, :]
                count = count + 1
                yield val
            return
        dci = DataChunkIterator(data=my_iter(), buffer_size=2)
        count = 0
        for chunk in dci:
            if count < 2:
                self.assertTupleEqual(chunk.shape, (2, 2, 3))
            else:
                self.assertTupleEqual(chunk.shape, (1, 2, 3))
            count += 1
        self.assertEqual(count, 3)
        # self.assertTupleEqual(dci.recommended_data_shape(), (2, 2, 3))
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_custom_iter_middle_axis(self):
        def my_iter():
            count = 0
            a = np.arange(45).reshape(5, 3, 3)
            while count < a.shape[1]:
                val = a[:, count, :]
                count = count + 1
                yield val
            return
        dci = DataChunkIterator(data=my_iter(), buffer_size=2, iter_axis=1)
        count = 0
        for chunk in dci:
            if count < 1:
                self.assertTupleEqual(chunk.shape, (5, 2, 3))
            else:
                self.assertTupleEqual(chunk.shape, (5, 1, 3))
            count += 1
        self.assertEqual(count, 2)
        # self.assertTupleEqual(dci.recommended_data_shape(), (5, 2, 3))
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_custom_iter_last_axis(self):
        def my_iter():
            count = 0
            a = np.arange(30).reshape(5, 2, 3)
            while count < a.shape[2]:
                val = a[:, :, count]
                count = count + 1
                yield val
            return
        dci = DataChunkIterator(data=my_iter(), buffer_size=2, iter_axis=2)
        count = 0
        for chunk in dci:
            if count < 1:
                self.assertTupleEqual(chunk.shape, (5, 2, 2))
            else:
                self.assertTupleEqual(chunk.shape, (5, 2, 1))
            count += 1
        self.assertEqual(count, 2)
        # self.assertTupleEqual(dci.recommended_data_shape(), (5, 2, 2))
        self.assertIsNone(dci.recommended_chunk_shape())

    def test_custom_iter_mismatched_axis(self):
        def my_iter():
            count = 0
            a = np.arange(30).reshape(5, 2, 3)
            while count < a.shape[2]:
                val = a[:, :, count]
                count = count + 1
                yield val
            return
        # iterator returns slices of size (5, 2)
        # because iter_axis is by default 0, these chunks will be placed along the first dimension
        dci = DataChunkIterator(data=my_iter(), buffer_size=2)
        count = 0
        for chunk in dci:
            if count < 1:
                self.assertTupleEqual(chunk.shape, (2, 5, 2))
            else:
                self.assertTupleEqual(chunk.shape, (1, 5, 2))
            count += 1
        self.assertEqual(count, 2)
        # self.assertTupleEqual(dci.recommended_data_shape(), (5, 2, 2))
        self.assertIsNone(dci.recommended_chunk_shape())


class DataChunkTests(TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_len_operator_no_data(self):
        temp = DataChunk()
        self.assertEqual(len(temp), 0)

    def test_len_operator_with_data(self):
        temp = DataChunk(np.arange(10).reshape(5, 2))
        self.assertEqual(len(temp), 5)

    def test_dtype(self):
        temp = DataChunk(np.arange(10).astype('int'))
        temp_dtype = temp.dtype
        self.assertEqual(temp_dtype, np.dtype('int'))

    def test_astype(self):
        temp1 = DataChunk(np.arange(10).reshape(5, 2))
        temp2 = temp1.astype('float32')
        self.assertEqual(temp2.dtype, np.dtype('float32'))
