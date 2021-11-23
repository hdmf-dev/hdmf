import numpy as np
from pathlib import Path
from tempfile import mkdtemp
from shutil import rmtree

import h5py

from hdmf.data_utils import GenericDataChunkIterator
# from hdmf.backends.hdf5.h5tools import HDF5IO
# from hdmf.backends.hdf5.h5_utils import H5DataIO
from hdmf.testing import TestCase


class GenericDataChunkIteratorTests(TestCase):

    class TestNumpyArrayDataChunkIterator(GenericDataChunkIterator):
        def __init__(self, array: np.ndarray, **kwargs):
            self.array = array
            super().__init__(**kwargs)

        def _get_data(self, selection):
            return self.array[selection]

        def _get_maxshape(self):
            return self.array.shape

        def _get_dtype(self):
            return self.array.dtype

    def setUp(self):
        np.random.seed(seed=0)
        self.test_dir = Path(mkdtemp())
        self.test_array = np.random.randint(low=-2**15, high=2**15-1, size=(2000, 384), dtype="int16")

    def tearDown(self):
        rmtree(self.test_dir)

    def check_first_chunk_call(self, expected_selection, iterator_options):
        test = self.TestNumpyArrayDataChunkIterator(array=self.test_array, **iterator_options)
        first_data_chunk = next(test)
        self.assertEqual(first_data_chunk.selection, expected_selection)
        np.testing.assert_array_equal(first_data_chunk, self.test_array[expected_selection])

    def check_direct_hdf5_write(self, iterator_options):
        iterator = self.TestNumpyArrayDataChunkIterator(array=self.test_array, **iterator_options)
        with h5py.File(name=self.test_dir / "test_generic_iterator_array.hdf5", mode="w") as f:
            dset = f.create_dataset(
                name="test", shape=self.test_array.shape, dtype="int16", chunks=iterator.chunk_shape
            )
            for chunk in iterator:
                dset[chunk.selection] = chunk.data
            np.testing.assert_array_equal(np.array(dset), self.test_array)
            self.assertEqual(dset.chunks, iterator.chunk_shape)

    def test_abstract_assertions(self):
        class TestGenericDataChunkIterator(GenericDataChunkIterator):
            pass
        with self.assertRaises(TypeError) as error:
            TestGenericDataChunkIterator()
        self.assertEqual(
            str(error.exception),
            "Can't instantiate abstract class TestGenericDataChunkIterator with abstract methods "
            "_get_data, _get_dtype, _get_maxshape"
        )

    def test_option_assertions(self):
        with self.assertRaises(AssertionError) as error:
            self.TestNumpyArrayDataChunkIterator(array=self.test_array, buffer_shape=(2000, 384), buffer_gb=1)
        self.assertEqual(str(error.exception), "Only one of 'buffer_gb' or 'buffer_shape' can be specified!")

        with self.assertRaises(AssertionError) as error:
            self.TestNumpyArrayDataChunkIterator(array=self.test_array, chunk_shape=(1580, 316), chunk_mb=1)
        self.assertEqual(str(error.exception), "Only one of 'chunk_mb' or 'chunk_shape' can be specified!")

    def test_numpy_array_chunk_iterator(self):
        iterator_options = dict()
        self.check_first_chunk_call(
          expected_selection=(slice(0, 2000), slice(0, 384)), iterator_options=iterator_options
        )
        self.check_direct_hdf5_write(iterator_options=iterator_options)

    def test_buffer_shape_option(self):
        test_buffer_shape = (1580, 316)
        iterator_options = dict(buffer_shape=test_buffer_shape)
        self.check_first_chunk_call(
            expected_selection=tuple([slice(0, buffer_shape_axis) for buffer_shape_axis in test_buffer_shape]),
            iterator_options=iterator_options,
        )
        self.check_direct_hdf5_write(iterator_options=iterator_options)

    def test_buffer_gb_option(self):
        # buffer is smaller than default chunk; should collapse to chunk shape
        resulting_buffer_shape = (1580, 316)
        iterator_options = dict(buffer_gb=0.0005)
        self.check_first_chunk_call(
            expected_selection=tuple([slice(0, buffer_shape_axis) for buffer_shape_axis in resulting_buffer_shape]),
            iterator_options=iterator_options,
        )
        self.check_direct_hdf5_write(iterator_options=iterator_options)

        # buffer is larger than total data size; should collapse to maxshape
        resulting_buffer_shape = (2000, 384)
        for buffer_gb_input_dtype_pass in [2, 2.]:
            iterator_options = dict(buffer_gb=2)
            self.check_first_chunk_call(
                expected_selection=tuple([slice(0, buffer_shape_axis) for buffer_shape_axis in resulting_buffer_shape]),
                iterator_options=iterator_options,
            )
            self.check_direct_hdf5_write(iterator_options=iterator_options)

    # TODO: need to figure out how to use a HDF5IO with a H5DataIO to test intercommunication
    # def test_chunk_shape_option(self):
    #     test_chunk_shape = (1580, 316)
    #     iterator = self.TestNumpyArrayDataChunkIterator(array=self.test_array, chunk_shape=test_chunk_shape)
    #     self.assertEqual(iterator.chunk_shape, test_chunk_shape)

    # def test_chunk_mb_option(self):
    #     test_chunk_shape = (1115, 223)
    #     iterator = self.TestNumpyArrayDataChunkIterator(array=self.test_array, chunk_mb=.5)
    #     self.assertEqual(iterator.chunk_shape, test_chunk_shape)

    #     # chunk is larger than total data size; should collapse to maxshape
    #     test_chunk_shape = (2000, 384)
    #     iterator = self.TestNumpyArrayDataChunkIterator(array=self.test_array, chunk_mb=2)
    #     self.assertEqual(iterator.chunk_shape, test_chunk_shape)
