import numpy as np
from pathlib import Path
from tempfile import mkdtemp
from shutil import rmtree

import h5py

from hdmf.data_utils import GenericDataChunkIterator
from hdmf.testing import TestCase


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


class GenericDataChunkIteratorTests(TestCase):

    def setUp(self):
        np.random.seed(seed=0)
        self.test_dir = Path(mkdtemp())
        self.test_array = np.random.randint(low=-2**15, high=2**15-1, size=(2000, 384), dtype="int16")

    def tearDown(self):
        rmtree(self.test_dir)

    def check_first_chunk_call(self, expected_selection, iterator_options):
        test = TestNumpyArrayDataChunkIterator(array=self.test_array, **iterator_options)
        first_data_chunk = next(test)
        self.assertEqual(first_data_chunk.selection, expected_selection)
        np.testing.assert_array_equal(first_data_chunk, self.test_array[expected_selection])

    def check_hdf5_write(self, iterator_options):
        with h5py.File(name=self.test_dir / "test_generic_iterator_array.hdf5", mode="w") as f:
            dset = f.create_dataset(name="test", shape=self.test_array.shape, dtype="int16")
            for chunk in TestNumpyArrayDataChunkIterator(array=self.test_array, **iterator_options):
                dset[chunk.selection] = chunk.data
            np.testing.assert_array_equal(np.array(dset), self.test_array)

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

    def test_numpy_array_chunk_iterator(self):
        iterator_options = dict()
        self.check_first_chunk_call(
          expected_selection=(slice(0, 2000), slice(0, 384)), iterator_options=iterator_options
        )
        self.check_hdf5_write(iterator_options=iterator_options)

    def test_buffer_shape_option(self):
        test_buffer_shape = (1580, 316)
        iterator_options = dict(buffer_shape=test_buffer_shape)
        self.check_first_chunk_call(
            expected_selection=tuple([slice(0, buffer_shape_axis) for buffer_shape_axis in test_buffer_shape]),
            iterator_options=iterator_options,
        )
        self.check_hdf5_write(iterator_options=iterator_options)
