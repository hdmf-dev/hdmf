import numpy as np
from pathlib import Path
from tempfile import mkdtemp
from shutil import rmtree
import unittest

import h5py

from hdmf.data_utils import GenericDataChunkIterator
from hdmf.testing import TestCase

try:
    import tqdm  # noqa: F401
    TQDM_INSTALLED = True
except ImportError:
    TQDM_INSTALLED = False


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
        self.test_array = np.random.randint(low=-(2 ** 15), high=2 ** 15 - 1, size=(2000, 384), dtype="int16")

    def tearDown(self):
        rmtree(self.test_dir)

    def check_first_data_chunk_call(self, expected_selection, iterator_options):
        test = self.TestNumpyArrayDataChunkIterator(array=self.test_array, **iterator_options)
        first_data_chunk = next(test)
        self.assertEqual(first_data_chunk.selection, expected_selection)
        np.testing.assert_array_equal(first_data_chunk, self.test_array[expected_selection])

    def check_direct_hdf5_write(self, iterator_options):
        iterator = self.TestNumpyArrayDataChunkIterator(
            array=self.test_array, **iterator_options
        )
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

        with self.assertRaisesWith(
            exc_type=TypeError,
            exc_msg=(
                "Can't instantiate abstract class TestGenericDataChunkIterator with abstract methods "
                "_get_data, _get_dtype, _get_maxshape"
            ),
        ):
            TestGenericDataChunkIterator()

    def test_joint_option_assertions(self):
        with self.assertRaisesWith(
            exc_type=AssertionError,
            exc_msg="Only one of 'buffer_gb' or 'buffer_shape' can be specified!",
        ):
            self.TestNumpyArrayDataChunkIterator(array=self.test_array, buffer_shape=(2000, 384), buffer_gb=1)

        with self.assertRaisesWith(
            exc_type=AssertionError,
            exc_msg="Only one of 'chunk_mb' or 'chunk_shape' can be specified!",
        ):
            self.TestNumpyArrayDataChunkIterator(array=self.test_array, chunk_shape=(1580, 316), chunk_mb=1)

        chunk_shape = (2001, 384)
        with self.assertRaisesWith(
            exc_type=AssertionError,
            exc_msg=(
                f"Some dimensions of chunk_shape ({chunk_shape}) exceed the "
                f"data dimensions ((2000, 384))!"
            ),
        ):
            self.TestNumpyArrayDataChunkIterator(
                array=self.test_array, chunk_shape=chunk_shape
            )

        buffer_shape = (1000, 192)
        chunk_shape = (100, 384)
        with self.assertRaisesWith(
            exc_type=AssertionError,
            exc_msg=(
                f"Some dimensions of chunk_shape ({chunk_shape}) exceed the "
                f"buffer shape ({buffer_shape})!"
            ),
        ):
            self.TestNumpyArrayDataChunkIterator(
                array=self.test_array, buffer_shape=buffer_shape, chunk_shape=chunk_shape
            )

        buffer_shape = (1000, 192)
        chunk_shape = (1000, 5)
        with self.assertRaisesWith(
            exc_type=AssertionError,
            exc_msg=(
                f"Some dimensions of chunk_shape ({chunk_shape}) do not evenly divide the "
                f"buffer shape ({buffer_shape})!"
            ),
        ):
            self.TestNumpyArrayDataChunkIterator(
                array=self.test_array, buffer_shape=buffer_shape, chunk_shape=chunk_shape
            )

    def test_buffer_option_assertions(self):
        buffer_gb = -1
        with self.assertRaisesWith(
            exc_type=AssertionError,
            exc_msg=f"buffer_gb ({buffer_gb}) must be greater than zero!"
        ):
            self.TestNumpyArrayDataChunkIterator(array=self.test_array, buffer_gb=buffer_gb)

        buffer_shape = (-1, 384)
        with self.assertRaisesWith(
            exc_type=AssertionError,
            exc_msg=f"Some dimensions of buffer_shape ({buffer_shape}) are less than zero!"
        ):
            self.TestNumpyArrayDataChunkIterator(array=self.test_array, buffer_shape=buffer_shape)

        buffer_shape = (2001, 384)
        with self.assertRaisesWith(
            exc_type=AssertionError,
            exc_msg=(
                f"Some dimensions of buffer_shape ({buffer_shape}) exceed the data "
                f"dimensions ({self.test_array.shape})!"
            )
        ):
            self.TestNumpyArrayDataChunkIterator(array=self.test_array, buffer_shape=buffer_shape)

    def test_chunk_option_assertions(self):
        chunk_mb = -1
        with self.assertRaisesWith(
            exc_type=AssertionError,
            exc_msg=f"chunk_mb ({chunk_mb}) must be greater than zero!"
        ):
            self.TestNumpyArrayDataChunkIterator(array=self.test_array, chunk_mb=chunk_mb)

        chunk_shape = (-1, 384)
        with self.assertRaisesWith(
            exc_type=AssertionError,
            exc_msg=f"Some dimensions of chunk_shape ({chunk_shape}) are less than zero!"
        ):
            self.TestNumpyArrayDataChunkIterator(array=self.test_array, chunk_shape=chunk_shape)

    @unittest.skipIf(not TQDM_INSTALLED, "optional tqdm module is not installed")
    def test_progress_bar_assertion(self):
        with self.assertWarnsWith(
            warn_type=UserWarning,
            exc_msg="Option 'total' in 'progress_bar_options' is not allowed to be over-written! Ignoring."
        ):
            _ = self.TestNumpyArrayDataChunkIterator(
                array=self.test_array,
                display_progress=True,
                progress_bar_options=dict(total=5),
            )

    def test_num_buffers(self):
        buffer_shape = (950, 190)
        chunk_shape = (50, 38)
        test = self.TestNumpyArrayDataChunkIterator(
            array=self.test_array, buffer_shape=buffer_shape, chunk_shape=chunk_shape
        )
        self.assertEqual(first=test.num_buffers, second=9)

    def test_numpy_array_chunk_iterator(self):
        iterator_options = dict()
        self.check_first_data_chunk_call(
            expected_selection=(slice(0, 2000), slice(0, 384)), iterator_options=iterator_options
        )
        self.check_direct_hdf5_write(iterator_options=iterator_options)

    def test_buffer_shape_option(self):
        test_buffer_shape = (1580, 316)
        iterator_options = dict(buffer_shape=test_buffer_shape)
        self.check_first_data_chunk_call(
            expected_selection=tuple([slice(0, buffer_shape_axis) for buffer_shape_axis in test_buffer_shape]),
            iterator_options=iterator_options,
        )
        self.check_direct_hdf5_write(iterator_options=iterator_options)

    def test_buffer_gb_option(self):
        # buffer is smaller than default chunk; should collapse to chunk shape
        resulting_buffer_shape = (1580, 316)
        iterator_options = dict(buffer_gb=0.0005)
        self.check_first_data_chunk_call(
            expected_selection=tuple(
                [
                    slice(0, buffer_shape_axis)
                    for buffer_shape_axis in resulting_buffer_shape
                ]
            ),
            iterator_options=iterator_options,
        )
        self.check_direct_hdf5_write(iterator_options=iterator_options)

        # buffer is larger than total data size; should collapse to maxshape
        resulting_buffer_shape = (2000, 384)
        for buffer_gb_input_dtype_pass in [2, 2.0]:
            iterator_options = dict(buffer_gb=2)
            self.check_first_data_chunk_call(
                expected_selection=tuple(
                    [
                        slice(0, buffer_shape_axis)
                        for buffer_shape_axis in resulting_buffer_shape
                    ]
                ),
                iterator_options=iterator_options,
            )
            self.check_direct_hdf5_write(iterator_options=iterator_options)

    def test_chunk_shape_option(self):
        test_chunk_shape = (1580, 316)
        iterator = self.TestNumpyArrayDataChunkIterator(array=self.test_array, chunk_shape=test_chunk_shape)
        self.assertEqual(iterator.chunk_shape, test_chunk_shape)

    def test_chunk_mb_option(self):
        test_chunk_shape = (1115, 223)
        iterator = self.TestNumpyArrayDataChunkIterator(array=self.test_array, chunk_mb=0.5)
        self.assertEqual(iterator.chunk_shape, test_chunk_shape)

        # chunk is larger than total data size; should collapse to maxshape
        test_chunk_shape = (2000, 384)
        iterator = self.TestNumpyArrayDataChunkIterator(array=self.test_array, chunk_mb=2)
        self.assertEqual(iterator.chunk_shape, test_chunk_shape)

        # test to evoke while condition of default shaping method
        test_chunk_shape = (1, 79, 79)
        special_array = np.random.randint(low=-(2 ** 15), high=2 ** 15 - 1, size=(1, 2000, 2000), dtype="int16")
        iterator = self.TestNumpyArrayDataChunkIterator(array=special_array)
        self.assertEqual(iterator.chunk_shape, test_chunk_shape)

    @unittest.skipIf(not TQDM_INSTALLED, "optional tqdm module is not installed")
    def test_progress_bar(self):
        out_text_file = self.test_dir / "test_progress_bar.txt"
        desc = "Testing progress bar..."
        with open(file=out_text_file, mode="w") as file:
            iterator = self.TestNumpyArrayDataChunkIterator(
                array=self.test_array, display_progress=True, progress_bar_options=dict(file=file, desc=desc)
            )
            j = 0
            for buffer in iterator:
                j += 1  # dummy operation; must be silent for proper updating of bar
        with open(file=out_text_file, mode="r") as file:
            first_line = file.read()
            self.assertIn(member=desc, container=first_line)

    @unittest.skipIf(not TQDM_INSTALLED, "optional tqdm module is installed")
    def test_progress_bar_no_options(self):
        dci = self.TestNumpyArrayDataChunkIterator(array=self.test_array, display_progress=True)
        self.assertIsNotNone(dci.progress_bar)
        self.assertTrue(dci.display_progress)

    @unittest.skipIf(TQDM_INSTALLED, "optional tqdm module is not installed")
    def test_tqdm_not_installed(self):
        with self.assertWarnsWith(
            warn_type=UserWarning,
            exc_msg=("You must install tqdm to use the progress bar feature (pip install tqdm)! "
                     "Progress bar is disabled.")
        ):
            dci = self.TestNumpyArrayDataChunkIterator(
                array=self.test_array,
                display_progress=True,
            )
            self.assertFalse(dci.display_progress)
