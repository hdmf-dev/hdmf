import unittest
import pickle
import numpy as np
from pathlib import Path
from tempfile import mkdtemp
from shutil import rmtree
from typing import Tuple, Iterable, Callable
from sys import version_info

import h5py
from numpy.testing import assert_array_equal

from hdmf.data_utils import GenericDataChunkIterator
from hdmf.testing import TestCase

try:
    import tqdm  # noqa: F401
    TQDM_INSTALLED = True
except ImportError:
    TQDM_INSTALLED = False


class PickleableNumpyArrayDataChunkIterator(GenericDataChunkIterator):
    def __init__(self, array: np.ndarray, **kwargs):
        self.array = array
        self._kwargs = kwargs
        super().__init__(**kwargs)

    def _get_data(self, selection) -> np.ndarray:
        return self.array[selection]

    def _get_maxshape(self) -> Tuple[int, ...]:
        return self.array.shape

    def _get_dtype(self) -> np.dtype:
        return self.array.dtype

    def _to_dict(self) -> dict:
        return dict(array=pickle.dumps(self.array), kwargs=self._kwargs)

    @staticmethod
    def _from_dict(dictionary: dict) -> Callable:
        array = pickle.loads(dictionary["array"])
        return PickleableNumpyArrayDataChunkIterator(array=array, **dictionary["kwargs"])


class GenericDataChunkIteratorTests(TestCase):
    class TestNumpyArrayDataChunkIterator(GenericDataChunkIterator):
        def __init__(self, array: np.ndarray, **kwargs):
            self.array = array
            super().__init__(**kwargs)

        def _get_data(self, selection) -> np.ndarray:
            return self.array[selection]

        def _get_maxshape(self) -> Tuple[int, ...]:
            return self.array.shape

        def _get_dtype(self) -> np.dtype:
            return self.array.dtype

    class TestNumpyArrayDataChunkIteratorWithNumpyDtypeShape(GenericDataChunkIterator):
        def __init__(self, array: np.ndarray, **kwargs):
            self.array = array
            super().__init__(**kwargs)

        def _get_data(self, selection) -> np.ndarray:
            return self.array[selection]

        def _get_maxshape(self) -> Tuple[np.uint64, ...]:  # Undesirable return type, but can be handled
            return tuple(np.uint64(x) for x in self.array.shape)

        def _get_dtype(self) -> np.dtype:
            return self.array.dtype

    @classmethod
    def setUpClass(cls):
        cls.test_dir = Path(mkdtemp())
        cls.test_array = np.empty(shape=(2000, 384), dtype="int16")

    @classmethod
    def tearDownClass(cls):
        rmtree(cls.test_dir)

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

    def check_all_of_iterable_is_python_int(self, iterable: Iterable):
        assert all(
            tuple(  # Easier to visualize failures in pytest with tuple vs. generator
                isinstance(x, int) for x in iterable
            )
        )

    def test_abstract_assertions(self):
        class TestGenericDataChunkIterator(GenericDataChunkIterator):
            pass

        with self.assertRaisesWith(
            exc_type=TypeError,
            exc_msg=(
                "Can't instantiate abstract class TestGenericDataChunkIterator with abstract methods "
                "_get_data, _get_dtype, _get_maxshape"
            ) if version_info < (3, 12) else (
                "Can't instantiate abstract class TestGenericDataChunkIterator without an "
                "implementation for abstract methods '_get_data', '_get_dtype', '_get_maxshape'"
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

    def test_buffer_option_assertion_negative_buffer_gb(self):
        buffer_gb = -1
        with self.assertRaisesWith(
            exc_type=AssertionError,
            exc_msg=f"buffer_gb ({buffer_gb}) must be greater than zero!"
        ):
            self.TestNumpyArrayDataChunkIterator(array=self.test_array, buffer_gb=buffer_gb)

    def test_buffer_option_assertion_exceed_maxshape(self):
        buffer_shape = (2001, 384)
        with self.assertRaisesWith(
            exc_type=AssertionError,
            exc_msg=(
                f"Some dimensions of buffer_shape ({buffer_shape}) exceed the data "
                f"dimensions ({self.test_array.shape})!"
            )
        ):
            self.TestNumpyArrayDataChunkIterator(array=self.test_array, buffer_shape=buffer_shape)

    def test_buffer_option_assertion_negative_shape(self):
        buffer_shape = (-1, 384)
        with self.assertRaisesWith(
            exc_type=AssertionError,
            exc_msg=f"Some dimensions of buffer_shape ({buffer_shape}) are less than zero!"
        ):
            self.TestNumpyArrayDataChunkIterator(array=self.test_array, buffer_shape=buffer_shape)

    def test_chunk_option_assertion_negative_chunk_mb(self):
        chunk_mb = -1
        with self.assertRaisesWith(
            exc_type=AssertionError,
            exc_msg=f"chunk_mb ({chunk_mb}) must be greater than zero!"
        ):
            self.TestNumpyArrayDataChunkIterator(array=self.test_array, chunk_mb=chunk_mb)

    def test_chunk_option_assertion_negative_shape(self):
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

    def test_private_to_dict_assertion(self):
        with self.assertRaisesWith(
            exc_type=NotImplementedError,
            exc_msg="The `._to_dict()` method for pickling has not been defined for this DataChunkIterator!"
        ):
            iterator = self.TestNumpyArrayDataChunkIterator(array=self.test_array)
            _ = iterator._to_dict()

    def test_private_from_dict_assertion(self):
        with self.assertRaisesWith(
            exc_type=NotImplementedError,
            exc_msg="The `._from_dict()` method for pickling has not been defined for this DataChunkIterator!"
        ):
            _ = self.TestNumpyArrayDataChunkIterator._from_dict(dict())

    def test_direct_pickle_assertion(self):
        with self.assertRaisesWith(
            exc_type=NotImplementedError,
            exc_msg="The `._to_dict()` method for pickling has not been defined for this DataChunkIterator!"
        ):
            iterator = self.TestNumpyArrayDataChunkIterator(array=self.test_array)
            _ = pickle.dumps(iterator)

    def test_maxshape_attribute_contains_int_type(self):
        """Motivated by issues described in https://github.com/hdmf-dev/hdmf/pull/780 & 781 regarding return types."""
        self.check_all_of_iterable_is_python_int(
            iterable=self.TestNumpyArrayDataChunkIterator(array=self.test_array).maxshape
        )

    def test_automated_buffer_shape_attribute_int_type(self):
        """Motivated by issues described in https://github.com/hdmf-dev/hdmf/pull/780 & 781 regarding return types."""
        self.check_all_of_iterable_is_python_int(
            iterable=self.TestNumpyArrayDataChunkIterator(array=self.test_array).buffer_shape
        )

    def test_automated_chunk_shape_attribute_int_type(self):
        """Motivated by issues described in https://github.com/hdmf-dev/hdmf/pull/780 & 781 regarding return types."""
        self.check_all_of_iterable_is_python_int(
            iterable=self.TestNumpyArrayDataChunkIterator(array=self.test_array).chunk_shape
        )

    def test_np_dtype_maxshape_attribute_int_type(self):
        """Motivated by issues described in https://github.com/hdmf-dev/hdmf/pull/780 & 781 regarding return types."""
        self.check_all_of_iterable_is_python_int(
            iterable=self.TestNumpyArrayDataChunkIteratorWithNumpyDtypeShape(array=self.test_array).maxshape
        )

    def test_manual_buffer_shape_attribute_int_type(self):
        """Motivated by issues described in https://github.com/hdmf-dev/hdmf/pull/780 & 781 regarding return types."""
        self.check_all_of_iterable_is_python_int(
            iterable=self.TestNumpyArrayDataChunkIterator(
                array=self.test_array,
                chunk_shape=(np.uint64(100), np.uint64(2)),
                buffer_shape=(np.uint64(200), np.uint64(4)),
            ).buffer_shape
        )

    def test_manual_chunk_shape_attribute_int_type(self):
        """Motivated by issues described in https://github.com/hdmf-dev/hdmf/pull/780 & 781 regarding return types."""
        self.check_all_of_iterable_is_python_int(
            iterable=self.TestNumpyArrayDataChunkIterator(
                array=self.test_array,
                chunk_shape=(np.uint64(100), np.uint64(2))
            ).chunk_shape
        )

    def test_selection_slices_int_type(self):
        """Motivated by issues described in https://github.com/hdmf-dev/hdmf/pull/780 & 781 regarding return types."""
        iterator = self.TestNumpyArrayDataChunkIterator(array=self.test_array)
        first_chunk = next(iterator)
        stop_0 = first_chunk.selection[0].stop
        start_0 = first_chunk.selection[0].start
        stop_1 = first_chunk.selection[1].stop
        start_1 = first_chunk.selection[1].start

        self.check_all_of_iterable_is_python_int(iterable=(stop_0, start_0, stop_1, start_1))

    def test_num_buffers(self):
        buffer_shape = (950, 190)
        chunk_shape = (50, 38)
        expected_num_buffers = 9

        test = self.TestNumpyArrayDataChunkIterator(
            array=self.test_array, buffer_shape=buffer_shape, chunk_shape=chunk_shape
        )
        self.assertEqual(first=test.num_buffers, second=expected_num_buffers)

    def test_numpy_array_chunk_iterator(self):
        iterator_options = dict()
        self.check_first_data_chunk_call(
            expected_selection=(slice(0, 2000), slice(0, 384)), iterator_options=iterator_options
        )
        self.check_direct_hdf5_write(iterator_options=iterator_options)

    def test_buffer_shape_option(self):
        expected_buffer_shape = (1580, 316)
        iterator_options = dict(buffer_shape=expected_buffer_shape, chunk_mb=1.0)
        self.check_first_data_chunk_call(
            expected_selection=tuple([slice(0, buffer_shape_axis) for buffer_shape_axis in expected_buffer_shape]),
            iterator_options=iterator_options,
        )
        self.check_direct_hdf5_write(iterator_options=iterator_options)

    def test_buffer_gb_option(self):
        # buffer is smaller than chunk; should collapse to chunk shape
        resulting_buffer_shape = (1580, 316)
        iterator_options = dict(buffer_gb=0.0005, chunk_mb=1.0)
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
        expected_chunk_shape = (1115, 223)
        iterator = self.TestNumpyArrayDataChunkIterator(array=self.test_array, chunk_mb=0.5)
        self.assertEqual(iterator.chunk_shape, expected_chunk_shape)

    def test_chunk_mb_option_larger_than_total_size(self):
        """Chunk is larger than total data size; should collapse to maxshape."""
        expected_chunk_shape = (2000, 384)
        iterator = self.TestNumpyArrayDataChunkIterator(array=self.test_array, chunk_mb=2)
        self.assertEqual(iterator.chunk_shape, expected_chunk_shape)

    def test_chunk_mb_option_while_condition(self):
        """Test to evoke while condition of default shaping method."""
        expected_chunk_shape = (2, 79, 79)
        special_array = np.random.randint(low=-(2 ** 15), high=2 ** 15 - 1, size=(2, 2000, 2000), dtype="int16")
        iterator = self.TestNumpyArrayDataChunkIterator(array=special_array, chunk_mb=1.0)
        self.assertEqual(iterator.chunk_shape, expected_chunk_shape)

    def test_chunk_mb_option_while_condition_unit_maxshape_axis(self):
        """Test to evoke while condition of default shaping method."""
        expected_chunk_shape = (1, 79, 79)
        special_array = np.random.randint(low=-(2 ** 15), high=2 ** 15 - 1, size=(1, 2000, 2000), dtype="int16")
        iterator = self.TestNumpyArrayDataChunkIterator(array=special_array, chunk_mb=1.0)
        self.assertEqual(iterator.chunk_shape, expected_chunk_shape)

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

    def test_pickle(self):
        pre_dump_iterator = PickleableNumpyArrayDataChunkIterator(array=self.test_array)
        post_dump_iterator = pickle.loads(pickle.dumps(pre_dump_iterator))

        assert isinstance(post_dump_iterator, PickleableNumpyArrayDataChunkIterator)
        assert post_dump_iterator.chunk_shape == pre_dump_iterator.chunk_shape
        assert post_dump_iterator.buffer_shape == pre_dump_iterator.buffer_shape
        assert_array_equal(post_dump_iterator.array, pre_dump_iterator.array)
