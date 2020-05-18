import os
import unittest
import tempfile
import warnings
import numpy as np
import h5py
from io import BytesIO

from hdmf.utils import docval, getargs
from hdmf.data_utils import DataChunkIterator, InvalidDataIOError
from hdmf.backends.hdf5.h5tools import HDF5IO, ROOT_NAME
from hdmf.backends.hdf5 import H5DataIO
from hdmf.backends.io import UnsupportedOperation
from hdmf.build import GroupBuilder, DatasetBuilder, BuildManager, TypeMap, ObjectMapper
from hdmf.spec.namespace import NamespaceCatalog
from hdmf.spec.spec import AttributeSpec, DatasetSpec, GroupSpec, ZERO_OR_MANY, ONE_OR_MANY
from hdmf.spec.namespace import SpecNamespace
from hdmf.spec.catalog import SpecCatalog
from hdmf.container import Container
from hdmf.testing import TestCase

from h5py import SoftLink, HardLink, ExternalLink, File
from h5py import filters as h5py_filters

from tests.unit.utils import Foo, FooBucket, CORE_NAMESPACE


class FooFile(Container):

    @docval({'name': 'buckets', 'type': list, 'doc': 'the FooBuckets in this file', 'default': list()})
    def __init__(self, **kwargs):
        buckets = getargs('buckets', kwargs)
        super().__init__(name=ROOT_NAME)  # name is not used - FooFile should be the root container
        self.__buckets = buckets
        for f in self.__buckets:
            f.parent = self

    def __eq__(self, other):
        return set(self.buckets) == set(other.buckets)

    def __str__(self):
        foo_str = "[" + ",".join(str(f) for f in self.buckets) + "]"
        return 'buckets=%s' % foo_str

    @property
    def buckets(self):
        return self.__buckets


def get_temp_filepath():
    # On Windows, h5py cannot truncate an open file in write mode.
    # The temp file will be closed before h5py truncates it and will be removed during the tearDown step.
    temp_file = tempfile.NamedTemporaryFile()
    temp_file.close()
    return temp_file.name


class H5IOTest(TestCase):
    """Tests for h5tools IO tools"""

    def setUp(self):
        self.path = get_temp_filepath()
        self.io = HDF5IO(self.path, mode='a')
        self.f = self.io._file

    def tearDown(self):
        self.io.close()
        os.remove(self.path)

    ##########################################
    #  __chunked_iter_fill__(...) tests
    ##########################################
    def test__chunked_iter_fill(self):
        """Matrix test of HDF5IO.__chunked_iter_fill__ using a DataChunkIterator with different parameters"""
        data_opts = {'iterator': range(10),
                     'numpy': np.arange(30).reshape(5, 2, 3),
                     'list': np.arange(30).reshape(5, 2, 3).tolist(),
                     'sparselist1': [1, 2, 3, None, None, None, None, 8, 9, 10],
                     'sparselist2': [None, None, 3],
                     'sparselist3': [1, 2, 3, None, None],  # note: cannot process None in ndarray
                     'nanlist': [[[1, 2, 3, np.nan, np.nan, 6], [np.nan, np.nan, 3, 4, np.nan, np.nan]],
                                 [[10, 20, 30, 40, np.nan, np.nan], [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan]]]}
        buffer_size_opts = [1, 2, 3, 4]  # data is divisible by some of these, some not
        for data_type, data in data_opts.items():
            iter_axis_opts = [0, 1, 2]
            if data_type == 'iterator' or data_type.startswith('sparselist'):
                iter_axis_opts = [0]  # only one dimension

            for iter_axis in iter_axis_opts:
                for buffer_size in buffer_size_opts:
                    with self.subTest(data_type=data_type, iter_axis=iter_axis, buffer_size=buffer_size):
                        with warnings.catch_warnings(record=True) as w:
                            dci = DataChunkIterator(data=data, buffer_size=buffer_size, iter_axis=iter_axis)
                            if len(w) <= 1:
                                # init may throw UserWarning for iterating over not-first dim of a list. ignore here
                                pass

                        dset_name = '%s, %d, %d' % (data_type, iter_axis, buffer_size)
                        my_dset = HDF5IO.__chunked_iter_fill__(self.f, dset_name, dci)

                        if data_type == 'iterator':
                            self.assertListEqual(my_dset[:].tolist(), list(data))
                        elif data_type == 'numpy':
                            self.assertTrue(np.all(my_dset[:] == data))
                            self.assertTupleEqual(my_dset.shape, data.shape)
                        elif data_type == 'list' or data_type == 'nanlist':
                            data_np = np.array(data)
                            np.testing.assert_array_equal(my_dset[:], data_np)
                            self.assertTupleEqual(my_dset.shape, data_np.shape)
                        elif data_type.startswith('sparselist'):
                            # replace None in original data with default hdf5 fillvalue 0
                            data_zeros = np.where(np.equal(np.array(data), None), 0, data)
                            np.testing.assert_array_equal(my_dset[:], data_zeros)
                            self.assertTupleEqual(my_dset.shape, data_zeros.shape)

    ##########################################
    #  write_dataset tests: scalars
    ##########################################
    def test_write_dataset_scalar(self):
        a = 10
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', a, attributes={}))
        dset = self.f['test_dataset']
        self.assertTupleEqual(dset.shape, ())
        self.assertEqual(dset[()], a)

    def test_write_dataset_string(self):
        a = 'test string'
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', a, attributes={}))
        dset = self.f['test_dataset']
        self.assertTupleEqual(dset.shape, ())
        # self.assertEqual(dset[()].decode('utf-8'), a)
        self.assertEqual(dset[()], a)

    ##########################################
    #  write_dataset tests: lists
    ##########################################
    def test_write_dataset_list(self):
        a = np.arange(30).reshape(5, 2, 3)
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', a.tolist(), attributes={}))
        dset = self.f['test_dataset']
        self.assertTrue(np.all(dset[:] == a))

    def test_write_dataset_list_compress_gzip(self):
        a = H5DataIO(np.arange(30).reshape(5, 2, 3),
                     compression='gzip',
                     compression_opts=5,
                     shuffle=True,
                     fletcher32=True)
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', a, attributes={}))
        dset = self.f['test_dataset']
        self.assertTrue(np.all(dset[:] == a.data))
        self.assertEqual(dset.compression, 'gzip')
        self.assertEqual(dset.compression_opts, 5)
        self.assertEqual(dset.shuffle, True)
        self.assertEqual(dset.fletcher32, True)

    @unittest.skipIf("lzf" not in h5py_filters.encode,
                     "LZF compression not supported in this h5py library install")
    def test_write_dataset_list_compress_lzf(self):
        warn_msg = ("lzf compression may not be available on all installations of HDF5. Use of gzip is "
                    "recommended to ensure portability of the generated HDF5 files.")
        with self.assertWarnsWith(UserWarning, warn_msg):
            a = H5DataIO(np.arange(30).reshape(5, 2, 3),
                         compression='lzf',
                         shuffle=True,
                         fletcher32=True)
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', a, attributes={}))
        dset = self.f['test_dataset']
        self.assertTrue(np.all(dset[:] == a.data))
        self.assertEqual(dset.compression, 'lzf')
        self.assertEqual(dset.shuffle, True)
        self.assertEqual(dset.fletcher32, True)

    @unittest.skipIf("szip" not in h5py_filters.encode,
                     "SZIP compression not supported in this h5py library install")
    def test_write_dataset_list_compress_szip(self):
        warn_msg = ("szip compression may not be available on all installations of HDF5. Use of gzip is "
                    "recommended to ensure portability of the generated HDF5 files.")
        with self.assertWarnsWith(UserWarning, warn_msg):
            a = H5DataIO(np.arange(30).reshape(5, 2, 3),
                         compression='szip',
                         compression_opts=('ec', 16),
                         shuffle=True,
                         fletcher32=True)
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', a, attributes={}))
        dset = self.f['test_dataset']
        self.assertTrue(np.all(dset[:] == a.data))
        self.assertEqual(dset.compression, 'szip')
        self.assertEqual(dset.shuffle, True)
        self.assertEqual(dset.fletcher32, True)

    def test_write_dataset_list_compress_available_int_filters(self):
        a = H5DataIO(np.arange(30).reshape(5, 2, 3),
                     compression=1,
                     shuffle=True,
                     fletcher32=True,
                     allow_plugin_filters=True)
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', a, attributes={}))
        dset = self.f['test_dataset']
        self.assertTrue(np.all(dset[:] == a.data))
        self.assertEqual(dset.compression, 'gzip')
        self.assertEqual(dset.shuffle, True)
        self.assertEqual(dset.fletcher32, True)

    def test_write_dataset_list_enable_default_compress(self):
        a = H5DataIO(np.arange(30).reshape(5, 2, 3),
                     compression=True)
        self.assertEqual(a.io_settings['compression'], 'gzip')
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', a, attributes={}))
        dset = self.f['test_dataset']
        self.assertTrue(np.all(dset[:] == a.data))
        self.assertEqual(dset.compression, 'gzip')

    def test_write_dataset_list_disable_default_compress(self):
        with warnings.catch_warnings(record=True) as w:
            a = H5DataIO(np.arange(30).reshape(5, 2, 3),
                         compression=False,
                         compression_opts=5)
            self.assertEqual(len(w), 1)  # We expect a warning that compression options are being ignored
            self.assertFalse('compression_ops' in a.io_settings)
            self.assertFalse('compression' in a.io_settings)

        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', a, attributes={}))
        dset = self.f['test_dataset']
        self.assertTrue(np.all(dset[:] == a.data))
        self.assertEqual(dset.compression, None)

    def test_write_dataset_list_chunked(self):
        a = H5DataIO(np.arange(30).reshape(5, 2, 3),
                     chunks=(1, 1, 3))
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', a, attributes={}))
        dset = self.f['test_dataset']
        self.assertTrue(np.all(dset[:] == a.data))
        self.assertEqual(dset.chunks, (1, 1, 3))

    def test_write_dataset_list_fillvalue(self):
        a = H5DataIO(np.arange(20).reshape(5, 4), fillvalue=-1)
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', a, attributes={}))
        dset = self.f['test_dataset']
        self.assertTrue(np.all(dset[:] == a.data))
        self.assertEqual(dset.fillvalue, -1)

    ##########################################
    #  write_dataset tests: tables
    ##########################################
    def test_write_table(self):
        cmpd_dt = np.dtype([('a', np.int32), ('b', np.float64)])
        data = np.zeros(10, dtype=cmpd_dt)
        data['a'][1] = 101
        data['b'][1] = 0.1
        dt = [{'name': 'a', 'dtype': 'int32', 'doc': 'a column'},
              {'name': 'b', 'dtype': 'float64', 'doc': 'b column'}]
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', data, attributes={}, dtype=dt))
        dset = self.f['test_dataset']
        self.assertEqual(dset['a'].tolist(), data['a'].tolist())
        self.assertEqual(dset['b'].tolist(), data['b'].tolist())

    def test_write_table_nested(self):
        b_cmpd_dt = np.dtype([('c', np.int32), ('d', np.float64)])
        cmpd_dt = np.dtype([('a', np.int32), ('b', b_cmpd_dt)])
        data = np.zeros(10, dtype=cmpd_dt)
        data['a'][1] = 101
        data['b']['c'] = 202
        data['b']['d'] = 10.1
        b_dt = [{'name': 'c', 'dtype': 'int32', 'doc': 'c column'},
                {'name': 'd', 'dtype': 'float64', 'doc': 'd column'}]
        dt = [{'name': 'a', 'dtype': 'int32', 'doc': 'a column'},
              {'name': 'b', 'dtype': b_dt, 'doc': 'b column'}]
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', data, attributes={}, dtype=dt))
        dset = self.f['test_dataset']
        self.assertEqual(dset['a'].tolist(), data['a'].tolist())
        self.assertEqual(dset['b'].tolist(), data['b'].tolist())

    ##########################################
    #  write_dataset tests: Iterable
    ##########################################
    def test_write_dataset_iterable(self):
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', range(10), attributes={}))
        dset = self.f['test_dataset']
        self.assertListEqual(dset[:].tolist(), list(range(10)))

    def test_write_dataset_iterable_multidimensional_array(self):
        a = np.arange(30).reshape(5, 2, 3)
        aiter = iter(a)
        daiter = DataChunkIterator.from_iterable(aiter, buffer_size=2)
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', daiter, attributes={}))
        dset = self.f['test_dataset']
        self.assertListEqual(dset[:].tolist(), a.tolist())

    def test_write_multi_dci_oaat(self):
        """
        Test writing multiple DataChunkIterators, one at a time
        """
        a = np.arange(30).reshape(5, 2, 3)
        b = np.arange(30, 60).reshape(5, 2, 3)
        aiter = iter(a)
        biter = iter(b)
        daiter1 = DataChunkIterator.from_iterable(aiter, buffer_size=2)
        daiter2 = DataChunkIterator.from_iterable(biter, buffer_size=2)
        builder = GroupBuilder("root")
        builder.add_dataset('test_dataset1', daiter1, attributes={})
        builder.add_dataset('test_dataset2', daiter2, attributes={})
        self.io.write_builder(builder)
        dset1 = self.f['test_dataset1']
        self.assertListEqual(dset1[:].tolist(), a.tolist())
        dset2 = self.f['test_dataset2']
        self.assertListEqual(dset2[:].tolist(), b.tolist())

    def test_write_multi_dci_conc(self):
        """
        Test writing multiple DataChunkIterators, concurrently
        """
        a = np.arange(30).reshape(5, 2, 3)
        b = np.arange(30, 60).reshape(5, 2, 3)
        aiter = iter(a)
        biter = iter(b)
        daiter1 = DataChunkIterator.from_iterable(aiter, buffer_size=2)
        daiter2 = DataChunkIterator.from_iterable(biter, buffer_size=2)
        builder = GroupBuilder("root")
        builder.add_dataset('test_dataset1', daiter1, attributes={})
        builder.add_dataset('test_dataset2', daiter2, attributes={})
        self.io.write_builder(builder)
        dset1 = self.f['test_dataset1']
        self.assertListEqual(dset1[:].tolist(), a.tolist())
        dset2 = self.f['test_dataset2']
        self.assertListEqual(dset2[:].tolist(), b.tolist())

    def test_write_dataset_iterable_multidimensional_array_compression(self):
        a = np.arange(30).reshape(5, 2, 3)
        aiter = iter(a)
        daiter = DataChunkIterator.from_iterable(aiter, buffer_size=2)
        wrapped_daiter = H5DataIO(data=daiter,
                                  compression='gzip',
                                  compression_opts=5,
                                  shuffle=True,
                                  fletcher32=True)
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', wrapped_daiter, attributes={}))
        dset = self.f['test_dataset']
        self.assertEqual(dset.shape, a.shape)
        self.assertListEqual(dset[:].tolist(), a.tolist())
        self.assertEqual(dset.compression, 'gzip')
        self.assertEqual(dset.compression_opts, 5)
        self.assertEqual(dset.shuffle, True)
        self.assertEqual(dset.fletcher32, True)

    #############################################
    #  write_dataset tests: data chunk iterator
    #############################################
    def test_write_dataset_data_chunk_iterator(self):
        dci = DataChunkIterator(data=np.arange(10), buffer_size=2)
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', dci, attributes={}, dtype=dci.dtype))
        dset = self.f['test_dataset']
        self.assertListEqual(dset[:].tolist(), list(range(10)))
        self.assertEqual(dset[:].dtype, dci.dtype)

    def test_write_dataset_data_chunk_iterator_with_compression(self):
        dci = DataChunkIterator(data=np.arange(10), buffer_size=2)
        wrapped_dci = H5DataIO(data=dci,
                               compression='gzip',
                               compression_opts=5,
                               shuffle=True,
                               fletcher32=True,
                               chunks=(2,))
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', wrapped_dci, attributes={}))
        dset = self.f['test_dataset']
        self.assertListEqual(dset[:].tolist(), list(range(10)))
        self.assertEqual(dset.compression, 'gzip')
        self.assertEqual(dset.compression_opts, 5)
        self.assertEqual(dset.shuffle, True)
        self.assertEqual(dset.fletcher32, True)
        self.assertEqual(dset.chunks, (2,))

    def test_pass_through_of_recommended_chunks(self):

        class DC(DataChunkIterator):
            def recommended_chunk_shape(self):
                return (5, 1, 1)

        dci = DC(data=np.arange(30).reshape(5, 2, 3))
        wrapped_dci = H5DataIO(data=dci,
                               compression='gzip',
                               compression_opts=5,
                               shuffle=True,
                               fletcher32=True)
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', wrapped_dci, attributes={}))
        dset = self.f['test_dataset']
        self.assertEqual(dset.chunks, (5, 1, 1))
        self.assertEqual(dset.compression, 'gzip')
        self.assertEqual(dset.compression_opts, 5)
        self.assertEqual(dset.shuffle, True)
        self.assertEqual(dset.fletcher32, True)

    def test_dci_h5dataset(self):
        data = np.arange(30).reshape(5, 2, 3)
        dci1 = DataChunkIterator(data=data, buffer_size=1, iter_axis=0)
        HDF5IO.__chunked_iter_fill__(self.f, 'test_dataset', dci1)
        dset = self.f['test_dataset']
        dci2 = DataChunkIterator(data=dset, buffer_size=2, iter_axis=2)

        chunk = dci2.next()
        self.assertTupleEqual(chunk.shape, (5, 2, 2))
        chunk = dci2.next()
        self.assertTupleEqual(chunk.shape, (5, 2, 1))

        # TODO test chunk data, shape, selection

        self.assertTupleEqual(dci2.recommended_data_shape(), data.shape)
        self.assertIsNone(dci2.recommended_chunk_shape())

    def test_dci_h5dataset_sparse_matched(self):
        data = [1, 2, 3, None, None, None, None, 8, 9, 10]
        dci1 = DataChunkIterator(data=data, buffer_size=3)
        HDF5IO.__chunked_iter_fill__(self.f, 'test_dataset', dci1)
        dset = self.f['test_dataset']
        dci2 = DataChunkIterator(data=dset, buffer_size=2)
        # dataset is read such that Nones in original data were not written, but are read as 0s

        self.assertTupleEqual(dci2.maxshape, (10,))
        self.assertEqual(dci2.dtype, np.dtype(int))
        count = 0
        for chunk in dci2:
            self.assertEqual(len(chunk.selection), 1)
            if count == 0:
                self.assertListEqual(chunk.data.tolist(), [1, 2])
                self.assertEqual(chunk.selection[0], slice(0, 2))
            elif count == 1:
                self.assertListEqual(chunk.data.tolist(), [3, 0])
                self.assertEqual(chunk.selection[0], slice(2, 4))
            elif count == 2:
                self.assertListEqual(chunk.data.tolist(), [0, 0])
                self.assertEqual(chunk.selection[0], slice(4, 6))
            elif count == 3:
                self.assertListEqual(chunk.data.tolist(), [0, 8])
                self.assertEqual(chunk.selection[0], slice(6, 8))
            elif count == 4:
                self.assertListEqual(chunk.data.tolist(), [9, 10])
                self.assertEqual(chunk.selection[0], slice(8, 10))
            count += 1

        self.assertEqual(count, 5)
        self.assertTupleEqual(dci2.recommended_data_shape(), (10,))
        self.assertIsNone(dci2.recommended_chunk_shape())

    def test_dci_h5dataset_sparse_unmatched(self):
        data = [1, 2, 3, None, None, None, None, 8, 9, 10]
        dci1 = DataChunkIterator(data=data, buffer_size=3)
        HDF5IO.__chunked_iter_fill__(self.f, 'test_dataset', dci1)
        dset = self.f['test_dataset']
        dci2 = DataChunkIterator(data=dset, buffer_size=4)
        # dataset is read such that Nones in original data were not written, but are read as 0s

        self.assertTupleEqual(dci2.maxshape, (10,))
        self.assertEqual(dci2.dtype, np.dtype(int))
        count = 0
        for chunk in dci2:
            self.assertEqual(len(chunk.selection), 1)
            if count == 0:
                self.assertListEqual(chunk.data.tolist(), [1, 2, 3, 0])
                self.assertEqual(chunk.selection[0], slice(0, 4))
            elif count == 1:
                self.assertListEqual(chunk.data.tolist(), [0, 0, 0, 8])
                self.assertEqual(chunk.selection[0], slice(4, 8))
            elif count == 2:
                self.assertListEqual(chunk.data.tolist(), [9, 10])
                self.assertEqual(chunk.selection[0], slice(8, 10))
            count += 1

        self.assertEqual(count, 3)
        self.assertTupleEqual(dci2.recommended_data_shape(), (10,))
        self.assertIsNone(dci2.recommended_chunk_shape())

    def test_dci_h5dataset_scalar(self):
        data = [1]
        dci1 = DataChunkIterator(data=data, buffer_size=3)
        HDF5IO.__chunked_iter_fill__(self.f, 'test_dataset', dci1)
        dset = self.f['test_dataset']
        dci2 = DataChunkIterator(data=dset, buffer_size=4)
        # dataset is read such that Nones in original data were not written, but are read as 0s

        self.assertTupleEqual(dci2.maxshape, (1,))
        self.assertEqual(dci2.dtype, np.dtype(int))
        count = 0
        for chunk in dci2:
            self.assertEqual(len(chunk.selection), 1)
            if count == 0:
                self.assertListEqual(chunk.data.tolist(), [1])
                self.assertEqual(chunk.selection[0], slice(0, 1))
            count += 1

        self.assertEqual(count, 1)
        self.assertTupleEqual(dci2.recommended_data_shape(), (1,))
        self.assertIsNone(dci2.recommended_chunk_shape())

    #############################################
    #  H5DataIO general
    #############################################
    def test_warning_on_non_gzip_compression(self):
        # Make sure no warning is issued when using gzip
        with warnings.catch_warnings(record=True) as w:
            dset = H5DataIO(np.arange(30),
                            compression='gzip')
            self.assertEqual(len(w), 0)
            self.assertEqual(dset.io_settings['compression'], 'gzip')
        # Make sure a warning is issued when using szip (even if installed)
        if "szip" in h5py_filters.encode:
            with warnings.catch_warnings(record=True) as w:
                dset = H5DataIO(np.arange(30),
                                compression='szip',
                                compression_opts=('ec', 16))
                self.assertEqual(len(w), 1)
                self.assertEqual(dset.io_settings['compression'], 'szip')
        else:
            with self.assertRaises(ValueError):
                H5DataIO(np.arange(30), compression='szip', compression_opts=('ec', 16))
        # Make sure a warning is issued when using lzf compression
        with warnings.catch_warnings(record=True) as w:
            dset = H5DataIO(np.arange(30),
                            compression='lzf')
            self.assertEqual(len(w), 1)
            self.assertEqual(dset.io_settings['compression'], 'lzf')

    def test_error_on_unsupported_compression_filter(self):
        # Make sure gzip does not raise an error
        try:
            H5DataIO(np.arange(30), compression='gzip', compression_opts=5)
        except ValueError:
            self.fail("Using gzip compression raised a ValueError when it should not")
        # Make sure szip raises an error if not installed (or does not raise an error if installed)
        warn_msg = ("szip compression may not be available on all installations of HDF5. Use of gzip is "
                    "recommended to ensure portability of the generated HDF5 files.")
        if "szip" not in h5py_filters.encode:
            with self.assertRaises(ValueError):
                H5DataIO(np.arange(30), compression='szip', compression_opts=('ec', 16))
        else:
            try:
                with self.assertWarnsWith(UserWarning, warn_msg):
                    H5DataIO(np.arange(30), compression='szip', compression_opts=('ec', 16))
            except ValueError:
                self.fail("SZIP is installed but H5DataIO still raises an error")
        # Test error on illegal (i.e., a made-up compressor)
        with self.assertRaises(ValueError):
            warn_msg = ("unknown compression may not be available on all installations of HDF5. Use of gzip is "
                        "recommended to ensure portability of the generated HDF5 files.")
            with self.assertWarnsWith(UserWarning, warn_msg):
                H5DataIO(np.arange(30), compression="unknown")
        # Make sure passing int compression filter raise an error if not installed
        if not h5py_filters.h5z.filter_avail(h5py_filters.h5z.FILTER_MAX):
            with self.assertRaises(ValueError):
                warn_msg = ("%i compression may not be available on all installations of HDF5. Use of gzip is "
                            "recommended to ensure portability of the generated HDF5 files."
                            % h5py_filters.h5z.FILTER_MAX)
                with self.assertWarnsWith(UserWarning, warn_msg):
                    H5DataIO(np.arange(30), compression=h5py_filters.h5z.FILTER_MAX, allow_plugin_filters=True)
        # Make sure available int compression filters raise an error without passing allow_plugin_filters=True
        with self.assertRaises(ValueError):
            H5DataIO(np.arange(30), compression=h5py_filters.h5z.FILTER_DEFLATE)

    def test_value_error_on_incompatible_compression_opts(self):
        # Make sure we warn when gzip with szip compression options is used
        with self.assertRaises(ValueError):
            H5DataIO(np.arange(30), compression='gzip', compression_opts=('ec', 16))
        # Make sure we warn if gzip with a too high agression is used
        with self.assertRaises(ValueError):
            H5DataIO(np.arange(30), compression='gzip', compression_opts=100)
        # Make sure we warn if lzf with gzip compression option is used
        with self.assertRaises(ValueError):
            H5DataIO(np.arange(30), compression='lzf', compression_opts=5)
        # Make sure we warn if lzf with szip compression option is used
        with self.assertRaises(ValueError):
            H5DataIO(np.arange(30), compression='lzf', compression_opts=('ec', 16))
        # Make sure we warn if szip with gzip compression option is used
        with self.assertRaises(ValueError):
            H5DataIO(np.arange(30), compression='szip', compression_opts=4)
        # Make sure szip raises a ValueError if bad options are used (odd compression option)
        with self.assertRaises(ValueError):
            H5DataIO(np.arange(30), compression='szip', compression_opts=('ec', 3))
        # Make sure szip raises a ValueError if bad options are used (bad methos)
        with self.assertRaises(ValueError):
            H5DataIO(np.arange(30), compression='szip', compression_opts=('bad_method', 16))

    def test_warning_on_linking_of_regular_array(self):
        with warnings.catch_warnings(record=True) as w:
            dset = H5DataIO(np.arange(30),
                            link_data=True)
            self.assertEqual(len(w), 1)
            self.assertEqual(dset.link_data, False)

    def test_warning_on_setting_io_options_on_h5dataset_input(self):
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', np.arange(10), attributes={}))
        with warnings.catch_warnings(record=True) as w:
            H5DataIO(self.f['test_dataset'],
                     compression='gzip',
                     compression_opts=4,
                     fletcher32=True,
                     shuffle=True,
                     maxshape=(10, 20),
                     chunks=(10,),
                     fillvalue=100)
            self.assertEqual(len(w), 7)

    def test_h5dataio_array_conversion_numpy(self):
        # Test that H5DataIO.__array__ is working when wrapping an ndarray
        test_speed = np.array([10., 20.])
        data = H5DataIO((test_speed))
        self.assertTrue(np.all(np.isfinite(data)))  # Force call of H5DataIO.__array__

    def test_h5dataio_array_conversion_list(self):
        # Test that H5DataIO.__array__ is working when wrapping a python list
        test_speed = [10., 20.]
        data = H5DataIO(test_speed)
        self.assertTrue(np.all(np.isfinite(data)))  # Force call of H5DataIO.__array__

    def test_h5dataio_array_conversion_datachunkiterator(self):
        # Test that H5DataIO.__array__ is working when wrapping a python list
        test_speed = DataChunkIterator(data=[10., 20.])
        data = H5DataIO(test_speed)
        with self.assertRaises(NotImplementedError):
            np.isfinite(data)  # Force call of H5DataIO.__array__

    #############################################
    #  Copy/Link h5py.Dataset object
    #############################################
    def test_link_h5py_dataset_input(self):
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', np.arange(10), attributes={}))
        self.io.write_dataset(self.f, DatasetBuilder('test_softlink', self.f['test_dataset'], attributes={}))
        self.assertTrue(isinstance(self.f.get('test_softlink', getlink=True), SoftLink))

    def test_copy_h5py_dataset_input(self):
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', np.arange(10), attributes={}))
        self.io.write_dataset(self.f,
                              DatasetBuilder('test_copy', self.f['test_dataset'], attributes={}),
                              link_data=False)
        self.assertTrue(isinstance(self.f.get('test_copy', getlink=True), HardLink))
        self.assertListEqual(self.f['test_dataset'][:].tolist(),
                             self.f['test_copy'][:].tolist())

    def test_link_h5py_dataset_h5dataio_input(self):
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', np.arange(10), attributes={}))
        self.io.write_dataset(self.f, DatasetBuilder('test_softlink',
                                                     H5DataIO(data=self.f['test_dataset'],
                                                              link_data=True),
                                                     attributes={}))
        self.assertTrue(isinstance(self.f.get('test_softlink', getlink=True), SoftLink))

    def test_copy_h5py_dataset_h5dataio_input(self):
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', np.arange(10), attributes={}))
        self.io.write_dataset(self.f,
                              DatasetBuilder('test_copy',
                                             H5DataIO(data=self.f['test_dataset'],
                                                      link_data=False),  # Force dataset copy
                                             attributes={}),
                              link_data=True)  # Make sure the default behavior is set to link the data
        self.assertTrue(isinstance(self.f.get('test_copy', getlink=True), HardLink))
        self.assertListEqual(self.f['test_dataset'][:].tolist(),
                             self.f['test_copy'][:].tolist())

    def test_list_fill_empty(self):
        dset = self.io.__list_fill__(self.f, 'empty_dataset', [], options={'dtype': int, 'io_settings': {}})
        self.assertTupleEqual(dset.shape, (0,))

    def test_list_fill_empty_no_dtype(self):
        with self.assertRaisesRegex(Exception, r"cannot add \S+ to [/\S]+ - could not determine type"):
            self.io.__list_fill__(self.f, 'empty_dataset', [])


def _get_manager():

    foo_spec = GroupSpec('A test group specification with a data type',
                         data_type_def='Foo',
                         datasets=[DatasetSpec('an example dataset',
                                               'int',
                                               name='my_data',
                                               attributes=[AttributeSpec('attr2',
                                                                         'an example integer attribute',
                                                                         'int')])],
                         attributes=[AttributeSpec('attr1', 'an example string attribute', 'text'),
                                     AttributeSpec('attr3', 'an example float attribute', 'float')])

    tmp_spec = GroupSpec('A subgroup for Foos',
                         name='foo_holder',
                         groups=[GroupSpec('the Foos in this bucket', data_type_inc='Foo', quantity=ZERO_OR_MANY)])

    bucket_spec = GroupSpec('A test group specification for a data type containing data type',
                            data_type_def='FooBucket',
                            groups=[tmp_spec])

    class FooMapper(ObjectMapper):
        def __init__(self, spec):
            super().__init__(spec)
            my_data_spec = spec.get_dataset('my_data')
            self.map_spec('attr2', my_data_spec.get_attribute('attr2'))

    class BucketMapper(ObjectMapper):
        def __init__(self, spec):
            super().__init__(spec)
            foo_holder_spec = spec.get_group('foo_holder')
            self.unmap(foo_holder_spec)
            foo_spec = foo_holder_spec.get_data_type('Foo')
            self.map_spec('foos', foo_spec)

    file_spec = GroupSpec("A file of Foos contained in FooBuckets",
                          data_type_def='FooFile',
                          groups=[GroupSpec('Holds the FooBuckets',
                                            name='buckets',
                                            groups=[GroupSpec("One or more FooBuckets",
                                                              data_type_inc='FooBucket',
                                                              quantity=ONE_OR_MANY)])])

    class FileMapper(ObjectMapper):
        def __init__(self, spec):
            super().__init__(spec)
            bucket_spec = spec.get_group('buckets').get_data_type('FooBucket')
            self.map_spec('buckets', bucket_spec)

    spec_catalog = SpecCatalog()
    spec_catalog.register_spec(foo_spec, 'test.yaml')
    spec_catalog.register_spec(bucket_spec, 'test.yaml')
    spec_catalog.register_spec(file_spec, 'test.yaml')
    namespace = SpecNamespace(
        'a test namespace',
        CORE_NAMESPACE,
        [{'source': 'test.yaml'}],
        version='0.1.0',
        catalog=spec_catalog)
    namespace_catalog = NamespaceCatalog()
    namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)
    type_map = TypeMap(namespace_catalog)

    type_map.register_container_type(CORE_NAMESPACE, 'Foo', Foo)
    type_map.register_container_type(CORE_NAMESPACE, 'FooBucket', FooBucket)
    type_map.register_container_type(CORE_NAMESPACE, 'FooFile', FooFile)

    type_map.register_map(Foo, FooMapper)
    type_map.register_map(FooBucket, BucketMapper)
    type_map.register_map(FooFile, FileMapper)

    manager = BuildManager(type_map)
    return manager


class TestRoundTrip(TestCase):

    def setUp(self):
        self.manager = _get_manager()
        self.path = get_temp_filepath()

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_roundtrip_basic(self):
        # Setup all the data we need
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('test_bucket', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            io.write(foofile)

        with HDF5IO(self.path, manager=self.manager, mode='r') as io:
            read_foofile = io.read()
            self.assertListEqual(foofile.buckets[0].foos[0].my_data,
                                 read_foofile.buckets[0].foos[0].my_data[:].tolist())

    def test_roundtrip_empty_dataset(self):
        foo1 = Foo('foo1', [], "I am foo1", 17, 3.14)
        foobucket = FooBucket('test_bucket', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            io.write(foofile)

        with HDF5IO(self.path, manager=self.manager, mode='r') as io:
            read_foofile = io.read()
            self.assertListEqual([], read_foofile.buckets[0].foos[0].my_data[:].tolist())

    def test_roundtrip_empty_group(self):
        foobucket = FooBucket('test_bucket', [])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            io.write(foofile)

        with HDF5IO(self.path, manager=self.manager, mode='r') as io:
            read_foofile = io.read()
            self.assertListEqual([], read_foofile.buckets[0].foos)


class TestHDF5IO(TestCase):

    def setUp(self):
        self.manager = _get_manager()
        self.path = get_temp_filepath()

        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('test_bucket', [foo1])
        self.foofile = FooFile([foobucket])

        self.file_obj = None

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

        if self.file_obj is not None:
            fn = self.file_obj.filename
            self.file_obj.close()
            if os.path.exists(fn):
                os.remove(fn)

    def test_constructor(self):
        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            self.assertEqual(io.manager, self.manager)
            self.assertEqual(io.source, self.path)

    def test_set_file_mismatch(self):
        self.file_obj = File(get_temp_filepath(), 'w')
        err_msg = ("You argued %s as this object's path, but supplied a file with filename: %s"
                   % (self.path, self.file_obj.filename))
        with self.assertRaisesWith(ValueError, err_msg):
            HDF5IO(self.path, manager=self.manager, mode='w', file=self.file_obj)


class TestCacheSpec(TestCase):

    def setUp(self):
        self.manager = _get_manager()
        self.path = get_temp_filepath()

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_cache_spec(self):
        foo1 = Foo('foo1', [0, 1, 2, 3, 4], "I am foo1", 17, 3.14)
        foo2 = Foo('foo2', [5, 6, 7, 8, 9], "I am foo2", 34, 6.28)
        foobucket = FooBucket('test_bucket', [foo1, foo2])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            io.write(foofile)

            ns_catalog = NamespaceCatalog()
            HDF5IO.load_namespaces(ns_catalog, self.path)
            self.assertEqual(ns_catalog.namespaces, (CORE_NAMESPACE,))
            source_types = self.__get_types(io.manager.namespace_catalog)
            read_types = self.__get_types(ns_catalog)
            self.assertSetEqual(source_types, read_types)

    def test_double_cache_spec(self):
        # Setup all the data we need
        foo1 = Foo('foo1', [0, 1, 2, 3, 4], "I am foo1", 17, 3.14)
        foo2 = Foo('foo2', [5, 6, 7, 8, 9], "I am foo2", 34, 6.28)
        foobucket = FooBucket('test_bucket', [foo1, foo2])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            io.write(foofile)

        with HDF5IO(self.path, manager=self.manager, mode='a') as io:
            io.write(foofile)

    def __get_types(self, catalog):
        types = set()
        for ns_name in catalog.namespaces:
            ns = catalog.get_namespace(ns_name)
            for source in ns['schema']:
                types.update(catalog.get_types(source['source']))
        return types


class TestNoCacheSpec(TestCase):

    def setUp(self):
        self.manager = _get_manager()
        self.path = get_temp_filepath()

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_no_cache_spec(self):
        # Setup all the data we need
        foo1 = Foo('foo1', [0, 1, 2, 3, 4], "I am foo1", 17, 3.14)
        foo2 = Foo('foo2', [5, 6, 7, 8, 9], "I am foo2", 34, 6.28)
        foobucket = FooBucket('test_bucket', [foo1, foo2])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            io.write(foofile, cache_spec=False)

        with File(self.path, 'r') as f:
            self.assertNotIn('specifications', f)


class HDF5IOMultiFileTest(TestCase):
    """Tests for h5tools IO tools"""

    def setUp(self):
        numfiles = 3
        base_name = "test_multifile_hdf5_%d.h5"
        self.test_temp_files = [base_name % i for i in range(numfiles)]

        # On Windows h5py cannot truncate an open file in write mode.
        # The temp file will be closed before h5py truncates it
        # and will be removed during the tearDown step.
        self.io = [HDF5IO(i, mode='a', manager=_get_manager()) for i in self.test_temp_files]
        self.f = [i._file for i in self.io]

    def tearDown(self):
        # Close all the files
        for i in self.io:
            i.close()
            del(i)
        self.io = None
        self.f = None
        # Make sure the files have been deleted
        for tf in self.test_temp_files:
            try:
                os.remove(tf)
            except OSError:
                pass
        self.test_temp_files = None

    def test_copy_file_with_external_links(self):
        # Create the first file
        foo1 = Foo('foo1', [0, 1, 2, 3, 4], "I am foo1", 17, 3.14)
        bucket1 = FooBucket('test_bucket1', [foo1])

        foofile1 = FooFile(buckets=[bucket1])

        # Write the first file
        self.io[0].write(foofile1)

        # Create the second file
        bucket1_read = self.io[0].read()
        foo2 = Foo('foo2', bucket1_read.buckets[0].foos[0].my_data, "I am foo2", 34, 6.28)
        bucket2 = FooBucket('test_bucket2', [foo2])
        foofile2 = FooFile(buckets=[bucket2])
        # Write the second file
        self.io[1].write(foofile2)
        self.io[1].close()
        self.io[0].close()  # Don't forget to close the first file too

        # Copy the file
        self.io[2].close()
        HDF5IO.copy_file(source_filename=self.test_temp_files[1],
                         dest_filename=self.test_temp_files[2],
                         expand_external=True,
                         expand_soft=False,
                         expand_refs=False)

        # Test that everything is working as expected
        # Confirm that our original data file is correct
        f1 = File(self.test_temp_files[0], 'r')
        self.assertIsInstance(f1.get('/buckets/test_bucket1/foo_holder/foo1/my_data', getlink=True), HardLink)
        # Confirm that we successfully created and External Link in our second file
        f2 = File(self.test_temp_files[1], 'r')
        self.assertIsInstance(f2.get('/buckets/test_bucket2/foo_holder/foo2/my_data', getlink=True), ExternalLink)
        # Confirm that we successfully resolved the External Link when we copied our second file
        f3 = File(self.test_temp_files[2], 'r')
        self.assertIsInstance(f3.get('/buckets/test_bucket2/foo_holder/foo2/my_data', getlink=True), HardLink)


class HDF5IOInitNoFileTest(TestCase):
    """ Test if file does not exist, init with mode (r, r+) throws error, all others succeed """

    def test_init_no_file_r(self):
        self.path = "test_init_nofile_r.h5"
        with self.assertRaisesWith(UnsupportedOperation,
                                   "Unable to open file %s in 'r' mode. File does not exist." % self.path):
            HDF5IO(self.path, mode='r')

    def test_init_no_file_rplus(self):
        self.path = "test_init_nofile_rplus.h5"
        with self.assertRaisesWith(UnsupportedOperation,
                                   "Unable to open file %s in 'r+' mode. File does not exist." % self.path):
            HDF5IO(self.path, mode='r+')

    def test_init_no_file_ok(self):
        # test that no errors are thrown
        modes = ('w', 'w-', 'x', 'a')
        for m in modes:
            self.path = "test_init_nofile.h5"
            with HDF5IO(self.path, mode=m):
                pass
            if os.path.exists(self.path):
                os.remove(self.path)


class HDF5IOInitFileExistsTest(TestCase):
    """ Test if file exists, init with mode w-/x throws error, all others succeed """

    def setUp(self):
        self.path = get_temp_filepath()
        temp_io = HDF5IO(self.path, mode='w')
        temp_io.close()
        self.io = None

    def tearDown(self):
        if self.io is not None:
            self.io.close()
            del(self.io)
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_init_wminus_file_exists(self):
        with self.assertRaisesWith(UnsupportedOperation,
                                   "Unable to open file %s in 'w-' mode. File already exists." % self.path):
            self.io = HDF5IO(self.path, mode='w-')

    def test_init_x_file_exists(self):
        with self.assertRaisesWith(UnsupportedOperation,
                                   "Unable to open file %s in 'x' mode. File already exists." % self.path):
            self.io = HDF5IO(self.path, mode='x')

    def test_init_file_exists_ok(self):
        # test that no errors are thrown
        modes = ('r', 'r+', 'w', 'a')
        for m in modes:
            with HDF5IO(self.path, mode=m):
                pass


class HDF5IOReadNoDataTest(TestCase):
    """ Test if file exists and there is no data, read with mode (r, r+, a) throws error """

    def setUp(self):
        self.path = get_temp_filepath()
        temp_io = HDF5IO(self.path, mode='w')
        temp_io.close()
        self.io = None

    def tearDown(self):
        if self.io is not None:
            self.io.close()
            del(self.io)

        if os.path.exists(self.path):
            os.remove(self.path)

    def test_read_no_data_r(self):
        self.io = HDF5IO(self.path, mode='r')
        with self.assertRaisesWith(UnsupportedOperation,
                                   "Cannot read data from file %s in mode 'r'. There are no values." % self.path):
            self.io.read()

    def test_read_no_data_rplus(self):
        self.io = HDF5IO(self.path, mode='r+')
        with self.assertRaisesWith(UnsupportedOperation,
                                   "Cannot read data from file %s in mode 'r+'. There are no values." % self.path):
            self.io.read()

    def test_read_no_data_a(self):
        self.io = HDF5IO(self.path, mode='a')
        with self.assertRaisesWith(UnsupportedOperation,
                                   "Cannot read data from file %s in mode 'a'. There are no values." % self.path):
            self.io.read()


class HDF5IOReadData(TestCase):
    """ Test if file exists and there is no data, read in mode (r, r+, a) is ok
    and read in mode w throws error
    """

    def setUp(self):
        self.path = get_temp_filepath()
        foo1 = Foo('foo1', [0, 1, 2, 3, 4], "I am foo1", 17, 3.14)
        bucket1 = FooBucket('test_bucket1', [foo1])
        self.foofile1 = FooFile(buckets=[bucket1])

        with HDF5IO(self.path, manager=_get_manager(), mode='w') as temp_io:
            temp_io.write(self.foofile1)
        self.io = None

    def tearDown(self):
        if self.io is not None:
            self.io.close()
            del(self.io)
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_read_file_ok(self):
        modes = ('r', 'r+', 'a')
        for m in modes:
            with HDF5IO(self.path, manager=_get_manager(), mode=m) as io:
                io.read()

    def test_read_file_w(self):
        with HDF5IO(self.path, manager=_get_manager(), mode='w') as io:
            with self.assertRaisesWith(UnsupportedOperation,
                                       "Cannot read from file %s in mode 'w'. Please use mode 'r', 'r+', or 'a'."
                                       % self.path):
                read_foofile1 = io.read()
                self.assertListEqual(self.foofile1.buckets[0].foos[0].my_data,
                                     read_foofile1.buckets[0].foos[0].my_data[:].tolist())


class HDF5IOWriteNoFile(TestCase):
    """ Test if file does not exist, write in mode (w, w-, x, a) is ok """

    def setUp(self):
        foo1 = Foo('foo1', [0, 1, 2, 3, 4], "I am foo1", 17, 3.14)
        bucket1 = FooBucket('test_bucket1', [foo1])
        self.foofile1 = FooFile(buckets=[bucket1])
        self.path = 'test_write_nofile.h5'

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_write_no_file_w_ok(self):
        self.__write_file('w')

    def test_write_no_file_wminus_ok(self):
        self.__write_file('w-')

    def test_write_no_file_x_ok(self):
        self.__write_file('x')

    def test_write_no_file_a_ok(self):
        self.__write_file('a')

    def __write_file(self, mode):
        with HDF5IO(self.path, manager=_get_manager(), mode=mode) as io:
            io.write(self.foofile1)

        with HDF5IO(self.path, manager=_get_manager(), mode='r') as io:
            read_foofile = io.read()
            self.assertListEqual(self.foofile1.buckets[0].foos[0].my_data,
                                 read_foofile.buckets[0].foos[0].my_data[:].tolist())


class HDF5IOWriteFileExists(TestCase):
    """ Test if file exists, write in mode (r+, w, a) is ok and write in mode r throws error """

    def setUp(self):
        self.path = get_temp_filepath()

        foo1 = Foo('foo1', [0, 1, 2, 3, 4], "I am foo1", 17, 3.14)
        bucket1 = FooBucket('test_bucket1', [foo1])
        self.foofile1 = FooFile(buckets=[bucket1])

        foo2 = Foo('foo2', [0, 1, 2, 3, 4], "I am foo2", 17, 3.14)
        bucket2 = FooBucket('test_bucket2', [foo2])
        self.foofile2 = FooFile(buckets=[bucket2])

        with HDF5IO(self.path, manager=_get_manager(), mode='w') as io:
            io.write(self.foofile1)
        self.io = None

    def tearDown(self):
        if self.io is not None:
            self.io.close()
            del(self.io)
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_write_rplus(self):
        with HDF5IO(self.path, manager=_get_manager(), mode='r+') as io:
            # even though foofile1 and foofile2 have different names, writing a
            # root object into a file that already has a root object, in r+ mode
            # should throw an error
            with self.assertRaisesWith(ValueError, "Unable to create group (name already exists)"):
                io.write(self.foofile2)

    def test_write_a(self):
        with HDF5IO(self.path, manager=_get_manager(), mode='a') as io:
            # even though foofile1 and foofile2 have different names, writing a
            # root object into a file that already has a root object, in r+ mode
            # should throw an error
            with self.assertRaisesWith(ValueError, "Unable to create group (name already exists)"):
                io.write(self.foofile2)

    def test_write_w(self):
        # mode 'w' should overwrite contents of file
        with HDF5IO(self.path, manager=_get_manager(), mode='w') as io:
            io.write(self.foofile2)

        with HDF5IO(self.path, manager=_get_manager(), mode='r') as io:
            read_foofile = io.read()
            self.assertListEqual(self.foofile2.buckets[0].foos[0].my_data,
                                 read_foofile.buckets[0].foos[0].my_data[:].tolist())

    def test_write_r(self):
        with HDF5IO(self.path, manager=_get_manager(), mode='r') as io:
            with self.assertRaisesWith(UnsupportedOperation,
                                       ("Cannot write to file %s in mode 'r'. "
                                        "Please use mode 'r+', 'w', 'w-', 'x', or 'a'") % self.path):
                io.write(self.foofile2)


class H5DataIOValid(TestCase):

    def setUp(self):
        self.paths = [get_temp_filepath(), ]

        self.foo1 = Foo('foo1', H5DataIO([1, 2, 3, 4, 5]), "I am foo1", 17, 3.14)
        bucket1 = FooBucket('test_bucket1', [self.foo1])
        foofile1 = FooFile(buckets=[bucket1])

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as io:
            io.write(foofile1)

    def tearDown(self):
        for path in self.paths:
            if os.path.exists(path):
                os.remove(path)

    def test_valid(self):
        self.assertTrue(self.foo1.my_data.valid)

    def test_read_valid(self):
        """Test that h5py.H5Dataset.id.valid works as expected"""
        with HDF5IO(self.paths[0], manager=_get_manager(), mode='r') as io:
            read_foofile1 = io.read()
            self.assertTrue(read_foofile1.buckets[0].foos[0].my_data.id.valid)

        self.assertFalse(read_foofile1.buckets[0].foos[0].my_data.id.valid)

    def test_link(self):
        """Test that wrapping of linked data within H5DataIO """
        with HDF5IO(self.paths[0], manager=_get_manager(), mode='r') as io:
            read_foofile1 = io.read()

            self.foo2 = Foo('foo2', H5DataIO(data=read_foofile1.buckets[0].foos[0].my_data), "I am foo2", 17, 3.14)
            bucket2 = FooBucket('test_bucket2', [self.foo2])
            foofile2 = FooFile(buckets=[bucket2])

            self.paths.append(get_temp_filepath())

            with HDF5IO(self.paths[1], manager=_get_manager(), mode='w') as io:
                io.write(foofile2)

            self.assertTrue(self.foo2.my_data.valid)  # test valid
            self.assertEqual(len(self.foo2.my_data), 5)  # test len
            self.assertEqual(self.foo2.my_data.shape, (5,))  # test getattr with shape
            self.assertTrue(np.array_equal(np.array(self.foo2.my_data), [1, 2, 3, 4, 5]))  # test array conversion

            # test loop through iterable
            match = [1, 2, 3, 4, 5]
            for (i, j) in zip(self.foo2.my_data, match):
                self.assertEqual(i, j)

            # test iterator
            my_iter = iter(self.foo2.my_data)
            self.assertEqual(next(my_iter), 1)

        # foo2.my_data dataset is now closed
        self.assertFalse(self.foo2.my_data.valid)

        with self.assertRaisesWith(InvalidDataIOError, "Cannot get length of data. Data is not valid."):
            len(self.foo2.my_data)

        with self.assertRaisesWith(InvalidDataIOError, "Cannot get attribute 'shape' of data. Data is not valid."):
            self.foo2.my_data.shape

        with self.assertRaisesWith(InvalidDataIOError, "Cannot convert data to array. Data is not valid."):
            np.array(self.foo2.my_data)

        with self.assertRaisesWith(InvalidDataIOError, "Cannot iterate on data. Data is not valid."):
            for i in self.foo2.my_data:
                pass

        with self.assertRaisesWith(InvalidDataIOError, "Cannot iterate on data. Data is not valid."):
            iter(self.foo2.my_data)

        # re-open the file with the data linking to other file (still closed)
        with HDF5IO(self.paths[1], manager=_get_manager(), mode='r') as io:
            read_foofile2 = io.read()
            read_foo2 = read_foofile2.buckets[0].foos[0]

            # note that read_foo2 dataset does not have an attribute 'valid'
            self.assertEqual(len(read_foo2.my_data), 5)  # test len
            self.assertEqual(read_foo2.my_data.shape, (5,))  # test getattr with shape
            self.assertTrue(np.array_equal(np.array(read_foo2.my_data), [1, 2, 3, 4, 5]))  # test array conversion

            # test loop through iterable
            match = [1, 2, 3, 4, 5]
            for (i, j) in zip(read_foo2.my_data, match):
                self.assertEqual(i, j)

            # test iterator
            my_iter = iter(read_foo2.my_data)
            self.assertEqual(next(my_iter), 1)


class TestReadLink(TestCase):
    def setUp(self):
        self.target_path = get_temp_filepath()
        self.link_path = get_temp_filepath()
        self.root1 = GroupBuilder(name='root')
        self.subgroup = self.root1.add_group('test_group')
        self.dataset = self.subgroup.add_dataset('test_dataset', data=[1, 2, 3, 4])

        self.root2 = GroupBuilder(name='root')
        self.group_link = self.root2.add_link(self.subgroup, 'link_to_test_group')
        self.dataset_link = self.root2.add_link(self.dataset, 'link_to_test_dataset')

        with HDF5IO(self.target_path, manager=_get_manager(), mode='w') as io:
            io.write_builder(self.root1)
        self.root1.source = self.target_path

        with HDF5IO(self.link_path, manager=_get_manager(), mode='w') as io:
            io.write_builder(self.root2)
        self.root2.source = self.link_path

    def test_set_link_loc(self):
        """
        Test that Builder location is set when it is read as a link
        """
        read_io = HDF5IO(self.link_path, manager=_get_manager(), mode='r')
        bldr = read_io.read_builder()
        self.assertEqual(bldr['link_to_test_group'].builder.location, '/')
        self.assertEqual(bldr['link_to_test_dataset'].builder.location, '/test_group')
        read_io.close()

    def test_link_to_link(self):
        """
        Test that link to link gets written and read properly
        """
        link_to_link_path = get_temp_filepath()
        read_io1 = HDF5IO(self.link_path, manager=_get_manager(), mode='r')
        bldr1 = read_io1.read_builder()
        root3 = GroupBuilder(name='root')
        root3.add_link(bldr1['link_to_test_group'].builder, 'link_to_link')
        with HDF5IO(link_to_link_path, manager=_get_manager(), mode='w') as io:
            io.write_builder(root3)
        read_io1.close()

        read_io2 = HDF5IO(link_to_link_path, manager=_get_manager(), mode='r')
        bldr2 = read_io2.read_builder()
        self.assertEqual(bldr2['link_to_link'].builder.source, self.target_path)
        read_io2.close()


class TestLoadNamespaces(TestCase):

    def setUp(self):
        self.manager = _get_manager()
        self.path = get_temp_filepath()

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_load_namespaces_none_version(self):
        """Test that reading a file with a cached namespace and None version works but raises a warning."""
        # Setup all the data we need
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('test_bucket', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            io.write(foofile)

        # make the file have group name "None" instead of "0.1.0" (namespace version is used as group name)
        # and set the version key to "None"
        with h5py.File(self.path, mode='r+') as f:
            # rename the group
            f.move('/specifications/' + CORE_NAMESPACE + '/0.1.0', '/specifications/' + CORE_NAMESPACE + '/None')

            # replace the namespace dataset with a serialized dict without the version key
            new_ns = ('{"namespaces":[{"doc":"a test namespace","schema":[{"source":"test"}],"name":"test_core",'
                      '"version":"None"}]}')
            f['/specifications/' + CORE_NAMESPACE + '/None/namespace'][()] = new_ns

        # load the namespace from file
        ns_catalog = NamespaceCatalog()
        msg = "Loaded namespace '%s' is unversioned. Please notify the extension author." % CORE_NAMESPACE
        with self.assertWarnsWith(UserWarning, msg):
            HDF5IO.load_namespaces(ns_catalog, self.path)

    def test_load_namespaces_unversioned(self):
        """Test that reading a file with a cached, unversioned version works but raises a warning."""
        # Setup all the data we need
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('test_bucket', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            io.write(foofile)

        # make the file have group name "unversioned" instead of "0.1.0" (namespace version is used as group name)
        # and remove the version key
        with h5py.File(self.path, mode='r+') as f:
            # rename the group
            f.move('/specifications/' + CORE_NAMESPACE + '/0.1.0', '/specifications/' + CORE_NAMESPACE + '/unversioned')

            # replace the namespace dataset with a serialized dict without the version key
            new_ns = ('{"namespaces":[{"doc":"a test namespace","schema":[{"source":"test"}],"name":"test_core"}]}')
            f['/specifications/' + CORE_NAMESPACE + '/unversioned/namespace'][()] = new_ns

        # load the namespace from file
        ns_catalog = NamespaceCatalog()
        msg = ("Loaded namespace '%s' is missing the required key 'version'. Version will be set to "
               "'%s'. Please notify the extension author." % (CORE_NAMESPACE, SpecNamespace.UNVERSIONED))
        with self.assertWarnsWith(UserWarning, msg):
            HDF5IO.load_namespaces(ns_catalog, self.path)

    def test_load_namespaces_path(self):
        """Test that loading namespaces given a path is OK and returns the correct dictionary."""

        # Setup all the data we need
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('test_bucket', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            io.write(foofile)

        ns_catalog = NamespaceCatalog()
        d = HDF5IO.load_namespaces(ns_catalog, self.path)
        self.assertEqual(d, {'test_core': {}})  # test_core has no dependencies

    def test_load_namespaces_no_path_no_file(self):
        """Test that loading namespaces without a path or file raises an error."""
        ns_catalog = NamespaceCatalog()

        msg = "Either the 'path' or 'file' argument must be supplied to load_namespaces."
        with self.assertRaisesWith(ValueError, msg):
            HDF5IO.load_namespaces(ns_catalog)

    def test_load_namespaces_file_no_path(self):
        """
        Test that loading namespaces from an h5py.File not backed by a file on disk is OK and does not close the file.
        """

        # Setup all the data we need
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('test_bucket', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            io.write(foofile)

        with open(self.path, 'rb') as raw_file:
            buffer = BytesIO(raw_file.read())
            file_obj = h5py.File(buffer, 'r')

        ns_catalog = NamespaceCatalog()
        d = HDF5IO.load_namespaces(ns_catalog, file=file_obj)

        self.assertTrue(file_obj.__bool__())  # check file object is still open
        self.assertEqual(d, {'test_core': {}})

        file_obj.close()

    def test_load_namespaces_file_path_matched(self):
        """Test that loading namespaces given an h5py.File and path is OK and does not close the file."""

        # Setup all the data we need
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('test_bucket', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            io.write(foofile)

        file_obj = h5py.File(self.path, 'r')

        ns_catalog = NamespaceCatalog()
        d = HDF5IO.load_namespaces(ns_catalog, path=self.path, file=file_obj)

        self.assertTrue(file_obj.__bool__())  # check file object is still open
        self.assertEqual(d, {'test_core': {}})

        file_obj.close()

    def test_load_namespaces_file_path_mismatched(self):
        """Test that loading namespaces given an h5py.File and path that are mismatched raises an error."""

        # Setup all the data we need
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('test_bucket', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            io.write(foofile)

        file_obj = h5py.File(self.path, 'r')

        ns_catalog = NamespaceCatalog()

        msg = "You argued 'different_path' as this object's path, but supplied a file with filename: %s" % self.path
        with self.assertRaisesWith(ValueError, msg):
            HDF5IO.load_namespaces(ns_catalog, path='different_path', file=file_obj)

        file_obj.close()
