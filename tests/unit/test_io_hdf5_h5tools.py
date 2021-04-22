import os
import unittest
import warnings
from io import BytesIO
from pathlib import Path

import h5py
import numpy as np
from h5py import SoftLink, HardLink, ExternalLink, File
from h5py import filters as h5py_filters
from hdmf.backends.hdf5 import H5DataIO
from hdmf.backends.hdf5.h5tools import HDF5IO, ROOT_NAME, SPEC_LOC_ATTR
from hdmf.backends.io import HDMFIO, UnsupportedOperation
from hdmf.backends.warnings import BrokenLinkWarning
from hdmf.build import (GroupBuilder, DatasetBuilder, BuildManager, TypeMap, ObjectMapper, OrphanContainerBuildError,
                        LinkBuilder)
from hdmf.container import Container, Data
from hdmf.data_utils import DataChunkIterator, InvalidDataIOError
from hdmf.spec.catalog import SpecCatalog
from hdmf.spec.namespace import NamespaceCatalog
from hdmf.spec.namespace import SpecNamespace
from hdmf.spec.spec import (AttributeSpec, DatasetSpec, GroupSpec, LinkSpec, ZERO_OR_MANY, ONE_OR_MANY, ZERO_OR_ONE,
                            RefSpec, DtypeSpec)
from hdmf.testing import TestCase
from hdmf.utils import docval, getargs

from tests.unit.utils import (Foo, FooBucket, CORE_NAMESPACE, get_temp_filepath, CustomGroupSpec, CustomDatasetSpec,
                              CustomSpecNamespace)


class FooFile(Container):

    @docval({'name': 'buckets', 'type': list, 'doc': 'the FooBuckets in this file', 'default': list()},
            {'name': 'foo_link', 'type': Foo, 'doc': 'an optional linked Foo', 'default': None},
            {'name': 'foofile_data', 'type': 'array_data', 'doc': 'an optional dataset', 'default': None},
            {'name': 'foo_ref_attr', 'type': Foo, 'doc': 'a reference Foo', 'default': None},
            )
    def __init__(self, **kwargs):
        buckets, foo_link, foofile_data, foo_ref_attr = getargs('buckets', 'foo_link', 'foofile_data',
                                                                'foo_ref_attr', kwargs)
        super().__init__(name=ROOT_NAME)  # name is not used - FooFile should be the root container
        self.__buckets = {b.name: b for b in buckets}  # note: collections of groups are unordered in HDF5
        for f in buckets:
            f.parent = self
        self.__foo_link = foo_link
        self.__foofile_data = foofile_data
        self.__foo_ref_attr = foo_ref_attr

    def __eq__(self, other):
        return (self.buckets == other.buckets
                and self.foo_link == other.foo_link
                and self.foofile_data == other.foofile_data)

    def __str__(self):
        return ('buckets=%s, foo_link=%s, foofile_data=%s' % (self.buckets, self.foo_link, self.foofile_data))

    @property
    def buckets(self):
        return self.__buckets

    def add_bucket(self, bucket):
        self.__buckets[bucket.name] = bucket
        bucket.parent = self

    def remove_bucket(self, bucket_name):
        bucket = self.__buckets.pop(bucket_name)
        if bucket.parent is self:
            self._remove_child(bucket)
        return bucket

    @property
    def foo_link(self):
        return self.__foo_link

    @foo_link.setter
    def foo_link(self, value):
        if self.__foo_link is None:
            self.__foo_link = value
        else:
            raise ValueError("can't reset foo_link attribute")

    @property
    def foofile_data(self):
        return self.__foofile_data

    @foofile_data.setter
    def foofile_data(self, value):
        if self.__foofile_data is None:
            self.__foofile_data = value
        else:
            raise ValueError("can't reset foofile_data attribute")

    @property
    def foo_ref_attr(self):
        return self.__foo_ref_attr

    @foo_ref_attr.setter
    def foo_ref_attr(self, value):
        if self.__foo_ref_attr is None:
            self.__foo_ref_attr = value
        else:
            raise ValueError("can't reset foo_ref_attr attribute")


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
        read_a = dset[()]
        if isinstance(read_a, bytes):
            read_a = read_a.decode('utf-8')
        self.assertEqual(read_a, a)

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
        dataset1 = DatasetBuilder('test_dataset1', daiter1)
        dataset2 = DatasetBuilder('test_dataset2', daiter2)
        builder.set_dataset(dataset1)
        builder.set_dataset(dataset2)
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
        dataset1 = DatasetBuilder('test_dataset1', daiter1)
        dataset2 = DatasetBuilder('test_dataset2', daiter2)
        builder.set_dataset(dataset1)
        builder.set_dataset(dataset2)
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
                                             attributes={}))  # Make sure the default behavior is set to link the data
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

    file_links_spec = GroupSpec('Foo link group',
                                name='links',
                                links=[LinkSpec('Foo link',
                                                name='foo_link',
                                                target_type='Foo',
                                                quantity=ZERO_OR_ONE)]
                                )

    file_spec = GroupSpec("A file of Foos contained in FooBuckets",
                          data_type_def='FooFile',
                          groups=[GroupSpec('Holds the FooBuckets',
                                            name='buckets',
                                            groups=[GroupSpec("One or more FooBuckets",
                                                              data_type_inc='FooBucket',
                                                              quantity=ZERO_OR_MANY)]),
                                  file_links_spec],
                          datasets=[DatasetSpec('Foo data',
                                                name='foofile_data',
                                                dtype='int',
                                                quantity=ZERO_OR_ONE)],
                          attributes=[AttributeSpec(doc='Foo ref attr',
                                                    name='foo_ref_attr',
                                                    dtype=RefSpec('Foo', 'object'),
                                                    required=False)],
                          )

    class FileMapper(ObjectMapper):
        def __init__(self, spec):
            super().__init__(spec)
            bucket_spec = spec.get_group('buckets').get_data_type('FooBucket')
            self.map_spec('buckets', bucket_spec)
            self.unmap(spec.get_group('links'))
            foo_link_spec = spec.get_group('links').get_link('foo_link')
            self.map_spec('foo_link', foo_link_spec)

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
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            io.write(foofile)

        with HDF5IO(self.path, manager=self.manager, mode='r') as io:
            read_foofile = io.read()
            self.assertListEqual(foofile.buckets['bucket1'].foos['foo1'].my_data,
                                 read_foofile.buckets['bucket1'].foos['foo1'].my_data[:].tolist())

    def test_roundtrip_empty_dataset(self):
        foo1 = Foo('foo1', [], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            io.write(foofile)

        with HDF5IO(self.path, manager=self.manager, mode='r') as io:
            read_foofile = io.read()
            self.assertListEqual([], read_foofile.buckets['bucket1'].foos['foo1'].my_data[:].tolist())

    def test_roundtrip_empty_group(self):
        foobucket = FooBucket('bucket1', [])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            io.write(foofile)

        with HDF5IO(self.path, manager=self.manager, mode='r') as io:
            read_foofile = io.read()
            self.assertDictEqual({}, read_foofile.buckets['bucket1'].foos)

    def test_roundtrip_pathlib_path(self):
        pathlib_path = Path(self.path)
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(pathlib_path, manager=self.manager, mode='w') as io:
            io.write(foofile)

        with HDF5IO(pathlib_path, manager=self.manager, mode='r') as io:
            read_foofile = io.read()
            self.assertListEqual(foofile.buckets['bucket1'].foos['foo1'].my_data,
                                 read_foofile.buckets['bucket1'].foos['foo1'].my_data[:].tolist())


class TestHDF5IO(TestCase):

    def setUp(self):
        self.manager = _get_manager()
        self.path = get_temp_filepath()

        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
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

    def test_pathlib_path(self):
        pathlib_path = Path(self.path)
        with HDF5IO(pathlib_path, mode='w') as io:
            self.assertEqual(io.source, self.path)


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
        foobucket = FooBucket('bucket1', [foo1, foo2])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            io.write(foofile)

            ns_catalog = NamespaceCatalog()
            HDF5IO.load_namespaces(ns_catalog, self.path)
            self.assertEqual(ns_catalog.namespaces, (CORE_NAMESPACE,))
            source_types = self.__get_types(io.manager.namespace_catalog)
            read_types = self.__get_types(ns_catalog)
            self.assertSetEqual(source_types, read_types)

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
        foobucket = FooBucket('bucket1', [foo1, foo2])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            io.write(foofile, cache_spec=False)

        with File(self.path, 'r') as f:
            self.assertNotIn('specifications', f)


class TestMultiWrite(TestCase):

    def setUp(self):
        self.path = get_temp_filepath()
        foo1 = Foo('foo1', [0, 1, 2, 3, 4], "I am foo1", 17, 3.14)
        foo2 = Foo('foo2', [5, 6, 7, 8, 9], "I am foo2", 34, 6.28)
        foobucket = FooBucket('bucket1', [foo1, foo2])
        self.foofile = FooFile([foobucket])

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_double_write_new_manager(self):
        """Test writing to a container in write mode twice using a new manager without changing the container."""
        with HDF5IO(self.path, manager=_get_manager(), mode='w') as io:
            io.write(self.foofile)

        with HDF5IO(self.path, manager=_get_manager(), mode='w') as io:
            io.write(self.foofile)

        # check that new bucket was written
        with HDF5IO(self.path, manager=_get_manager(), mode='r') as io:
            read_foofile = io.read()
            self.assertContainerEqual(read_foofile, self.foofile)

    def test_double_write_same_manager(self):
        """Test writing to a container in write mode twice using the same manager without changing the container."""
        manager = _get_manager()
        with HDF5IO(self.path, manager=manager, mode='w') as io:
            io.write(self.foofile)

        with HDF5IO(self.path, manager=manager, mode='w') as io:
            io.write(self.foofile)

        # check that new bucket was written
        with HDF5IO(self.path, manager=_get_manager(), mode='r') as io:
            read_foofile = io.read()
            self.assertContainerEqual(read_foofile, self.foofile)

    @unittest.skip('Functionality not yet supported')
    def test_double_append_new_manager(self):
        """Test writing to a container in append mode twice using a new manager without changing the container."""
        with HDF5IO(self.path, manager=_get_manager(), mode='a') as io:
            io.write(self.foofile)

        with HDF5IO(self.path, manager=_get_manager(), mode='a') as io:
            io.write(self.foofile)

        # check that new bucket was written
        with HDF5IO(self.path, manager=_get_manager(), mode='r') as io:
            read_foofile = io.read()
            self.assertContainerEqual(read_foofile, self.foofile)

    @unittest.skip('Functionality not yet supported')
    def test_double_append_same_manager(self):
        """Test writing to a container in append mode twice using the same manager without changing the container."""
        manager = _get_manager()
        with HDF5IO(self.path, manager=manager, mode='a') as io:
            io.write(self.foofile)

        with HDF5IO(self.path, manager=manager, mode='a') as io:
            io.write(self.foofile)

        # check that new bucket was written
        with HDF5IO(self.path, manager=_get_manager(), mode='r') as io:
            read_foofile = io.read()
            self.assertContainerEqual(read_foofile, self.foofile)

    def test_write_add_write(self):
        """Test writing a container, adding to the in-memory container, then overwriting the same file."""
        manager = _get_manager()
        with HDF5IO(self.path, manager=manager, mode='w') as io:
            io.write(self.foofile)

        # append new container to in-memory container
        foo3 = Foo('foo3', [10, 20], "I am foo3", 2, 0.1)
        new_bucket1 = FooBucket('new_bucket1', [foo3])
        self.foofile.add_bucket(new_bucket1)

        # write to same file with same manager, overwriting existing file
        with HDF5IO(self.path, manager=manager, mode='w') as io:
            io.write(self.foofile)

        # check that new bucket was written
        with HDF5IO(self.path, manager=_get_manager(), mode='r') as io:
            read_foofile = io.read()
            self.assertEqual(len(read_foofile.buckets), 2)
            self.assertContainerEqual(read_foofile.buckets['new_bucket1'], new_bucket1)

    def test_write_add_append_bucket(self):
        """Test appending a container to a file."""
        manager = _get_manager()
        with HDF5IO(self.path, manager=manager, mode='w') as io:
            io.write(self.foofile)

        foo3 = Foo('foo3', [10, 20], "I am foo3", 2, 0.1)
        new_bucket1 = FooBucket('new_bucket1', [foo3])

        # append to same file with same manager, overwriting existing file
        with HDF5IO(self.path, manager=manager, mode='a') as io:
            read_foofile = io.read()
            # append to read container and call write
            read_foofile.add_bucket(new_bucket1)
            io.write(read_foofile)

        # check that new bucket was written
        with HDF5IO(self.path, manager=_get_manager(), mode='r') as io:
            read_foofile = io.read()
            self.assertEqual(len(read_foofile.buckets), 2)
            self.assertContainerEqual(read_foofile.buckets['new_bucket1'], new_bucket1)

    def test_write_add_append_double_write(self):
        """Test using the same IO object to append a container to a file twice."""
        manager = _get_manager()
        with HDF5IO(self.path, manager=manager, mode='w') as io:
            io.write(self.foofile)

        foo3 = Foo('foo3', [10, 20], "I am foo3", 2, 0.1)
        new_bucket1 = FooBucket('new_bucket1', [foo3])
        foo4 = Foo('foo4', [10, 20], "I am foo4", 2, 0.1)
        new_bucket2 = FooBucket('new_bucket2', [foo4])

        # append to same file with same manager, overwriting existing file
        with HDF5IO(self.path, manager=manager, mode='a') as io:
            read_foofile = io.read()
            # append to read container and call write
            read_foofile.add_bucket(new_bucket1)
            io.write(read_foofile)

            # append to read container again and call write again
            read_foofile.add_bucket(new_bucket2)
            io.write(read_foofile)

        # check that both new buckets were written
        with HDF5IO(self.path, manager=_get_manager(), mode='r') as io:
            read_foofile = io.read()
            self.assertEqual(len(read_foofile.buckets), 3)
            self.assertContainerEqual(read_foofile.buckets['new_bucket1'], new_bucket1)
            self.assertContainerEqual(read_foofile.buckets['new_bucket2'], new_bucket2)


class HDF5IOMultiFileTest(TestCase):
    """Tests for h5tools IO tools"""

    def setUp(self):
        numfiles = 3
        self.paths = [get_temp_filepath() for i in range(numfiles)]

        # On Windows h5py cannot truncate an open file in write mode.
        # The temp file will be closed before h5py truncates it
        # and will be removed during the tearDown step.
        self.io = [HDF5IO(i, mode='a', manager=_get_manager()) for i in self.paths]
        self.f = [i._file for i in self.io]

    def tearDown(self):
        # Close all the files
        for i in self.io:
            i.close()
            del(i)
        self.io = None
        self.f = None
        # Make sure the files have been deleted
        for tf in self.paths:
            try:
                os.remove(tf)
            except OSError:
                pass

    def test_copy_file_with_external_links(self):
        # Create the first file
        foo1 = Foo('foo1', [0, 1, 2, 3, 4], "I am foo1", 17, 3.14)
        bucket1 = FooBucket('bucket1', [foo1])
        foofile1 = FooFile(buckets=[bucket1])

        # Write the first file
        self.io[0].write(foofile1)

        # Create the second file
        read_foofile1 = self.io[0].read()
        foo2 = Foo('foo2', read_foofile1.buckets['bucket1'].foos['foo1'].my_data, "I am foo2", 34, 6.28)
        bucket2 = FooBucket('bucket2', [foo2])
        foofile2 = FooFile(buckets=[bucket2])
        # Write the second file
        self.io[1].write(foofile2)
        self.io[1].close()
        self.io[0].close()  # Don't forget to close the first file too

        # Copy the file
        self.io[2].close()

        with self.assertWarns(DeprecationWarning):
            HDF5IO.copy_file(source_filename=self.paths[1],
                             dest_filename=self.paths[2],
                             expand_external=True,
                             expand_soft=False,
                             expand_refs=False)

        # Test that everything is working as expected
        # Confirm that our original data file is correct
        f1 = File(self.paths[0], 'r')
        self.assertIsInstance(f1.get('/buckets/bucket1/foo_holder/foo1/my_data', getlink=True), HardLink)
        # Confirm that we successfully created and External Link in our second file
        f2 = File(self.paths[1], 'r')
        self.assertIsInstance(f2.get('/buckets/bucket2/foo_holder/foo2/my_data', getlink=True), ExternalLink)
        # Confirm that we successfully resolved the External Link when we copied our second file
        f3 = File(self.paths[2], 'r')
        self.assertIsInstance(f3.get('/buckets/bucket2/foo_holder/foo2/my_data', getlink=True), HardLink)


class TestCloseLinks(TestCase):

    def setUp(self):
        self.path1 = get_temp_filepath()
        self.path2 = get_temp_filepath()

    def tearDown(self):
        if self.path1 is not None:
            os.remove(self.path1)  # linked file may not be closed
        if self.path2 is not None:
            os.remove(self.path2)

    def test_close_file_with_links(self):
        # Create the first file
        foo1 = Foo('foo1', [0, 1, 2, 3, 4], "I am foo1", 17, 3.14)
        bucket1 = FooBucket('bucket1', [foo1])
        foofile1 = FooFile(buckets=[bucket1])

        # Write the first file
        with HDF5IO(self.path1, mode='w', manager=_get_manager()) as io:
            io.write(foofile1)

        # Create the second file
        manager = _get_manager()  # use the same manager for read and write so that links work
        with HDF5IO(self.path1, mode='r', manager=manager) as read_io:
            read_foofile1 = read_io.read()
            foofile2 = FooFile(foo_link=read_foofile1.buckets['bucket1'].foos['foo1'])  # cross-file link

            # Write the second file
            with HDF5IO(self.path2, mode='w', manager=manager) as write_io:
                write_io.write(foofile2)

        with HDF5IO(self.path2, mode='a', manager=_get_manager()) as new_io1:
            read_foofile2 = new_io1.read()  # keep reference to container in memory

        self.assertTrue(read_foofile2.foo_link.my_data)
        new_io1.close_linked_files()
        self.assertFalse(read_foofile2.foo_link.my_data)

        # should be able to reopen both files
        with HDF5IO(self.path1, mode='a', manager=_get_manager()) as new_io3:
            new_io3.read()

    def test_double_close_file_with_links(self):
        # Create the first file
        foo1 = Foo('foo1', [0, 1, 2, 3, 4], "I am foo1", 17, 3.14)
        bucket1 = FooBucket('bucket1', [foo1])
        foofile1 = FooFile(buckets=[bucket1])

        # Write the first file
        with HDF5IO(self.path1, mode='w', manager=_get_manager()) as io:
            io.write(foofile1)

        # Create the second file
        manager = _get_manager()  # use the same manager for read and write so that links work
        with HDF5IO(self.path1, mode='r', manager=manager) as read_io:
            read_foofile1 = read_io.read()
            foofile2 = FooFile(foo_link=read_foofile1.buckets['bucket1'].foos['foo1'])  # cross-file link

            # Write the second file
            with HDF5IO(self.path2, mode='w', manager=manager) as write_io:
                write_io.write(foofile2)

        with HDF5IO(self.path2, mode='a', manager=_get_manager()) as new_io1:
            read_foofile2 = new_io1.read()  # keep reference to container in memory

        read_foofile2.foo_link.my_data.file.close()  # explicitly close the file from the h5dataset
        self.assertFalse(read_foofile2.foo_link.my_data)
        new_io1.close_linked_files()  # make sure this does not fail because the linked-to file is already closed


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
        bucket1 = FooBucket('bucket1', [foo1])
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
                self.assertListEqual(self.foofile1.buckets['bucket1'].foos['foo1'].my_data,
                                     read_foofile1.buckets['bucket1'].foos['foo1'].my_data[:].tolist())


class HDF5IOReadBuilderClosed(TestCase):
    """Test if file exists but is closed, then read_builder raises an error. """

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

    def test_read_closed(self):
        self.io = HDF5IO(self.path, mode='r')
        self.io.close()
        msg = "Cannot read data from closed HDF5 file '%s'" % self.path
        with self.assertRaisesWith(UnsupportedOperation, msg):
            self.io.read_builder()


class HDF5IOWriteNoFile(TestCase):
    """ Test if file does not exist, write in mode (w, w-, x, a) is ok """

    def setUp(self):
        foo1 = Foo('foo1', [0, 1, 2, 3, 4], "I am foo1", 17, 3.14)
        bucket1 = FooBucket('bucket1', [foo1])
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
            self.assertListEqual(self.foofile1.buckets['bucket1'].foos['foo1'].my_data,
                                 read_foofile.buckets['bucket1'].foos['foo1'].my_data[:].tolist())


class HDF5IOWriteFileExists(TestCase):
    """ Test if file exists, write in mode (r+, w, a) is ok and write in mode r throws error """

    def setUp(self):
        self.path = get_temp_filepath()

        foo1 = Foo('foo1', [0, 1, 2, 3, 4], "I am foo1", 17, 3.14)
        bucket1 = FooBucket('bucket1', [foo1])
        self.foofile1 = FooFile(buckets=[bucket1])

        foo2 = Foo('foo2', [0, 1, 2, 3, 4], "I am foo2", 17, 3.14)
        bucket2 = FooBucket('bucket2', [foo2])
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
            # root object into a file that already has a root object, in a mode
            # should throw an error
            with self.assertRaisesWith(ValueError, "Unable to create group (name already exists)"):
                io.write(self.foofile2)

    def test_write_w(self):
        # mode 'w' should overwrite contents of file
        with HDF5IO(self.path, manager=_get_manager(), mode='w') as io:
            io.write(self.foofile2)

        with HDF5IO(self.path, manager=_get_manager(), mode='r') as io:
            read_foofile = io.read()
            self.assertListEqual(self.foofile2.buckets['bucket2'].foos['foo2'].my_data,
                                 read_foofile.buckets['bucket2'].foos['foo2'].my_data[:].tolist())

    def test_write_r(self):
        with HDF5IO(self.path, manager=_get_manager(), mode='r') as io:
            with self.assertRaisesWith(UnsupportedOperation,
                                       ("Cannot write to file %s in mode 'r'. "
                                        "Please use mode 'r+', 'w', 'w-', 'x', or 'a'") % self.path):
                io.write(self.foofile2)


class TestWritten(TestCase):

    def setUp(self):
        self.manager = _get_manager()
        self.path = get_temp_filepath()
        foo1 = Foo('foo1', [0, 1, 2, 3, 4], "I am foo1", 17, 3.14)
        foo2 = Foo('foo2', [5, 6, 7, 8, 9], "I am foo2", 34, 6.28)
        foobucket = FooBucket('bucket1', [foo1, foo2])
        self.foofile = FooFile([foobucket])

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_set_written_on_write(self):
        """Test that write_builder changes the written flag of the builder and its children from False to True."""
        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            builder = self.manager.build(container=self.foofile, source=self.path)
            self.assertFalse(io.get_written(builder))
            self._check_written_children(io, builder, False)
            io.write_builder(builder)
            self.assertTrue(io.get_written(builder))
            self._check_written_children(io, builder, True)

    def _check_written_children(self, io, builder, val):
        """Test whether the io object has the written flag of the child builders set to val."""
        for group_bldr in builder.groups.values():
            self.assertEqual(io.get_written(group_bldr), val)
            self._check_written_children(io, group_bldr, val)
        for dset_bldr in builder.datasets.values():
            self.assertEqual(io.get_written(dset_bldr), val)
        for link_bldr in builder.links.values():
            self.assertEqual(io.get_written(link_bldr), val)


class H5DataIOValid(TestCase):

    def setUp(self):
        self.paths = [get_temp_filepath(), ]

        self.foo1 = Foo('foo1', H5DataIO([1, 2, 3, 4, 5]), "I am foo1", 17, 3.14)
        bucket1 = FooBucket('bucket1', [self.foo1])
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
            self.assertTrue(read_foofile1.buckets['bucket1'].foos['foo1'].my_data.id.valid)

        self.assertFalse(read_foofile1.buckets['bucket1'].foos['foo1'].my_data.id.valid)

    def test_link(self):
        """Test that wrapping of linked data within H5DataIO """
        with HDF5IO(self.paths[0], manager=_get_manager(), mode='r') as io:
            read_foofile1 = io.read()

            self.foo2 = Foo('foo2', H5DataIO(data=read_foofile1.buckets['bucket1'].foos['foo1'].my_data),
                            "I am foo2", 17, 3.14)
            bucket2 = FooBucket('bucket2', [self.foo2])
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
            read_foo2 = read_foofile2.buckets['bucket2'].foos['foo2']

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
        root1 = GroupBuilder(name='root')
        subgroup = GroupBuilder(name='test_group')
        root1.set_group(subgroup)
        dataset = DatasetBuilder('test_dataset', data=[1, 2, 3, 4])
        subgroup.set_dataset(dataset)

        root2 = GroupBuilder(name='root')
        link_group = LinkBuilder(subgroup, 'link_to_test_group')
        root2.set_link(link_group)
        link_dataset = LinkBuilder(dataset, 'link_to_test_dataset')
        root2.set_link(link_dataset)

        with HDF5IO(self.target_path, manager=_get_manager(), mode='w') as io:
            io.write_builder(root1)
        root1.source = self.target_path

        with HDF5IO(self.link_path, manager=_get_manager(), mode='w') as io:
            io.write_builder(root2)
        root2.source = self.link_path

        self.ios = []

    def tearDown(self):
        for io in self.ios:
            io.close_linked_files()
        if os.path.exists(self.target_path):
            os.remove(self.target_path)
        if os.path.exists(self.link_path):
            os.remove(self.link_path)

    def test_set_link_loc(self):
        """
        Test that Builder location is set when it is read as a link
        """
        read_io = HDF5IO(self.link_path, manager=_get_manager(), mode='r')
        self.ios.append(read_io)  # store IO object for closing in tearDown
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
        self.ios.append(read_io1)  # store IO object for closing in tearDown
        bldr1 = read_io1.read_builder()
        root3 = GroupBuilder(name='root')
        link = LinkBuilder(bldr1['link_to_test_group'].builder, 'link_to_link')
        root3.set_link(link)
        with HDF5IO(link_to_link_path, manager=_get_manager(), mode='w') as io:
            io.write_builder(root3)
        read_io1.close()

        read_io2 = HDF5IO(link_to_link_path, manager=_get_manager(), mode='r')
        self.ios.append(read_io2)
        bldr2 = read_io2.read_builder()
        self.assertEqual(bldr2['link_to_link'].builder.source, self.target_path)
        read_io2.close()

    def test_broken_link(self):
        """Test that opening a file with a broken link raises a warning but is still readable."""
        os.remove(self.target_path)
        # with self.assertWarnsWith(BrokenLinkWarning, '/link_to_test_dataset'):  # can't check both warnings
        with self.assertWarnsWith(BrokenLinkWarning, '/link_to_test_group'):
            with HDF5IO(self.link_path, manager=_get_manager(), mode='r') as read_io:
                bldr = read_io.read_builder()
                self.assertDictEqual(bldr.links, {})

    def test_broken_linked_data(self):
        """Test that opening a file with a broken link raises a warning but is still readable."""
        manager = _get_manager()

        with HDF5IO(self.target_path, manager=manager, mode='r') as read_io:
            read_root = read_io.read_builder()
            read_dataset_data = read_root.groups['test_group'].datasets['test_dataset'].data

            with HDF5IO(self.link_path, manager=manager, mode='w') as write_io:
                root2 = GroupBuilder(name='root')
                dataset = DatasetBuilder(name='link_to_test_dataset', data=read_dataset_data)
                root2.set_dataset(dataset)
                write_io.write_builder(root2, link_data=True)

        os.remove(self.target_path)
        with self.assertWarnsWith(BrokenLinkWarning, '/link_to_test_dataset'):
            with HDF5IO(self.link_path, manager=_get_manager(), mode='r') as read_io:
                bldr = read_io.read_builder()
                self.assertDictEqual(bldr.links, {})


class TestBuildWriteLinkToLink(TestCase):

    def setUp(self):
        self.paths = [
            get_temp_filepath(),
            get_temp_filepath(),
            get_temp_filepath()
        ]
        self.ios = []

    def tearDown(self):
        for io in self.ios:
            io.close_linked_files()
        for p in self.paths:
            if os.path.exists(p):
                os.remove(p)

    def test_external_link_to_external_link(self):
        """Test writing a file with external links to external links."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        manager = _get_manager()
        with HDF5IO(self.paths[0], manager=manager, mode='r') as read_io:
            read_foofile = read_io.read()
            # make external link to existing group
            foofile2 = FooFile(foo_link=read_foofile.buckets['bucket1'].foos['foo1'])

            with HDF5IO(self.paths[1], manager=manager, mode='w') as write_io:
                write_io.write(foofile2)

        manager = _get_manager()
        with HDF5IO(self.paths[1], manager=manager, mode='r') as read_io:
            self.ios.append(read_io)  # track IO objects for tearDown
            read_foofile2 = read_io.read()
            foofile3 = FooFile(foo_link=read_foofile2.foo_link)  # make external link to external link

            with HDF5IO(self.paths[2], manager=manager, mode='w') as write_io:
                write_io.write(foofile3)

        with HDF5IO(self.paths[2], manager=_get_manager(), mode='r') as read_io:
            self.ios.append(read_io)  # track IO objects for tearDown
            read_foofile3 = read_io.read()

            self.assertEqual(read_foofile3.foo_link.container_source, self.paths[0])

    def test_external_link_to_soft_link(self):
        """Test writing a file with external links to external links."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket], foo_link=foo1)  # create soft link

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        manager = _get_manager()
        with HDF5IO(self.paths[0], manager=manager, mode='r') as read_io:
            read_foofile = read_io.read()
            foofile2 = FooFile(foo_link=read_foofile.foo_link)  # make external link to existing soft link

            with HDF5IO(self.paths[1], manager=manager, mode='w') as write_io:
                write_io.write(foofile2)

        manager = _get_manager()
        with HDF5IO(self.paths[1], manager=manager, mode='r') as read_io:
            self.ios.append(read_io)  # track IO objects for tearDown
            read_foofile2 = read_io.read()
            foofile3 = FooFile(foo_link=read_foofile2.foo_link)  # make external link to external link

            with HDF5IO(self.paths[2], manager=manager, mode='w') as write_io:
                write_io.write(foofile3)

        with HDF5IO(self.paths[2], manager=_get_manager(), mode='r') as read_io:
            self.ios.append(read_io)  # track IO objects for tearDown
            read_foofile3 = read_io.read()

            self.assertEqual(read_foofile3.foo_link.container_source, self.paths[0])


class TestLinkData(TestCase):

    def setUp(self):
        self.target_path = get_temp_filepath()
        self.link_path = get_temp_filepath()
        root1 = GroupBuilder(name='root')
        subgroup = GroupBuilder(name='test_group')
        root1.set_group(subgroup)
        dataset = DatasetBuilder('test_dataset', data=[1, 2, 3, 4])
        subgroup.set_dataset(dataset)

        with HDF5IO(self.target_path, manager=_get_manager(), mode='w') as io:
            io.write_builder(root1)

    def tearDown(self):
        if os.path.exists(self.target_path):
            os.remove(self.target_path)
        if os.path.exists(self.link_path):
            os.remove(self.link_path)

    def test_link_data_true(self):
        """Test that the argument link_data=True for write_builder creates an external link."""
        manager = _get_manager()
        with HDF5IO(self.target_path, manager=manager, mode='r') as read_io:
            read_root = read_io.read_builder()
            read_dataset_data = read_root.groups['test_group'].datasets['test_dataset'].data

            with HDF5IO(self.link_path, manager=manager, mode='w') as write_io:
                root2 = GroupBuilder(name='root')
                dataset = DatasetBuilder(name='link_to_test_dataset', data=read_dataset_data)
                root2.set_dataset(dataset)
                write_io.write_builder(root2, link_data=True)

        with File(self.link_path, mode='r') as f:
            self.assertIsInstance(f.get('link_to_test_dataset', getlink=True), ExternalLink)

    def test_link_data_false(self):
        """Test that the argument link_data=False for write_builder copies the data."""
        manager = _get_manager()
        with HDF5IO(self.target_path, manager=manager, mode='r') as read_io:
            read_root = read_io.read_builder()
            read_dataset_data = read_root.groups['test_group'].datasets['test_dataset'].data

            with HDF5IO(self.link_path, manager=manager, mode='w') as write_io:
                root2 = GroupBuilder(name='root')
                dataset = DatasetBuilder(name='link_to_test_dataset', data=read_dataset_data)
                root2.set_dataset(dataset)
                write_io.write_builder(root2, link_data=False)

        with File(self.link_path, mode='r') as f:
            self.assertFalse(isinstance(f.get('link_to_test_dataset', getlink=True), ExternalLink))
            self.assertListEqual(f.get('link_to_test_dataset')[:].tolist(), [1, 2, 3, 4])


class TestLoadNamespaces(TestCase):

    def setUp(self):
        self.manager = _get_manager()
        self.path = get_temp_filepath()
        container = FooFile()
        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            io.write(container)

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    def test_load_namespaces_none_version(self):
        """Test that reading a file with a cached namespace and None version works but raises a warning."""
        # make the file have group name "None" instead of "0.1.0" (namespace version is used as group name)
        # and set the version key to "None"
        with h5py.File(self.path, mode='r+') as f:
            # rename the group
            f.move('/specifications/test_core/0.1.0', '/specifications/test_core/None')

            # replace the namespace dataset with a serialized dict with the version key set to 'None'
            new_ns = ('{"namespaces":[{"doc":"a test namespace","schema":[{"source":"test"}],"name":"test_core",'
                      '"version":"None"}]}')
            f['/specifications/test_core/None/namespace'][()] = new_ns

        # load the namespace from file
        ns_catalog = NamespaceCatalog()
        msg = "Loaded namespace '%s' is unversioned. Please notify the extension author." % CORE_NAMESPACE
        with self.assertWarnsWith(UserWarning, msg):
            HDF5IO.load_namespaces(ns_catalog, self.path)

    def test_load_namespaces_unversioned(self):
        """Test that reading a file with a cached, unversioned version works but raises a warning."""
        # make the file have group name "unversioned" instead of "0.1.0" (namespace version is used as group name)
        # and remove the version key
        with h5py.File(self.path, mode='r+') as f:
            # rename the group
            f.move('/specifications/test_core/0.1.0', '/specifications/test_core/unversioned')

            # replace the namespace dataset with a serialized dict without the version key
            new_ns = ('{"namespaces":[{"doc":"a test namespace","schema":[{"source":"test"}],"name":"test_core"}]}')
            f['/specifications/test_core/unversioned/namespace'][()] = new_ns

        # load the namespace from file
        ns_catalog = NamespaceCatalog()
        msg = ("Loaded namespace '%s' is missing the required key 'version'. Version will be set to "
               "'%s'. Please notify the extension author." % (CORE_NAMESPACE, SpecNamespace.UNVERSIONED))
        with self.assertWarnsWith(UserWarning, msg):
            HDF5IO.load_namespaces(ns_catalog, self.path)

    def test_load_namespaces_path(self):
        """Test that loading namespaces given a path is OK and returns the correct dictionary."""
        ns_catalog = NamespaceCatalog()
        d = HDF5IO.load_namespaces(ns_catalog, self.path)
        self.assertEqual(d, {'test_core': {}})  # test_core has no dependencies

    def test_load_namespaces_no_path_no_file(self):
        """Test that loading namespaces without a path or file raises an error."""
        ns_catalog = NamespaceCatalog()

        msg = "Either the 'path' or 'file' argument must be supplied."
        with self.assertRaisesWith(ValueError, msg):
            HDF5IO.load_namespaces(ns_catalog)

    def test_load_namespaces_file_no_path(self):
        """
        Test that loading namespaces from an h5py.File not backed by a file on disk is OK and does not close the file.
        """
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
        with h5py.File(self.path, 'r') as file_obj:
            ns_catalog = NamespaceCatalog()
            d = HDF5IO.load_namespaces(ns_catalog, path=self.path, file=file_obj)

            self.assertTrue(file_obj.__bool__())  # check file object is still open
            self.assertEqual(d, {'test_core': {}})

    def test_load_namespaces_file_path_mismatched(self):
        """Test that loading namespaces given an h5py.File and path that are mismatched raises an error."""
        with h5py.File(self.path, 'r') as file_obj:
            ns_catalog = NamespaceCatalog()

            msg = "You argued 'different_path' as this object's path, but supplied a file with filename: %s" % self.path
            with self.assertRaisesWith(ValueError, msg):
                HDF5IO.load_namespaces(ns_catalog, path='different_path', file=file_obj)

    def test_load_namespaces_with_pathlib_path(self):
        """Test that loading a namespace using a valid pathlib Path is OK and returns the correct dictionary."""
        pathlib_path = Path(self.path)
        ns_catalog = NamespaceCatalog()
        d = HDF5IO.load_namespaces(ns_catalog, pathlib_path)
        self.assertEqual(d, {'test_core': {}})  # test_core has no dependencies

    def test_load_namespaces_with_dependencies(self):
        """Test loading namespaces where one includes another."""
        file_spec = GroupSpec(doc="A FooFile", data_type_def='FooFile')
        spec_catalog = SpecCatalog()
        name = 'test_core2'
        namespace = SpecNamespace(
            doc='a test namespace',
            name=name,
            schema=[{'source': 'test.yaml', 'namespace': 'test_core'}],  # depends on test_core
            version='0.1.0',
            catalog=spec_catalog
        )
        spec_catalog.register_spec(file_spec, 'test.yaml')
        namespace_catalog = NamespaceCatalog()
        namespace_catalog.add_namespace(name, namespace)
        type_map = TypeMap(namespace_catalog)
        type_map.register_container_type(name, 'FooFile', FooFile)
        manager = BuildManager(type_map)
        container = FooFile()
        with HDF5IO(self.path, manager=manager, mode='a') as io:  # append to file
            io.write(container)

        ns_catalog = NamespaceCatalog()
        d = HDF5IO.load_namespaces(ns_catalog, self.path)
        self.assertEqual(d, {'test_core': {}, 'test_core2': {'test_core': ('Foo', 'FooBucket', 'FooFile')}})

    def test_load_namespaces_no_specloc(self):
        """Test loading namespaces where the file does not contain a SPEC_LOC_ATTR."""
        # delete the spec location attribute from the file
        with h5py.File(self.path, mode='r+') as f:
            del f.attrs[SPEC_LOC_ATTR]

        # load the namespace from file
        ns_catalog = NamespaceCatalog()
        msg = "No cached namespaces found in %s" % self.path
        with self.assertWarnsWith(UserWarning, msg):
            ret = HDF5IO.load_namespaces(ns_catalog, self.path)
        self.assertDictEqual(ret, {})

    def test_load_namespaces_resolve_custom_deps(self):
        """Test that reading a file with a cached namespace and different def/inc keys works."""
        # Setup all the data we need
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.path, manager=self.manager, mode='w') as io:
            io.write(foofile)

        with h5py.File(self.path, mode='r+') as f:
            # add two types where one extends the other and overrides an attribute
            # check that the inherited attribute resolves correctly despite having a different def/inc key than those
            # used in the namespace catalog
            added_types = (',{"data_type_def":"BigFoo","data_type_inc":"Foo","doc":"doc","attributes":['
                           '{"name":"my_attr","dtype":"text","doc":"an attr"}]},'
                           '{"data_type_def":"BiggerFoo","data_type_inc":"BigFoo","doc":"doc"}]}')
            old_test_source = f['/specifications/test_core/0.1.0/test']
            old_test_source[()] = old_test_source[()][0:-2] + added_types  # strip the ]} from end, then add to groups
            new_ns = ('{"namespaces":[{"doc":"a test namespace","schema":['
                      '{"namespace":"test_core","my_data_types":["Foo"]},'
                      '{"source":"test-ext.extensions"}'
                      '],"name":"test-ext","version":"0.1.0"}]}')
            f.create_dataset('/specifications/test-ext/0.1.0/namespace', data=new_ns)
            new_ext = '{"groups":[{"my_data_type_def":"FooExt","my_data_type_inc":"Foo","doc":"doc"}]}'
            f.create_dataset('/specifications/test-ext/0.1.0/test-ext.extensions', data=new_ext)

        # load the namespace from file
        ns_catalog = NamespaceCatalog(CustomGroupSpec, CustomDatasetSpec, CustomSpecNamespace)
        namespace_deps = HDF5IO.load_namespaces(ns_catalog, self.path)

        # test that the dependencies are correct
        expected = ('Foo',)
        self.assertTupleEqual((namespace_deps['test-ext']['test_core']), expected)

        # test that the types are loaded
        types = ns_catalog.get_types('test-ext.extensions')
        expected = ('FooExt',)
        self.assertTupleEqual(types, expected)

        # test that the def_key is updated for test-ext ns
        foo_ext_spec = ns_catalog.get_spec('test-ext', 'FooExt')
        self.assertTrue('my_data_type_def' in foo_ext_spec)
        self.assertTrue('my_data_type_inc' in foo_ext_spec)

        # test that the data_type_def is replaced with my_data_type_def for test_core ns
        bigger_foo_spec = ns_catalog.get_spec('test_core', 'BiggerFoo')
        self.assertTrue('my_data_type_def' in bigger_foo_spec)
        self.assertTrue('my_data_type_inc' in bigger_foo_spec)

        # test that my_attr is properly inherited in BiggerFoo from BigFoo and attr1, attr3 are inherited from Foo
        self.assertTrue(len(bigger_foo_spec.attributes) == 3)


class TestGetNamespaces(TestCase):

    def create_test_namespace(self, name, version):
        file_spec = GroupSpec(doc="A FooFile", data_type_def='FooFile')
        spec_catalog = SpecCatalog()
        namespace = SpecNamespace(
            doc='a test namespace',
            name=name,
            schema=[{'source': 'test.yaml'}],
            version=version,
            catalog=spec_catalog
        )
        spec_catalog.register_spec(file_spec, 'test.yaml')
        return namespace

    def write_test_file(self, name, version, mode):
        namespace = self.create_test_namespace(name, version)
        namespace_catalog = NamespaceCatalog()
        namespace_catalog.add_namespace(name, namespace)
        type_map = TypeMap(namespace_catalog)
        type_map.register_container_type(name, 'FooFile', FooFile)
        manager = BuildManager(type_map)
        with HDF5IO(self.path, manager=manager, mode=mode) as io:
            io.write(self.container)

    def setUp(self):
        self.path = get_temp_filepath()
        self.container = FooFile()

    def tearDown(self):
        if os.path.exists(self.path):
            os.remove(self.path)

    # see other tests for path & file match/mismatch testing in TestLoadNamespaces

    def test_get_namespaces_with_path(self):
        """Test getting namespaces given a path."""
        self.write_test_file('test_core', '0.1.0', 'w')

        ret = HDF5IO.get_namespaces(path=self.path)
        self.assertEqual(ret, {'test_core': '0.1.0'})

    def test_get_namespaces_with_file(self):
        """Test getting namespaces given a file object."""
        self.write_test_file('test_core', '0.1.0', 'w')

        with File(self.path, 'r') as f:
            ret = HDF5IO.get_namespaces(file=f)
            self.assertEqual(ret, {'test_core': '0.1.0'})
            self.assertTrue(f.__bool__())  # check file object is still open

    def test_get_namespaces_different_versions(self):
        """Test getting namespaces with multiple versions given a path."""
        # write file with spec with smaller version string
        self.write_test_file('test_core', '0.0.10', 'w')

        # append to file with spec with larger version string
        self.write_test_file('test_core', '0.1.0', 'a')

        ret = HDF5IO.get_namespaces(path=self.path)
        self.assertEqual(ret, {'test_core': '0.1.0'})

    def test_get_namespaces_multiple_namespaces(self):
        """Test getting multiple namespaces given a path."""
        self.write_test_file('test_core1', '0.0.10', 'w')
        self.write_test_file('test_core2', '0.1.0', 'a')

        ret = HDF5IO.get_namespaces(path=self.path)
        self.assertEqual(ret, {'test_core1': '0.0.10', 'test_core2': '0.1.0'})

    def test_get_namespaces_none_version(self):
        """Test getting namespaces where file has one None-versioned namespace."""
        self.write_test_file('test_core', '0.1.0', 'w')

        # make the file have group name "None" instead of "0.1.0" (namespace version is used as group name)
        # and set the version key to "None"
        with h5py.File(self.path, mode='r+') as f:
            # rename the group
            f.move('/specifications/test_core/0.1.0', '/specifications/test_core/None')

            # replace the namespace dataset with a serialized dict with the version key set to 'None'
            new_ns = ('{"namespaces":[{"doc":"a test namespace","schema":[{"source":"test"}],"name":"test_core",'
                      '"version":"None"}]}')
            f['/specifications/test_core/None/namespace'][()] = new_ns

        ret = HDF5IO.get_namespaces(path=self.path)
        self.assertEqual(ret, {'test_core': 'None'})

    def test_get_namespaces_none_and_other_version(self):
        """Test getting namespaces file has a namespace with a normal version and an 'None" version."""
        self.write_test_file('test_core', '0.1.0', 'w')

        # make the file have group name "None" instead of "0.1.0" (namespace version is used as group name)
        # and set the version key to "None"
        with h5py.File(self.path, mode='r+') as f:
            # rename the group
            f.move('/specifications/test_core/0.1.0', '/specifications/test_core/None')

            # replace the namespace dataset with a serialized dict with the version key set to 'None'
            new_ns = ('{"namespaces":[{"doc":"a test namespace","schema":[{"source":"test"}],"name":"test_core",'
                      '"version":"None"}]}')
            f['/specifications/test_core/None/namespace'][()] = new_ns

        # append to file with spec with a larger version string
        self.write_test_file('test_core', '0.2.0', 'a')

        ret = HDF5IO.get_namespaces(path=self.path)
        self.assertEqual(ret, {'test_core': '0.2.0'})

    def test_get_namespaces_unversioned(self):
        """Test getting namespaces where file has one unversioned namespace."""
        self.write_test_file('test_core', '0.1.0', 'w')

        # make the file have group name "unversioned" instead of "0.1.0" (namespace version is used as group name)
        with h5py.File(self.path, mode='r+') as f:
            # rename the group
            f.move('/specifications/test_core/0.1.0', '/specifications/test_core/unversioned')

            # replace the namespace dataset with a serialized dict without the version key
            new_ns = ('{"namespaces":[{"doc":"a test namespace","schema":[{"source":"test"}],"name":"test_core"}]}')
            f['/specifications/test_core/unversioned/namespace'][()] = new_ns

        ret = HDF5IO.get_namespaces(path=self.path)
        self.assertEqual(ret, {'test_core': 'unversioned'})

    def test_get_namespaces_unversioned_and_other(self):
        """Test getting namespaces file has a namespace with a normal version and an 'unversioned" version."""
        self.write_test_file('test_core', '0.1.0', 'w')

        # make the file have group name "unversioned" instead of "0.1.0" (namespace version is used as group name)
        with h5py.File(self.path, mode='r+') as f:
            # rename the group
            f.move('/specifications/test_core/0.1.0', '/specifications/test_core/unversioned')

            # replace the namespace dataset with a serialized dict without the version key
            new_ns = ('{"namespaces":[{"doc":"a test namespace","schema":[{"source":"test"}],"name":"test_core"}]}')
            f['/specifications/test_core/unversioned/namespace'][()] = new_ns

        # append to file with spec with a larger version string
        self.write_test_file('test_core', '0.2.0', 'a')

        ret = HDF5IO.get_namespaces(path=self.path)
        self.assertEqual(ret, {'test_core': '0.2.0'})

    def test_get_namespaces_no_specloc(self):
        """Test getting namespaces where the file does not contain a SPEC_LOC_ATTR."""
        self.write_test_file('test_core', '0.1.0', 'w')

        # delete the spec location attribute from the file
        with h5py.File(self.path, mode='r+') as f:
            del f.attrs[SPEC_LOC_ATTR]

        # load the namespace from file
        msg = "No cached namespaces found in %s" % self.path
        with self.assertWarnsWith(UserWarning, msg):
            ret = HDF5IO.get_namespaces(path=self.path)
        self.assertDictEqual(ret, {})


class TestExport(TestCase):
    """Test exporting HDF5 to HDF5 using HDF5IO.export_container_to_hdf5."""

    def setUp(self):
        self.paths = [
            get_temp_filepath(),
            get_temp_filepath(),
            get_temp_filepath(),
            get_temp_filepath(),
        ]
        self.ios = []

    def tearDown(self):
        for io in self.ios:
            io.close_linked_files()
        for p in self.paths:
            if os.path.exists(p):
                os.remove(p)

    def test_basic(self):
        """Test that exporting a written container works."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='r') as read_io:
            with HDF5IO(self.paths[1], mode='w') as export_io:
                export_io.export(src_io=read_io)

        self.assertTrue(os.path.exists(self.paths[1]))
        self.assertEqual(foofile.container_source, self.paths[0])

        with HDF5IO(self.paths[1], manager=_get_manager(), mode='r') as read_io:
            read_foofile = read_io.read()
            self.assertEqual(read_foofile.container_source, self.paths[1])
            self.assertContainerEqual(foofile, read_foofile, ignore_hdmf_attrs=True)
            self.assertEqual(os.path.abspath(read_foofile.buckets['bucket1'].foos['foo1'].my_data.file.filename),
                             self.paths[1])

    def test_basic_container(self):
        """Test that exporting a written container, passing in the container arg, works."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='r') as read_io:
            read_foofile = read_io.read()

            with HDF5IO(self.paths[1], mode='w') as export_io:
                export_io.export(src_io=read_io, container=read_foofile)

        self.assertTrue(os.path.exists(self.paths[1]))
        self.assertEqual(foofile.container_source, self.paths[0])

        with HDF5IO(self.paths[1], manager=_get_manager(), mode='r') as read_io:
            read_foofile = read_io.read()
            self.assertEqual(read_foofile.container_source, self.paths[1])
            self.assertContainerEqual(foofile, read_foofile, ignore_hdmf_attrs=True)

    def test_container_part(self):
        """Test that exporting a part of a written container raises an error."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='r') as read_io:
            read_foofile = read_io.read()

            with HDF5IO(self.paths[1], mode='w') as export_io:
                msg = ("The provided container must be the root of the hierarchy of the source used to read the "
                       "container.")
                with self.assertRaisesWith(ValueError, msg):
                    export_io.export(src_io=read_io, container=read_foofile.buckets['bucket1'])

    def test_container_unknown(self):
        """Test that exporting a container that did not come from the src_io object raises an error."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='r') as read_io:

            with HDF5IO(self.paths[1], mode='w') as export_io:
                dummy_file = FooFile([])
                msg = "The provided container must have been read by the provided src_io."
                with self.assertRaisesWith(ValueError, msg):
                    export_io.export(src_io=read_io, container=dummy_file)

    def test_cache_spec(self):
        """Test that exporting with cache_spec works."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='r') as read_io:
            read_foofile = read_io.read()

            with HDF5IO(self.paths[1], mode='w') as export_io:
                export_io.export(
                    src_io=read_io,
                    container=read_foofile,
                    cache_spec=False,
                )

        with File(self.paths[1], 'r') as f:
            self.assertNotIn('specifications', f)

    def test_soft_link_group(self):
        """Test that exporting a written file with soft linked groups keeps links within the file."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket], foo_link=foo1)

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='r') as read_io:

            with HDF5IO(self.paths[1], mode='w') as export_io:
                export_io.export(src_io=read_io)

        with HDF5IO(self.paths[1], manager=_get_manager(), mode='r') as read_io:
            self.ios.append(read_io)  # track IO objects for tearDown
            read_foofile2 = read_io.read()

            # make sure the linked group is within the same file
            self.assertEqual(read_foofile2.foo_link.container_source, self.paths[1])

    def test_soft_link_dataset(self):
        """Test that exporting a written file with soft linked datasets keeps links within the file."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket], foofile_data=foo1.my_data)

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='r') as read_io:
            self.ios.append(read_io)  # track IO objects for tearDown

            with HDF5IO(self.paths[1], mode='w') as export_io:
                export_io.export(src_io=read_io)

        with HDF5IO(self.paths[1], manager=_get_manager(), mode='r') as read_io:
            self.ios.append(read_io)  # track IO objects for tearDown
            read_foofile2 = read_io.read()

            # make sure the linked dataset is within the same file
            self.assertEqual(read_foofile2.foofile_data.file.filename, self.paths[1])

    def test_external_link_group(self):
        """Test that exporting a written file with external linked groups maintains the links."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as read_io:
            read_io.write(foofile)

        manager = _get_manager()
        with HDF5IO(self.paths[0], manager=manager, mode='r') as read_io:
            read_foofile = read_io.read()
            # make external link to existing group
            foofile2 = FooFile(foo_link=read_foofile.buckets['bucket1'].foos['foo1'])

            with HDF5IO(self.paths[1], manager=manager, mode='w') as write_io:
                write_io.write(foofile2)

        with HDF5IO(self.paths[1], manager=_get_manager(), mode='r') as read_io:
            self.ios.append(read_io)  # track IO objects for tearDown
            read_foofile2 = read_io.read()

            with HDF5IO(self.paths[2], mode='w') as export_io:
                export_io.export(src_io=read_io)

        with HDF5IO(self.paths[2], manager=_get_manager(), mode='r') as read_io:
            self.ios.append(read_io)  # track IO objects for tearDown
            read_foofile2 = read_io.read()

            # make sure the linked group is read from the first file
            self.assertEqual(read_foofile2.foo_link.container_source, self.paths[0])

    def test_external_link_dataset(self):
        """Test that exporting a written file with external linked datasets maintains the links."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket], foofile_data=[1, 2, 3])

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        manager = _get_manager()
        with HDF5IO(self.paths[0], manager=manager, mode='r') as read_io:
            read_foofile = read_io.read()
            foofile2 = FooFile(foofile_data=read_foofile.foofile_data)  # make external link to existing dataset

            with HDF5IO(self.paths[1], manager=manager, mode='w') as write_io:
                write_io.write(foofile2)

        with HDF5IO(self.paths[1], manager=_get_manager(), mode='r') as read_io:
            self.ios.append(read_io)  # track IO objects for tearDown

            with HDF5IO(self.paths[2], mode='w') as export_io:
                export_io.export(src_io=read_io)

        with HDF5IO(self.paths[2], manager=_get_manager(), mode='r') as read_io:
            self.ios.append(read_io)  # track IO objects for tearDown
            read_foofile2 = read_io.read()

            # make sure the linked dataset is read from the first file
            self.assertEqual(read_foofile2.foofile_data.file.filename, self.paths[0])

    def test_external_link_link(self):
        """Test that exporting a written file with external links to external links maintains the links."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        manager = _get_manager()
        with HDF5IO(self.paths[0], manager=manager, mode='r') as read_io:
            read_foofile = read_io.read()
            # make external link to existing group
            foofile2 = FooFile(foo_link=read_foofile.buckets['bucket1'].foos['foo1'])

            with HDF5IO(self.paths[1], manager=manager, mode='w') as write_io:
                write_io.write(foofile2)

        manager = _get_manager()
        with HDF5IO(self.paths[1], manager=manager, mode='r') as read_io:
            self.ios.append(read_io)  # track IO objects for tearDown
            read_foofile2 = read_io.read()
            foofile3 = FooFile(foo_link=read_foofile2.foo_link)  # make external link to external link

            with HDF5IO(self.paths[2], manager=manager, mode='w') as write_io:
                write_io.write(foofile3)

        with HDF5IO(self.paths[2], manager=_get_manager(), mode='r') as read_io:
            self.ios.append(read_io)  # track IO objects for tearDown

            with HDF5IO(self.paths[3], mode='w') as export_io:
                export_io.export(src_io=read_io)

        with HDF5IO(self.paths[3], manager=_get_manager(), mode='r') as read_io:
            self.ios.append(read_io)  # track IO objects for tearDown
            read_foofile3 = read_io.read()

            # make sure the linked group is read from the first file
            self.assertEqual(read_foofile3.foo_link.container_source, self.paths[0])

    def test_attr_reference(self):
        """Test that exporting a written file with attribute references maintains the references."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket], foo_ref_attr=foo1)

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as read_io:
            read_io.write(foofile)

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='r') as read_io:

            with HDF5IO(self.paths[1], mode='w') as export_io:
                export_io.export(src_io=read_io)

        with HDF5IO(self.paths[1], manager=_get_manager(), mode='r') as read_io:
            read_foofile2 = read_io.read()

            # make sure the attribute reference resolves to the container within the same file
            self.assertIs(read_foofile2.foo_ref_attr, read_foofile2.buckets['bucket1'].foos['foo1'])

        with File(self.paths[1], 'r') as f:
            self.assertIsInstance(f.attrs['foo_ref_attr'], h5py.Reference)

    def test_pop_data(self):
        """Test that exporting a written container after removing an element from it works."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='r') as read_io:
            read_foofile = read_io.read()
            read_foofile.remove_bucket('bucket1')  # remove child group

            with HDF5IO(self.paths[1], mode='w') as export_io:
                export_io.export(src_io=read_io, container=read_foofile)

        with HDF5IO(self.paths[1], manager=_get_manager(), mode='r') as read_io:
            read_foofile2 = read_io.read()

            # make sure the read foofile has no buckets
            self.assertDictEqual(read_foofile2.buckets, {})

        # check that file size of file 2 is smaller
        self.assertTrue(os.path.getsize(self.paths[0]) > os.path.getsize(self.paths[1]))

    def test_pop_linked_group(self):
        """Test that exporting a written container after removing a linked element from it works."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket], foo_link=foo1)

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='r') as read_io:
            read_foofile = read_io.read()
            read_foofile.buckets['bucket1'].remove_foo('foo1')  # remove child group

            with HDF5IO(self.paths[1], mode='w') as export_io:
                msg = ("links (links): Linked Foo 'foo1' has no parent. Remove the link or ensure the linked "
                       "container is added properly.")
                with self.assertRaisesWith(OrphanContainerBuildError, msg):
                    export_io.export(src_io=read_io, container=read_foofile)

    def test_append_data(self):
        """Test that exporting a written container after adding groups, links, and references to it works."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='r') as read_io:
            read_foofile = read_io.read()

            # create a foo with link to existing dataset my_data, add the foo to new foobucket
            # this should make a soft link within the exported file
            foo2 = Foo('foo2', read_foofile.buckets['bucket1'].foos['foo1'].my_data, "I am foo2", 17, 3.14)
            foobucket2 = FooBucket('bucket2', [foo2])
            read_foofile.add_bucket(foobucket2)

            # also add link from foofile to new foo2 container
            read_foofile.foo_link = foo2

            # also add link from foofile to new foo2.my_data dataset which is a link to foo1.my_data dataset
            read_foofile.foofile_data = foo2.my_data

            # also add reference from foofile to new foo2
            read_foofile.foo_ref_attr = foo2

            with HDF5IO(self.paths[1], mode='w') as export_io:
                export_io.export(src_io=read_io, container=read_foofile)

        with HDF5IO(self.paths[1], manager=_get_manager(), mode='r') as read_io:
            self.ios.append(read_io)  # track IO objects for tearDown
            read_foofile2 = read_io.read()

            # test new soft link to dataset in file
            self.assertIs(read_foofile2.buckets['bucket1'].foos['foo1'].my_data,
                          read_foofile2.buckets['bucket2'].foos['foo2'].my_data)

            # test new soft link to group in file
            self.assertIs(read_foofile2.foo_link, read_foofile2.buckets['bucket2'].foos['foo2'])

            # test new soft link to new soft link to dataset in file
            self.assertIs(read_foofile2.buckets['bucket1'].foos['foo1'].my_data, read_foofile2.foofile_data)

            # test new attribute reference to new group in file
            self.assertIs(read_foofile2.foo_ref_attr, read_foofile2.buckets['bucket2'].foos['foo2'])

        with File(self.paths[1], 'r') as f:
            self.assertEqual(f['foofile_data'].file.filename, self.paths[1])
            self.assertIsInstance(f.attrs['foo_ref_attr'], h5py.Reference)

    def test_append_external_link_data(self):
        """Test that exporting a written container after adding a link with link_data=True creates external links."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        foofile2 = FooFile([])

        with HDF5IO(self.paths[1], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile2)

        manager = _get_manager()
        with HDF5IO(self.paths[0], manager=manager, mode='r') as read_io1:
            self.ios.append(read_io1)  # track IO objects for tearDown
            read_foofile1 = read_io1.read()

            with HDF5IO(self.paths[1], manager=manager, mode='r') as read_io2:
                self.ios.append(read_io2)
                read_foofile2 = read_io2.read()

                # create a foo with link to existing dataset my_data (not in same file), add the foo to new foobucket
                # this should make an external link within the exported file
                foo2 = Foo('foo2', read_foofile1.buckets['bucket1'].foos['foo1'].my_data, "I am foo2", 17, 3.14)
                foobucket2 = FooBucket('bucket2', [foo2])
                read_foofile2.add_bucket(foobucket2)

                # also add link from foofile to new foo2.my_data dataset which is a link to foo1.my_data dataset
                # this should make an external link within the exported file
                read_foofile2.foofile_data = foo2.my_data

                with HDF5IO(self.paths[2], mode='w') as export_io:
                    export_io.export(src_io=read_io2, container=read_foofile2)

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='r') as read_io1:
            self.ios.append(read_io1)  # track IO objects for tearDown
            read_foofile3 = read_io1.read()

            with HDF5IO(self.paths[2], manager=_get_manager(), mode='r') as read_io2:
                self.ios.append(read_io2)  # track IO objects for tearDown
                read_foofile4 = read_io2.read()

                self.assertEqual(read_foofile4.buckets['bucket2'].foos['foo2'].my_data,
                                 read_foofile3.buckets['bucket1'].foos['foo1'].my_data)
                self.assertEqual(read_foofile4.foofile_data, read_foofile3.buckets['bucket1'].foos['foo1'].my_data)

        with File(self.paths[2], 'r') as f:
            self.assertEqual(f['buckets/bucket2/foo_holder/foo2/my_data'].file.filename, self.paths[0])
            self.assertEqual(f['foofile_data'].file.filename, self.paths[0])
            self.assertIsInstance(f.get('buckets/bucket2/foo_holder/foo2/my_data', getlink=True),
                                  h5py.ExternalLink)
            self.assertIsInstance(f.get('foofile_data', getlink=True), h5py.ExternalLink)

    def test_append_external_link_copy_data(self):
        """Test that exporting a written container after adding a link with link_data=False copies the data."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        foofile2 = FooFile([])

        with HDF5IO(self.paths[1], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile2)

        manager = _get_manager()
        with HDF5IO(self.paths[0], manager=manager, mode='r') as read_io1:
            self.ios.append(read_io1)  # track IO objects for tearDown
            read_foofile1 = read_io1.read()

            with HDF5IO(self.paths[1], manager=manager, mode='r') as read_io2:
                self.ios.append(read_io2)
                read_foofile2 = read_io2.read()

                # create a foo with link to existing dataset my_data (not in same file), add the foo to new foobucket
                # this would normally make an external link but because link_data=False, data will be copied
                foo2 = Foo('foo2', read_foofile1.buckets['bucket1'].foos['foo1'].my_data, "I am foo2", 17, 3.14)
                foobucket2 = FooBucket('bucket2', [foo2])
                read_foofile2.add_bucket(foobucket2)

                # also add link from foofile to new foo2.my_data dataset which is a link to foo1.my_data dataset
                # this would normally make an external link but because link_data=False, data will be copied
                read_foofile2.foofile_data = foo2.my_data

                with HDF5IO(self.paths[2], mode='w') as export_io:
                    export_io.export(src_io=read_io2, container=read_foofile2, write_args={'link_data': False})

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='r') as read_io1:
            self.ios.append(read_io1)  # track IO objects for tearDown
            read_foofile3 = read_io1.read()

            with HDF5IO(self.paths[2], manager=_get_manager(), mode='r') as read_io2:
                self.ios.append(read_io2)  # track IO objects for tearDown
                read_foofile4 = read_io2.read()

                # check that file can be read
                self.assertNotEqual(read_foofile4.buckets['bucket2'].foos['foo2'].my_data,
                                    read_foofile3.buckets['bucket1'].foos['foo1'].my_data)
                self.assertNotEqual(read_foofile4.foofile_data, read_foofile3.buckets['bucket1'].foos['foo1'].my_data)
                self.assertNotEqual(read_foofile4.foofile_data, read_foofile4.buckets['bucket2'].foos['foo2'].my_data)

        with File(self.paths[2], 'r') as f:
            self.assertEqual(f['buckets/bucket2/foo_holder/foo2/my_data'].file.filename, self.paths[2])
            self.assertEqual(f['foofile_data'].file.filename, self.paths[2])

    def test_export_io(self):
        """Test that exporting a written container using HDF5IO.export_io works."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='r') as read_io:
            HDF5IO.export_io(src_io=read_io, path=self.paths[1])

        self.assertTrue(os.path.exists(self.paths[1]))
        self.assertEqual(foofile.container_source, self.paths[0])

        with HDF5IO(self.paths[1], manager=_get_manager(), mode='r') as read_io:
            read_foofile = read_io.read()
            self.assertEqual(read_foofile.container_source, self.paths[1])
            self.assertContainerEqual(foofile, read_foofile, ignore_hdmf_attrs=True)

    def test_export_dset_refs(self):
        """Test that exporting a written container with a dataset of references works."""
        bazs = []
        num_bazs = 10
        for i in range(num_bazs):
            bazs.append(Baz(name='baz%d' % i))
        baz_data = BazData(name='baz_data1', data=bazs)
        bucket = BazBucket(name='bucket1', bazs=bazs.copy(), baz_data=baz_data)

        with HDF5IO(self.paths[0], manager=_get_baz_manager(), mode='w') as write_io:
            write_io.write(bucket)

        with HDF5IO(self.paths[0], manager=_get_baz_manager(), mode='r') as read_io:
            read_bucket1 = read_io.read()

            # NOTE: reference IDs might be the same between two identical files
            # adding a Baz with a smaller name should change the reference IDs on export
            new_baz = Baz(name='baz000')
            read_bucket1.add_baz(new_baz)

            with HDF5IO(self.paths[1], mode='w') as export_io:
                export_io.export(src_io=read_io, container=read_bucket1)

        with HDF5IO(self.paths[1], manager=_get_baz_manager(), mode='r') as read_io:
            read_bucket2 = read_io.read()

            # remove and check the appended child, then compare the read container with the original
            read_new_baz = read_bucket2.remove_baz('baz000')
            self.assertContainerEqual(new_baz, read_new_baz, ignore_hdmf_attrs=True)

            self.assertContainerEqual(bucket, read_bucket2, ignore_name=True, ignore_hdmf_attrs=True)
            for i in range(num_bazs):
                baz_name = 'baz%d' % i
                self.assertIs(read_bucket2.baz_data.data[i], read_bucket2.bazs[baz_name])

    def test_export_cpd_dset_refs(self):
        """Test that exporting a written container with a compound dataset with references works."""
        bazs = []
        baz_pairs = []
        num_bazs = 10
        for i in range(num_bazs):
            b = Baz(name='baz%d' % i)
            bazs.append(b)
            baz_pairs.append((i, b))
        baz_cpd_data = BazCpdData(name='baz_cpd_data1', data=baz_pairs)
        bucket = BazBucket(name='bucket1', bazs=bazs.copy(), baz_cpd_data=baz_cpd_data)

        with HDF5IO(self.paths[0], manager=_get_baz_manager(), mode='w') as write_io:
            write_io.write(bucket)

        with HDF5IO(self.paths[0], manager=_get_baz_manager(), mode='r') as read_io:
            read_bucket1 = read_io.read()

            # NOTE: reference IDs might be the same between two identical files
            # adding a Baz with a smaller name should change the reference IDs on export
            new_baz = Baz(name='baz000')
            read_bucket1.add_baz(new_baz)

            with HDF5IO(self.paths[1], mode='w') as export_io:
                export_io.export(src_io=read_io, container=read_bucket1)

        with HDF5IO(self.paths[1], manager=_get_baz_manager(), mode='r') as read_io:
            read_bucket2 = read_io.read()

            # remove and check the appended child, then compare the read container with the original
            read_new_baz = read_bucket2.remove_baz(new_baz.name)
            self.assertContainerEqual(new_baz, read_new_baz, ignore_hdmf_attrs=True)

            self.assertContainerEqual(bucket, read_bucket2, ignore_name=True, ignore_hdmf_attrs=True)
            for i in range(num_bazs):
                baz_name = 'baz%d' % i
                self.assertEqual(read_bucket2.baz_cpd_data.data[i][0], i)
                self.assertIs(read_bucket2.baz_cpd_data.data[i][1], read_bucket2.bazs[baz_name])

    def test_non_manager_container(self):
        """Test that exporting with a src_io without a manager raises an error."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        class OtherIO(HDMFIO):

            def read_builder(self):
                pass

            def write_builder(self, **kwargs):
                pass

            def open(self):
                pass

            def close(self):
                pass

        with OtherIO() as read_io:
            with HDF5IO(self.paths[1], mode='w') as export_io:
                msg = 'When a container is provided, src_io must have a non-None manager (BuildManager) property.'
                with self.assertRaisesWith(ValueError, msg):
                    export_io.export(src_io=read_io, container=foofile, write_args={'link_data': False})

    def test_non_HDF5_src_link_data_true(self):
        """Test that exporting with a src_io without a manager raises an error."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        class OtherIO(HDMFIO):

            def __init__(self, manager):
                super().__init__(manager=manager)

            def read_builder(self):
                pass

            def write_builder(self, **kwargs):
                pass

            def open(self):
                pass

            def close(self):
                pass

        with OtherIO(manager=_get_manager()) as read_io:
            with HDF5IO(self.paths[1], mode='w') as export_io:
                msg = "Cannot export from non-HDF5 backend OtherIO to HDF5 with write argument link_data=True."
                with self.assertRaisesWith(UnsupportedOperation, msg):
                    export_io.export(src_io=read_io, container=foofile)

    def test_wrong_mode(self):
        """Test that exporting with a src_io without a manager raises an error."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile([foobucket])

        with HDF5IO(self.paths[0], manager=_get_manager(), mode='w') as write_io:
            write_io.write(foofile)

        with HDF5IO(self.paths[0], mode='r') as read_io:
            with HDF5IO(self.paths[1], mode='a') as export_io:
                msg = "Cannot export to file %s in mode 'a'. Please use mode 'w'." % self.paths[1]
                with self.assertRaisesWith(UnsupportedOperation, msg):
                    export_io.export(src_io=read_io)


class TestDatasetRefs(TestCase):

    def test_roundtrip(self):
        self.path = get_temp_filepath()
        bazs = []
        num_bazs = 10
        for i in range(num_bazs):
            bazs.append(Baz(name='baz%d' % i))
        baz_data = BazData(name='baz_data1', data=bazs)
        bucket = BazBucket(name='bucket1', bazs=bazs.copy(), baz_data=baz_data)

        with HDF5IO(self.path, manager=_get_baz_manager(), mode='w') as write_io:
            write_io.write(bucket)

        with HDF5IO(self.path, manager=_get_baz_manager(), mode='r') as read_io:
            read_bucket = read_io.read()

            self.assertContainerEqual(bucket, read_bucket, ignore_name=True)
            for i in range(num_bazs):
                baz_name = 'baz%d' % i
                self.assertIs(read_bucket.baz_data.data[i], read_bucket.bazs[baz_name])


class TestCpdDatasetRefs(TestCase):

    def test_roundtrip(self):
        self.path = get_temp_filepath()
        bazs = []
        baz_pairs = []
        num_bazs = 10
        for i in range(num_bazs):
            b = Baz(name='baz%d' % i)
            bazs.append(b)
            baz_pairs.append((i, b))
        baz_cpd_data = BazCpdData(name='baz_cpd_data1', data=baz_pairs)
        bucket = BazBucket(name='bucket1', bazs=bazs.copy(), baz_cpd_data=baz_cpd_data)

        with HDF5IO(self.path, manager=_get_baz_manager(), mode='w') as write_io:
            write_io.write(bucket)

        with HDF5IO(self.path, manager=_get_baz_manager(), mode='r') as read_io:
            read_bucket = read_io.read()

            self.assertContainerEqual(bucket, read_bucket, ignore_name=True)
            for i in range(num_bazs):
                baz_name = 'baz%d' % i
                self.assertEqual(read_bucket.baz_cpd_data.data[i][0], i)
                self.assertIs(read_bucket.baz_cpd_data.data[i][1], read_bucket.bazs[baz_name])


class Baz(Container):

    pass


class BazData(Data):

    pass


class BazCpdData(Data):

    pass


class BazBucket(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this bucket'},
            {'name': 'bazs', 'type': list, 'doc': 'the Baz objects in this bucket'},
            {'name': 'baz_data', 'type': BazData, 'doc': 'dataset of Baz references', 'default': None},
            {'name': 'baz_cpd_data', 'type': BazCpdData, 'doc': 'dataset of Baz references', 'default': None})
    def __init__(self, **kwargs):
        name, bazs, baz_data, baz_cpd_data = getargs('name', 'bazs', 'baz_data', 'baz_cpd_data', kwargs)
        super().__init__(name=name)
        self.__bazs = {b.name: b for b in bazs}  # note: collections of groups are unordered in HDF5
        for b in bazs:
            b.parent = self
        self.__baz_data = baz_data
        if self.__baz_data is not None:
            self.__baz_data.parent = self
        self.__baz_cpd_data = baz_cpd_data
        if self.__baz_cpd_data is not None:
            self.__baz_cpd_data.parent = self

    @property
    def bazs(self):
        return self.__bazs

    @property
    def baz_data(self):
        return self.__baz_data

    @property
    def baz_cpd_data(self):
        return self.__baz_cpd_data

    def add_baz(self, baz):
        self.__bazs[baz.name] = baz
        baz.parent = self

    def remove_baz(self, baz_name):
        baz = self.__bazs.pop(baz_name)
        self._remove_child(baz)
        return baz


def _get_baz_manager():
    baz_spec = GroupSpec(
        doc='A test group specification with a data type',
        data_type_def='Baz',
    )

    baz_data_spec = DatasetSpec(
        doc='A test dataset of references specification with a data type',
        name='baz_data',
        data_type_def='BazData',
        dtype=RefSpec('Baz', 'object'),
        shape=[None],
    )

    baz_cpd_data_spec = DatasetSpec(
        doc='A test compound dataset with references specification with a data type',
        name='baz_cpd_data',
        data_type_def='BazCpdData',
        dtype=[DtypeSpec(name='part1', doc='doc', dtype='int'),
               DtypeSpec(name='part2', doc='doc', dtype=RefSpec('Baz', 'object'))],
        shape=[None],
    )

    baz_holder_spec = GroupSpec(
        doc='group of bazs',
        name='bazs',
        groups=[GroupSpec(doc='Baz', data_type_inc='Baz', quantity=ONE_OR_MANY)],
    )

    baz_bucket_spec = GroupSpec(
        doc='A test group specification for a data type containing data type',
        data_type_def='BazBucket',
        groups=[baz_holder_spec],
        datasets=[DatasetSpec(doc='doc', data_type_inc='BazData', quantity=ZERO_OR_ONE),
                  DatasetSpec(doc='doc', data_type_inc='BazCpdData', quantity=ZERO_OR_ONE)],
    )

    spec_catalog = SpecCatalog()
    spec_catalog.register_spec(baz_spec, 'test.yaml')
    spec_catalog.register_spec(baz_data_spec, 'test.yaml')
    spec_catalog.register_spec(baz_cpd_data_spec, 'test.yaml')
    spec_catalog.register_spec(baz_bucket_spec, 'test.yaml')

    namespace = SpecNamespace(
        'a test namespace',
        CORE_NAMESPACE,
        [{'source': 'test.yaml'}],
        version='0.1.0',
        catalog=spec_catalog)

    namespace_catalog = NamespaceCatalog()
    namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)

    type_map = TypeMap(namespace_catalog)
    type_map.register_container_type(CORE_NAMESPACE, 'Baz', Baz)
    type_map.register_container_type(CORE_NAMESPACE, 'BazData', BazData)
    type_map.register_container_type(CORE_NAMESPACE, 'BazCpdData', BazCpdData)
    type_map.register_container_type(CORE_NAMESPACE, 'BazBucket', BazBucket)

    class BazBucketMapper(ObjectMapper):
        def __init__(self, spec):
            super().__init__(spec)
            baz_holder_spec = spec.get_group('bazs')
            self.unmap(baz_holder_spec)
            baz_spec = baz_holder_spec.get_data_type('Baz')
            self.map_spec('bazs', baz_spec)

    type_map.register_map(BazBucket, BazBucketMapper)

    manager = BuildManager(type_map)
    return manager
