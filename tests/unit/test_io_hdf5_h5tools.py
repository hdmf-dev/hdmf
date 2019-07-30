import os
import unittest2 as unittest
import tempfile
import warnings
import numpy as np
import re

from hdmf.utils import docval, getargs
from hdmf.data_utils import DataChunkIterator, InvalidDataIOError
from hdmf.backends.hdf5.h5tools import HDF5IO, ROOT_NAME
from hdmf.backends.hdf5 import H5DataIO
from hdmf.backends.io import UnsupportedOperation
from hdmf.build import DatasetBuilder, BuildManager, TypeMap, ObjectMapper
from hdmf.spec.namespace import NamespaceCatalog
from hdmf.spec.spec import AttributeSpec, DatasetSpec, GroupSpec, ZERO_OR_MANY, ONE_OR_MANY
from hdmf.spec.namespace import SpecNamespace
from hdmf.spec.catalog import SpecCatalog
from hdmf.container import Container
from h5py import SoftLink, HardLink, ExternalLink, File

from tests.unit.test_utils import Foo, FooBucket, CORE_NAMESPACE


class FooFile(Container):

    @docval({'name': 'buckets', 'type': list, 'doc': 'the FooBuckets in this file', 'default': list()})
    def __init__(self, **kwargs):
        buckets = getargs('buckets', kwargs)
        super(FooFile, self).__init__(name=ROOT_NAME)  # name is not used - FooFile should be the root container
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


class H5IOTest(unittest.TestCase):
    """Tests for h5tools IO tools"""

    def setUp(self):
        self.test_temp_file = tempfile.NamedTemporaryFile()

        # On Windows h5py cannot truncate an open file in write mode.
        # The temp file will be closed before h5py truncates it
        # and will be removed during the tearDown step.
        self.test_temp_file.close()
        self.io = HDF5IO(self.test_temp_file.name, mode='a')
        self.f = self.io._file

    def tearDown(self):
        path = self.f.filename
        self.f.close()
        os.remove(path)
        del self.f
        del self.test_temp_file
        self.f = None
        self.test_temp_file = None

    ##########################################
    #  __chunked_iter_fill__(...) tests
    ##########################################
    def test__chunked_iter_fill_iterator_matched_buffer_size(self):
        dci = DataChunkIterator(data=range(10), buffer_size=2)
        my_dset = HDF5IO.__chunked_iter_fill__(self.f, 'test_dataset', dci)
        self.assertListEqual(my_dset[:].tolist(), list(range(10)))

    def test__chunked_iter_fill_iterator_unmatched_buffer_size(self):
        dci = DataChunkIterator(data=range(10), buffer_size=3)
        my_dset = HDF5IO.__chunked_iter_fill__(self.f, 'test_dataset', dci)
        self.assertListEqual(my_dset[:].tolist(), list(range(10)))

    def test__chunked_iter_fill_numpy_matched_buffer_size(self):
        a = np.arange(30).reshape(5, 2, 3)
        dci = DataChunkIterator(data=a, buffer_size=1)
        my_dset = HDF5IO.__chunked_iter_fill__(self.f, 'test_dataset', dci)
        self.assertTrue(np.all(my_dset[:] == a))
        self.assertTupleEqual(my_dset.shape, a.shape)

    def test__chunked_iter_fill_numpy_unmatched_buffer_size(self):
        a = np.arange(30).reshape(5, 2, 3)
        dci = DataChunkIterator(data=a, buffer_size=3)
        my_dset = HDF5IO.__chunked_iter_fill__(self.f, 'test_dataset', dci)
        self.assertTrue(np.all(my_dset[:] == a))
        self.assertTupleEqual(my_dset.shape, a.shape)

    def test__chunked_iter_fill_list_matched_buffer_size(self):
        a = np.arange(30).reshape(5, 2, 3)
        dci = DataChunkIterator(data=a.tolist(), buffer_size=1)
        my_dset = HDF5IO.__chunked_iter_fill__(self.f, 'test_dataset', dci)
        self.assertTrue(np.all(my_dset[:] == a))
        self.assertTupleEqual(my_dset.shape, a.shape)

    def test__chunked_iter_fill_numpy_unmatched_buffer_size(self):  # noqa: F811
        a = np.arange(30).reshape(5, 2, 3)
        dci = DataChunkIterator(data=a.tolist(), buffer_size=3)
        my_dset = HDF5IO.__chunked_iter_fill__(self.f, 'test_dataset', dci)
        self.assertTrue(np.all(my_dset[:] == a))
        self.assertTupleEqual(my_dset.shape, a.shape)

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

    def test_write_dataset_list_compress(self):
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
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', dci, attributes={}))
        dset = self.f['test_dataset']
        self.assertListEqual(dset[:].tolist(), list(range(10)))

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
        # Make sure no warning is issued when using szip
        with warnings.catch_warnings(record=True) as w:
            dset = H5DataIO(np.arange(30),
                            compression='szip')
            self.assertEqual(len(w), 1)
            self.assertEqual(dset.io_settings['compression'], 'szip')
        # Make sure no warning is issued when using lzf
        with warnings.catch_warnings(record=True) as w:
            dset = H5DataIO(np.arange(30),
                            compression='lzf')
            self.assertEqual(len(w), 1)
            self.assertEqual(dset.io_settings['compression'], 'lzf')

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

    class BucketMapper(ObjectMapper):
        def __init__(self, spec):
            super(BucketMapper, self).__init__(spec)
            foo_spec = spec.get_group('foo_holder').get_data_type('Foo')
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
            super(FileMapper, self).__init__(spec)
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
        catalog=spec_catalog)
    namespace_catalog = NamespaceCatalog()
    namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)
    type_map = TypeMap(namespace_catalog)

    type_map.register_container_type(CORE_NAMESPACE, 'Foo', Foo)
    type_map.register_container_type(CORE_NAMESPACE, 'FooBucket', FooBucket)
    type_map.register_container_type(CORE_NAMESPACE, 'FooFile', FooFile)

    type_map.register_map(FooBucket, BucketMapper)
    type_map.register_map(FooFile, FileMapper)

    manager = BuildManager(type_map)
    return manager


class TestRoundTrip(unittest.TestCase):

    def setUp(self):
        self.manager = _get_manager()
        self.test_temp_file = tempfile.NamedTemporaryFile()
        self.test_temp_file.close()
        # On Windows h5py cannot truncate an open file in write mode.
        # The temp file will be closed before h5py truncates it
        # and will be removed during the tearDown step.
        self.path = self.test_temp_file.name

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


class TestCacheSpec(unittest.TestCase):

    def setUp(self):
        self.manager = _get_manager()
        self.test_temp_file = tempfile.NamedTemporaryFile()
        self.test_temp_file.close()
        # On Windows h5py cannot truncate an open file in write mode.
        # The temp file will be closed before h5py truncates it
        # and will be removed during the tearDown step.
        self.path = self.test_temp_file.name

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


class TestNoCacheSpec(unittest.TestCase):

    def setUp(self):
        self.manager = _get_manager()
        self.test_temp_file = tempfile.NamedTemporaryFile()
        self.test_temp_file.close()
        # On Windows h5py cannot truncate an open file in write mode.
        # The temp file will be closed before h5py truncates it
        # and will be removed during the tearDown step.
        self.path = self.test_temp_file.name

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

        with File(self.test_temp_file.name) as f:
            self.assertNotIn('specifications', f)


class HDF5IOMultiFileTest(unittest.TestCase):
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
        f1 = File(self.test_temp_files[0])
        self.assertIsInstance(f1.get('/buckets/test_bucket1/foo_holder/foo1/my_data', getlink=True), HardLink)
        # Confirm that we successfully created and External Link in our second file
        f2 = File(self.test_temp_files[1])
        self.assertIsInstance(f2.get('/buckets/test_bucket2/foo_holder/foo2/my_data', getlink=True), ExternalLink)
        # Confirm that we successfully resolved the External Link when we copied our second file
        f3 = File(self.test_temp_files[2])
        self.assertIsInstance(f3.get('/buckets/test_bucket2/foo_holder/foo2/my_data', getlink=True), HardLink)


class HDF5IOInitNoFileTest(unittest.TestCase):
    """ Test if file does not exist, init with mode (r, r+) throws error, all others succeed """

    def test_init_no_file_r(self):
        self.path = "test_init_nofile_r.h5"
        with self.assertRaisesRegex(UnsupportedOperation,
                                    r"Unable to open file %s in 'r' mode\. File does not exist\."
                                    % re.escape(self.path)):
            HDF5IO(self.path, mode='r')

    def test_init_no_file_rplus(self):
        self.path = "test_init_nofile_rplus.h5"
        with self.assertRaisesRegex(UnsupportedOperation,
                                    r"Unable to open file %s in 'r\+' mode\. File does not exist\."
                                    % re.escape(self.path)):
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


class HDF5IOInitFileExistsTest(unittest.TestCase):
    """ Test if file exists, init with mode w-/x throws error, all others succeed """

    def setUp(self):
        # On Windows h5py cannot truncate an open file in write mode.
        # The temp file will be closed before h5py truncates it
        # and will be removed during the tearDown step.
        temp_file = tempfile.NamedTemporaryFile()
        temp_file.close()
        self.path = temp_file.name
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
        with self.assertRaisesRegex(UnsupportedOperation,
                                    r"Unable to open file %s in 'w-' mode\. File already exists\."
                                    % re.escape(self.path)):
            self.io = HDF5IO(self.path, mode='w-')

    def test_init_x_file_exists(self):
        with self.assertRaisesRegex(UnsupportedOperation,
                                    r"Unable to open file %s in 'x' mode\. File already exists\."
                                    % re.escape(self.path)):
            self.io = HDF5IO(self.path, mode='x')

    def test_init_file_exists_ok(self):
        # test that no errors are thrown
        modes = ('r', 'r+', 'w', 'a')
        for m in modes:
            with HDF5IO(self.path, mode=m):
                pass


class HDF5IOReadNoDataTest(unittest.TestCase):
    """ Test if file exists and there is no data, read with mode (r, r+, a) throws error """

    def setUp(self):
        # On Windows h5py cannot truncate an open file in write mode.
        # The temp file will be closed before h5py truncates it
        # and will be removed during the tearDown step.
        temp_file = tempfile.NamedTemporaryFile()
        temp_file.close()
        self.path = temp_file.name
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
        with self.assertRaisesRegex(UnsupportedOperation,
                                    r"Cannot read data from file %s in mode 'r'\. There are no values\."
                                    % re.escape(self.path)):
            self.io.read()

    def test_read_no_data_rplus(self):
        self.io = HDF5IO(self.path, mode='r+')
        with self.assertRaisesRegex(UnsupportedOperation,
                                    r"Cannot read data from file %s in mode 'r\+'\. There are no values\."
                                    % re.escape(self.path)):
            self.io.read()

    def test_read_no_data_a(self):
        self.io = HDF5IO(self.path, mode='a')
        with self.assertRaisesRegex(UnsupportedOperation,
                                    r"Cannot read data from file %s in mode 'a'\. There are no values\."
                                    % re.escape(self.path)):
            self.io.read()


class HDF5IOReadData(unittest.TestCase):
    """ Test if file exists and there is no data, read in mode (r, r+, a) is ok
    and read in mode w throws error
    """

    def setUp(self):
        temp_file = tempfile.NamedTemporaryFile()
        temp_file.close()
        self.path = temp_file.name
        # On Windows h5py cannot truncate an open file in write mode.
        # The temp file will be closed before h5py truncates it
        # and will be removed during the tearDown step.
        foo1 = Foo('foo1', [0, 1, 2, 3, 4], "I am foo1", 17, 3.14)
        bucket1 = FooBucket('test_bucket1', [foo1])
        self.foofile1 = FooFile('test_foofile1', buckets=[bucket1])

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
            with self.assertRaisesRegex(UnsupportedOperation,
                                        r"Cannot read from file %s in mode 'w'. Please use mode 'r', 'r\+', or 'a'\."
                                        % re.escape(self.path)):
                read_foofile1 = io.read()
                self.assertListEqual(self.foofile1.buckets[0].foos[0].my_data,
                                     read_foofile1.buckets[0].foos[0].my_data[:].tolist())


class HDF5IOWriteNoFile(unittest.TestCase):
    """ Test if file does not exist, write in mode (w, w-, x, a) is ok """

    def setUp(self):
        foo1 = Foo('foo1', [0, 1, 2, 3, 4], "I am foo1", 17, 3.14)
        bucket1 = FooBucket('test_bucket1', [foo1])
        self.foofile1 = FooFile('test_foofile1', buckets=[bucket1])
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


class HDF5IOWriteFileExists(unittest.TestCase):
    """ Test if file exists, write in mode (r+, w, a) is ok and write in mode r throws error """

    def setUp(self):
        temp_file = tempfile.NamedTemporaryFile()
        temp_file.close()
        self.path = temp_file.name

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
            with self.assertRaisesRegex(ValueError, r"Unable to create group \(name already exists\)"):
                io.write(self.foofile2)

    def test_write_a(self):
        with HDF5IO(self.path, manager=_get_manager(), mode='a') as io:
            # even though foofile1 and foofile2 have different names, writing a
            # root object into a file that already has a root object, in r+ mode
            # should throw an error
            with self.assertRaisesRegex(ValueError, r"Unable to create group \(name already exists\)"):
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
            with self.assertRaisesRegex(UnsupportedOperation,
                                        (r"Cannot write to file %s in mode 'r'\. " +
                                            r"Please use mode 'r\+', 'w', 'w-', 'x', or 'a'")
                                        % re.escape(self.path)):
                io.write(self.foofile2)


class H5DataIOValid(unittest.TestCase):

    def setUp(self):
        temp_file = tempfile.NamedTemporaryFile()
        temp_file.close()
        self.paths = [temp_file.name, ]

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

            temp_file = tempfile.NamedTemporaryFile()
            temp_file.close()
            self.paths.append(temp_file.name)

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

        with self.assertRaisesRegex(InvalidDataIOError, r"Cannot get length of data\. Data is not valid\."):
            len(self.foo2.my_data)

        with self.assertRaisesRegex(InvalidDataIOError, r"Cannot get attribute 'shape' of data\. Data is not valid\."):
            self.foo2.my_data.shape

        with self.assertRaisesRegex(InvalidDataIOError, r"Cannot convert data to array\. Data is not valid\."):
            np.array(self.foo2.my_data)

        with self.assertRaisesRegex(InvalidDataIOError, r"Cannot iterate on data\. Data is not valid\."):
            for i in self.foo2.my_data:
                pass

        with self.assertRaisesRegex(InvalidDataIOError, r"Cannot iterate on data\. Data is not valid\."):
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


if __name__ == '__main__':
    unittest.main()
