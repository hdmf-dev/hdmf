import unittest2 as unittest
import os
import numpy as np
import shutil
from six import text_type
import zarr
try:
    from numcodecs import Blosc, Delta
    DISABLE_ZARR_COMPRESSION_TESTS = False
except ImportError:
    DISABLE_ZARR_COMPRESSION_TESTS = True

from hdmf.spec.namespace import NamespaceCatalog
from hdmf.build import GroupBuilder, DatasetBuilder, ReferenceBuilder  # , LinkBuilder
from hdmf.backends.zarr import ZarrIO
from hdmf.backends.zarr import ZarrDataIO
from tests.unit.test_io_hdf5_h5tools import _get_manager, FooFile
from hdmf.data_utils import DataChunkIterator

from tests.unit.utils import Foo, FooBucket, CacheSpecTestHelper


class GroupBuilderTestCase(unittest.TestCase):
    '''
    A TestCase class for comparing GroupBuilders.
    '''

    def __is_scalar(self, obj):
        if hasattr(obj, 'shape'):
            return len(obj.shape) == 0
        else:
            if any(isinstance(obj, t) for t in (int, str, float, bytes, text_type)):
                return True
        return False

    # def __convert_h5_scalar(self, obj):
    #    if isinstance(obj, Dataset):
    #        return obj[...]
    #    return obj

    def __compare_attr_dicts(self, a, b):
        reasons = list()
        b_keys = set(b.keys())
        for k in a:
            if k not in b:
                reasons.append("'%s' attribute missing from second dataset" % k)
            else:
                if a[k] != b[k]:
                    reasons.append("'%s' attribute on datasets not equal" % k)
                b_keys.remove(k)
        for k in b_keys:
            reasons.append("'%s' attribute missing from first dataset" % k)
        return reasons

    def __compare_data(self, a, b):
        return False

    def __compare_dataset(self, a, b):
        attrs = [dict(a.attrs), dict(b.attrs)]
        reasons = self.__compare_attr_dicts(attrs[0], attrs[1])
        if not self.__compare_data(a.data, b.data):
            reasons.append("dataset '%s' not equal" % a.name)
        return reasons


class TestZarrWriter(unittest.TestCase):
    """Test writing of builder with Zarr"""

    def setUp(self):
        self.manager = _get_manager()
        self.path = "test_io.zarr"

    def tearDown(self):
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

    def createGroupBuilder(self):
        self.foo_builder = GroupBuilder('foo1',
                                        attributes={'data_type': 'Foo',
                                                    'namespace': 'test_core',
                                                    'attr1': 17.5},
                                        datasets={'my_data': self.__dataset_builder})
        # self.foo = Foo('foo1', self.__dataset_builder.data, attr1="bar", attr2=17, attr3=3.14)
        # self.manager.prebuilt(self.foo, self.foo_builder)
        self.builder = GroupBuilder(
            'root',
            source=self.path,
            groups={'test_bucket':
                    GroupBuilder('test_bucket',
                                 groups={'foo_holder':
                                         GroupBuilder('foo_holder',
                                                      groups={'foo1': self.foo_builder})})},
            attributes={'data_type': 'FooFile'})

    def getReferenceBuilder(self):
        data_1 = np.arange(100, 200, 10).reshape(2, 5)
        data_2 = np.arange(0, 200, 10).reshape(4, 5)
        dataset_1 = DatasetBuilder('dataset_1', data_1)
        dataset_2 = DatasetBuilder('dataset_2', data_2)

        ref_dataset_1 = ReferenceBuilder(dataset_1)
        ref_dataset_2 = ReferenceBuilder(dataset_2)
        ref_data = [ref_dataset_1, ref_dataset_2]
        dataset_ref = DatasetBuilder('ref_dataset', ref_data, dtype='object')

        builder = GroupBuilder('root',
                               source=self.path,
                               datasets={'dataset_1': dataset_1,
                                         'dataset_2': dataset_2,
                                         'ref_dataset': dataset_ref})
        return builder

    def getReferenceCompoundBuilder(self):
        data_1 = np.arange(100, 200, 10).reshape(2, 5)
        data_2 = np.arange(0, 200, 10).reshape(4, 5)
        dataset_1 = DatasetBuilder('dataset_1', data_1)
        dataset_2 = DatasetBuilder('dataset_2', data_2)

        ref_dataset_1 = ReferenceBuilder(dataset_1)
        ref_dataset_2 = ReferenceBuilder(dataset_2)
        ref_data = [
            (1, 'dataset_1', ref_dataset_1),
            (2, 'dataset_2', ref_dataset_2)
        ]
        ref_data_type = [{'name': 'id', 'dtype': 'int'},
                         {'name': 'name', 'dtype': str},
                         {'name': 'reference', 'dtype': 'object'}]
        dataset_ref = DatasetBuilder('ref_dataset', ref_data, dtype=ref_data_type)
        builder = GroupBuilder('root',
                               source=self.path,
                               datasets={'dataset_1': dataset_1,
                                         'dataset_2': dataset_2,
                                         'ref_dataset': dataset_ref})
        return builder

    def read_test_dataset(self):
        reader = ZarrIO(self.path, manager=self.manager, mode='r')
        self.root = reader.read_builder()
        dataset = self.root['test_bucket/foo_holder/foo1/my_data']
        return dataset

    def read(self):
        reader = ZarrIO(self.path, manager=self.manager, mode='r')
        self.root = reader.read_builder()

    def test_cache_spec(self):

        self.io = ZarrIO(self.path, manager=self.manager, mode='w')

        # Setup all the data we need
        foo1 = Foo('foo1', [0, 1, 2, 3, 4], "I am foo1", 17, 3.14)
        foo2 = Foo('foo2', [5, 6, 7, 8, 9], "I am foo2", 34, 6.28)
        foobucket = FooBucket('test_bucket', [foo1, foo2])
        foofile = FooFile([foobucket])

        # Write the first file
        self.io.write(foofile, cache_spec=True)
        self.io.close()

        # Load the spec and assert that it is valid
        ns_catalog = NamespaceCatalog()
        ZarrIO.load_namespaces(ns_catalog, self.path)
        self.assertEqual(ns_catalog.namespaces, ('test_core',))
        source_types = CacheSpecTestHelper.get_types(self.io.manager.namespace_catalog)
        read_types = CacheSpecTestHelper.get_types(ns_catalog)
        self.assertSetEqual(source_types, read_types)

    def test_write_int(self, test_data=None):
        data = np.arange(100, 200, 10).reshape(2, 5) if test_data is None else test_data
        self.__dataset_builder = DatasetBuilder('my_data', data, attributes={'attr2': 17})
        self.createGroupBuilder()
        writer = ZarrIO(self.path, manager=self.manager, mode='a')
        writer.write_builder(self.builder)
        writer.close()

    def test_write_compound(self, test_data=None):
        """
        :param test_data: Optional list of the form [(1, 'STR1'), (2, 'STR2')], i.e., a list of tuples where
                          each tuple consists of an int and a string
        :return:
        """
        data = [(1, 'Allen'),
                (2, 'Bob'),
                (3, 'Mike'),
                (4, 'Jenny')] if test_data is None else test_data
        data_type = [{'name': 'id', 'dtype': 'int'},
                     {'name': 'name', 'dtype': 'str'}]
        self.__dataset_builder = DatasetBuilder('my_data', data, dtype=data_type)
        self.createGroupBuilder()
        writer = ZarrIO(self.path, manager=self.manager, mode='a')
        writer.write_builder(self.builder)
        writer.close()

    def test_write_chunk(self, test_data=None):
        data = np.arange(100, 200, 10).reshape(2, 5) if test_data is None else test_data
        data_io = ZarrDataIO(data=data, chunks=(1, 5), fillvalue=-1)
        self.__dataset_builder = DatasetBuilder('my_data', data_io, attributes={'attr2': 17})
        self.createGroupBuilder()
        writer = ZarrIO(self.path, manager=self.manager, mode='a')
        writer.write_builder(self.builder)
        writer.close()

    def test_write_strings(self, test_data=None):
        data = [['a', 'aa', 'aaa', 'aaaa', 'aaaaa'],
                ['b', 'bb', 'bbb', 'bbbb', 'bbbbb']] if test_data is None else test_data
        self.__dataset_builder = DatasetBuilder('my_data', data, attributes={'attr2': 17})
        self.createGroupBuilder()
        writer = ZarrIO(self.path, manager=self.manager, mode='a')
        writer.write_builder(self.builder)
        writer.close()

    def test_write_links(self, test_data=None):
        data = np.arange(100, 200, 10).reshape(2, 5) if test_data is None else test_data
        self.__dataset_builder = DatasetBuilder('my_data', data, attributes={'attr2': 17})
        self.createGroupBuilder()
        link_parent = self.builder['test_bucket']
        link_parent.add_link(self.foo_builder, 'my_link')
        link_parent.add_link(self.__dataset_builder, 'my_dataset')
        writer = ZarrIO(self.path, manager=self.manager, mode='a')
        writer.write_builder(self.builder)
        writer.close()

    def test_write_link_array(self):
        data = np.arange(100, 200, 10).reshape(2, 5)
        self.__dataset_builder = DatasetBuilder('my_data', data, attributes={'attr2': 17})
        self.createGroupBuilder()
        writer = ZarrIO(self.path, manager=self.manager, mode='a')
        writer.write_builder(self.builder)
        zarr_array = zarr.open(self.path+"/test_bucket/foo_holder/foo1/my_data", mode='r')
        link_io = ZarrDataIO(data=zarr_array, link_data=True)
        link_dataset = DatasetBuilder('dataset_link', link_io)
        self.builder['test_bucket'].set_dataset(link_dataset)
        writer.write_builder(self.builder)
        writer.close()

        reader = ZarrIO(self.path, manager=self.manager, mode='r')
        self.root = reader.read_builder()
        read_link = self.root['test_bucket/dataset_link']
        read_link_data = read_link['builder']['data'][:]
        self.assertTrue(np.all(data == read_link_data))

    def test_write_reference(self):
        builder = self.getReferenceBuilder()
        writer = ZarrIO(self.path,
                        manager=self.manager,
                        mode='a')
        writer.write_builder(builder)
        writer.close()

    def test_write_reference_compound(self):
        builder = self.getReferenceCompoundBuilder()
        writer = ZarrIO(self.path, manager=self.manager, mode='a')
        writer.write_builder(builder)
        writer.close()

    def test_read_int(self):
        test_data = np.arange(100, 200, 10).reshape(5, 2)
        self.test_write_int(test_data=test_data)
        dataset = self.read_test_dataset()['data'][:]
        self.assertTrue(np.all(test_data == dataset))

    def test_read_chunk(self):
        test_data = np.arange(100, 200, 10).reshape(5, 2)
        self.test_write_chunk(test_data=test_data)
        dataset = self.read_test_dataset()['data'][:]
        self.assertTrue(np.all(test_data == dataset))

    def test_read_strings(self):
        test_data = [['a1', 'aa2', 'aaa3', 'aaaa4', 'aaaaa5'],
                     ['b1', 'bb2', 'bbb3', 'bbbb4', 'bbbbb5']]
        self.test_write_strings(test_data=test_data)
        dataset = self.read_test_dataset()['data'][:]
        self.assertTrue(np.all(np.asarray(test_data) == dataset))

    def test_read_compound(self):
        test_data = [(1, 'Allen1'),
                     (2, 'Bob1'),
                     (3, 'Mike1')]
        self.test_write_compound(test_data=test_data)
        dataset = self.read_test_dataset()['data']
        self.assertTupleEqual(test_data[0], tuple(dataset[0]))
        self.assertTupleEqual(test_data[1], tuple(dataset[1]))
        self.assertTupleEqual(test_data[2], tuple(dataset[2]))

    def test_read_link(self):
        test_data = np.arange(100, 200, 10).reshape(5, 2)
        self.test_write_links(test_data=test_data)
        self.read()
        link_data = self.root['test_bucket'].links['my_dataset'].builder.data[()]
        self.assertTrue(np.all(np.asarray(test_data) == link_data))
        # print(self.root['test_bucket'].links['my_dataset'].builder.data[()])

    def test_read_link_buf(self):
        data = np.arange(100, 200, 10).reshape(2, 5)
        self.__dataset_builder = DatasetBuilder('my_data', data, attributes={'attr2': 17})
        self.createGroupBuilder()
        link_parent_1 = self.builder['test_bucket']
        link_parent_2 = self.builder['test_bucket/foo_holder']
        link_parent_1.add_link(self.__dataset_builder, 'my_dataset_1')
        link_parent_2.add_link(self.__dataset_builder, 'my_dataset_2')
        writer = ZarrIO(self.path, manager=self.manager, mode='a')
        writer.write_builder(self.builder)
        writer.close()
        self.read()
        self.assertTrue(self.root['test_bucket'].links['my_dataset_1'].builder ==
                        self.root['test_bucket/foo_holder'].links['my_dataset_2'].builder)

    def test_read_reference(self):
        self.test_write_reference()
        self.read()
        builder = self.getReferenceBuilder()['ref_dataset']
        read_builder = self.root['ref_dataset']
        # Load the linked arrays and confirm we get the same data as we had in the original builder
        for i, v in enumerate(read_builder['data']):
            self.assertTrue(np.all(builder['data'][i]['builder']['data'] == v['data'][:]))

    def test_read_reference_compound(self):
        self.test_write_reference_compound()
        self.read()
        builder = self.getReferenceCompoundBuilder()['ref_dataset']
        read_builder = self.root['ref_dataset']
        # Load the elements of each entry in the compound dataset and compar the index, string, and referenced array
        for i, v in enumerate(read_builder['data']):
            self.assertEqual(v[0], builder['data'][i][0])  # Compare index value from compound tuple
            self.assertEqual(v[1], builder['data'][i][1])  # Compare string value from compound tuple
            self.assertTrue(np.all(v[2]['data'][:] == builder['data'][i][2]['builder']['data'][:]))  # Compare ref array
        # print(read_builder)

    def test_read_reference_compound_buf(self):
        data_1 = np.arange(100, 200, 10).reshape(2, 5)
        data_2 = np.arange(0, 200, 10).reshape(4, 5)
        dataset_1 = DatasetBuilder('dataset_1', data_1)
        dataset_2 = DatasetBuilder('dataset_2', data_2)

        # ref_dataset_1 = ReferenceBuilder(dataset_1)
        # ref_dataset_2 = ReferenceBuilder(dataset_2)
        ref_data = [
            (1, 'dataset_1', ReferenceBuilder(dataset_1)),
            (2, 'dataset_2', ReferenceBuilder(dataset_2)),
            (3, 'dataset_3', ReferenceBuilder(dataset_1)),
            (4, 'dataset_4', ReferenceBuilder(dataset_2))
        ]
        ref_data_type = [{'name': 'id', 'dtype': 'int'},
                         {'name': 'name', 'dtype': str},
                         {'name': 'reference', 'dtype': 'object'}]
        dataset_ref = DatasetBuilder('ref_dataset', ref_data, dtype=ref_data_type)
        builder = GroupBuilder('root',
                               source=self.path,
                               datasets={'dataset_1': dataset_1,
                                         'dataset_2': dataset_2,
                                         'ref_dataset': dataset_ref})
        writer = ZarrIO(self.path, manager=self.manager, mode='a')
        writer.write_builder(builder)
        writer.close()

        self.read()
        self.assertFalse(self.root["ref_dataset"].data[0][2] == self.root['ref_dataset'].data[1][2])
        self.assertTrue(self.root["ref_dataset"].data[0][2] == self.root['ref_dataset'].data[2][2])
        #  print(self.root['ref_dataset'])


# TODO Port tests from H5IOTest to here. We have copied the test cases in comments here but they are not all working yet
class TestZarrWriteUnit(unittest.TestCase):
    """
    Unit test for individul write functions
    """
    def setUp(self):
        self.path = "test_io.zarr"
        self.io = ZarrIO(self.path, mode='w')
        self.f = self.io._ZarrIO__file

    def tearDown(self):
        if os.path.exists(self.path):
            shutil.rmtree(self.path)

    #############################################
    #  ZarrDataIO general
    #############################################
    def test_h5dataio_array_conversion_numpy(self):
        # Test that ZarrDataIO.__array__ is working when wrapping an ndarray
        test_speed = np.array([10., 20.])
        data = ZarrDataIO((test_speed))
        self.assertTrue(np.all(np.isfinite(data)))  # Force call of ZarrDataIO.__array__

    def test_h5dataio_array_conversion_list(self):
        # Test that ZarrDataIO.__array__ is working when wrapping a python list
        test_speed = [10., 20.]
        data = ZarrDataIO(test_speed)
        self.assertTrue(np.all(np.isfinite(data)))  # Force call of ZarrDataIO.__array__

    def test_h5dataio_array_conversion_datachunkiterator(self):
        # Test that ZarrDataIO.__array__ is working when wrapping a python list
        test_speed = DataChunkIterator(data=[10., 20.])
        data = ZarrDataIO(test_speed)
        with self.assertRaises(NotImplementedError):
            np.isfinite(data)  # Force call of H5DataIO.__array__

    ##########################################
    #  write_dataset tests: scalars
    ##########################################
    def test_write_dataset_scalar(self):
        a = 10
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', a, attributes={}))
        dset = self.f['test_dataset']
        self.assertTupleEqual(dset.shape, (1,))
        self.assertEqual(dset[()], a)

    def test_write_dataset_string(self):
        a = 'test string'
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', a, attributes={}))
        dset = self.f['test_dataset']
        self.assertTupleEqual(dset.shape, (1,))
        self.assertEqual(dset[()], a)

    ##########################################
    #  write_dataset tests: lists
    ##########################################
    def test_write_dataset_list(self):
        a = np.arange(30).reshape(5, 2, 3)
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', a.tolist(), attributes={}))
        dset = self.f['test_dataset']
        self.assertTrue(np.all(dset[:] == a))

    def test_write_dataset_list_chunked(self):
        a = ZarrDataIO(np.arange(30).reshape(5, 2, 3),
                       chunks=(1, 1, 3))
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', a, attributes={}))
        dset = self.f['test_dataset']
        self.assertTrue(np.all(dset[:] == a.data))
        self.assertEqual(dset.chunks, (1, 1, 3))

    def test_write_dataset_list_fillvalue(self):
        a = ZarrDataIO(np.arange(20).reshape(5, 4), fillvalue=-1)
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', a, attributes={}))
        dset = self.f['test_dataset']
        self.assertTrue(np.all(dset[:] == a.data))
        self.assertEqual(dset.fill_value, -1)

    @unittest.skipIf(DISABLE_ZARR_COMPRESSION_TESTS, 'Skip test due to numcodec compressor not available')
    def test_write_dataset_list_compress(self):
        compressor = Blosc(cname='zstd', clevel=3, shuffle=Blosc.BITSHUFFLE)
        a = ZarrDataIO(np.arange(30).reshape(5, 2, 3),
                       compressor=compressor)
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', a, attributes={}))
        dset = self.f['test_dataset']
        self.assertTrue(np.all(dset[:] == a.data))
        self.assertTrue(dset.compressor == compressor)

    @unittest.skipIf(DISABLE_ZARR_COMPRESSION_TESTS, 'Skip test due to numcodec compressor not available')
    def test_write_dataset_list_compress_and_filter(self):
        compressor = Blosc(cname='zstd', clevel=3, shuffle=Blosc.BITSHUFFLE)
        filters = [Delta(dtype='i4')]
        a = ZarrDataIO(np.arange(30, dtype='i4').reshape(5, 2, 3),
                       compressor=compressor,
                       filters=filters)
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', a, attributes={}))
        dset = self.f['test_dataset']
        self.assertTrue(np.all(dset[:] == a.data))
        self.assertTrue(dset.compressor == compressor)
        self.assertListEqual(dset.filters, filters)

    def test_write_dataset_list_enable_default_compress(self):
        """Default compression is not supported, but we test the error message"""
        with self.assertRaises(TypeError):
            _ = ZarrDataIO(np.arange(30).reshape(5, 2, 3),
                           compressor=True)

    ##########################################
    #  write_dataset tests: Iterable
    ##########################################
    def test_write_dataset_iterable(self):
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', range(10), attributes={}))
        dset = self.f['test_dataset']
        self.assertListEqual(dset[:].tolist(), list(range(10)))

    """
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
    """

    """
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
    """

    """
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
    """

    """
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
    """
