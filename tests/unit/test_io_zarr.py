import unittest
import os
import numpy as np
import shutil
from six import text_type

try:
    import zarr
    from hdmf.backends.zarr.zarr_tools import ZarrIO
    from hdmf.backends.zarr.zarr_utils import ZarrDataIO
    DISABLE_ALL_ZARR_TESTS = False
except ImportError:
    DISABLE_ALL_ZARR_TESTS = True
try:
    from numcodecs import Blosc, Delta
    DISABLE_ZARR_COMPRESSION_TESTS = False
except ImportError:
    DISABLE_ZARR_COMPRESSION_TESTS = True

from hdmf.spec.namespace import NamespaceCatalog
from hdmf.build import GroupBuilder, DatasetBuilder, ReferenceBuilder, OrphanContainerBuildError
from hdmf.data_utils import DataChunkIterator
from hdmf.testing import TestCase
from hdmf.backends.io import HDMFIO, UnsupportedOperation

from tests.unit.utils import (Foo, FooBucket, FooFile, get_foo_buildmanager,
                              # Baz, BazData, BazCpdData, BazBucket, get_baz_buildmanager,
                              CacheSpecTestHelper, get_temp_filepath)


def total_directory_size(source):
    """Helper function used to compute the size of a directory"""
    dsize = os.path.getsize(source)
    for item in os.listdir(source):
        itempath = os.path.join(source, item)
        if os.path.isfile(itempath):
            dsize += os.path.getsize(itempath)
        elif os.path.isdir(itempath):
            dsize += total_directory_size(itempath)
    return dsize


class GroupBuilderTestCase(TestCase):
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


@unittest.skipIf(DISABLE_ALL_ZARR_TESTS, "Skipping TestZarrWriter because Zarr is not installed")
class TestZarrWriter(TestCase):
    """Test writing of builder with Zarr"""

    def setUp(self):
        self.manager = get_foo_buildmanager()
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
        foofile = FooFile(buckets=[foobucket])

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
                     {'name': 'name', 'dtype': str}]
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


@unittest.skipIf(DISABLE_ALL_ZARR_TESTS, "Skipping TestZarrWriteUnit because Zarr is not installed")
class TestZarrWriteUnit(TestCase):
    """
    Unit test for individual write functions
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
    def test_zarrdataio_array_conversion_numpy(self):
        # Test that ZarrDataIO.__array__ is working when wrapping an ndarray
        test_speed = np.array([10., 20.])
        data = ZarrDataIO((test_speed))
        self.assertTrue(np.all(np.isfinite(data)))  # Force call of ZarrDataIO.__array__

    def test_zarrdataio_array_conversion_list(self):
        # Test that ZarrDataIO.__array__ is working when wrapping a python list
        test_speed = [10., 20.]
        data = ZarrDataIO(test_speed)
        self.assertTrue(np.all(np.isfinite(data)))  # Force call of ZarrDataIO.__array__

    def test_zarrdataio_array_conversion_datachunkiterator(self):
        # Test that ZarrDataIO.__array__ is working when wrapping a python list
        test_speed = DataChunkIterator(data=[10., 20.])
        data = ZarrDataIO(test_speed)
        with self.assertRaises(NotImplementedError):
            np.isfinite(data)  # Force call of H5DataIO.__array__

    def test_get_builder_exists_on_disk(self):
        """Test that get_builder_exists_on_disk finds the existing builder"""
        dset_builder = DatasetBuilder('test_dataset', 10, attributes={})
        self.assertFalse(self.io.get_builder_exists_on_disk(dset_builder))  # Make sure False is returned before write
        self.io.write_dataset(self.f, dset_builder)
        self.assertTrue(self.io.get_builder_exists_on_disk(dset_builder))   # Make sure True is returned after write

    def test_get_written(self):
        """Test that get_builder_exists_on_disk finds the existing builder"""
        dset_builder = DatasetBuilder('test_dataset', 10, attributes={})
        self.assertFalse(self.io.get_written(dset_builder))  # Make sure False is returned before write
        self.io.write_dataset(self.f, dset_builder)
        self.assertTrue(self.io.get_written(dset_builder))   # Make sure True is returned after write
        self.assertTrue(self.io.get_written(dset_builder, check_on_disk=True))   # Make sure its also on disk
        # Now delete it from disk and check again
        shutil.rmtree(self.io.get_builder_disk_path(dset_builder))
        self.assertTrue(self.io.get_written(dset_builder))   # The written flag should still be true
        self.assertFalse(self.io.get_written(dset_builder, check_on_disk=True))   # But with check on disk should fail

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

    ##############################################
    #  write_dataset tests: compound data tables
    #############################################
    def test_write_structured_array_table(self):
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

    def test_write_nested_structured_array_table(self):
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
        # Test that all elements match. dset return np.void types so we just compare strings for simplicity
        for i in range(10):
            self.assertEqual(str(dset[i]), str(data[i]))

    #############################################
    #  write_dataset tests: data chunk iterator
    #############################################
    def test_write_dataset_iterable_multidimensional_array(self):
        a = np.arange(30).reshape(5, 2, 3)
        aiter = iter(a)
        daiter = DataChunkIterator.from_iterable(aiter, buffer_size=2)
        self.io.write_dataset(parent=self.f,
                              builder=DatasetBuilder('test_dataset', daiter, attributes={}))
        dset = self.f['test_dataset']
        self.assertListEqual(dset[:].tolist(), a.tolist())

    def test_write_dataset_iterable_multidimensional_array_compression(self):
        a = np.arange(30).reshape(5, 2, 3)
        aiter = iter(a)
        daiter = DataChunkIterator.from_iterable(aiter, buffer_size=2)
        compressor = Blosc(cname='zstd', clevel=3, shuffle=Blosc.BITSHUFFLE)
        wrapped_daiter = ZarrDataIO(data=daiter,
                                    compressor=compressor)
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', wrapped_daiter, attributes={}))
        dset = self.f['test_dataset']
        self.assertEqual(dset.shape, a.shape)
        self.assertListEqual(dset[:].tolist(), a.tolist())
        self.assertTrue(dset.compressor == compressor)

    def test_write_dataset_data_chunk_iterator(self):
        dci = DataChunkIterator(data=np.arange(10), buffer_size=2)
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', dci, attributes={}))
        dset = self.f['test_dataset']
        self.assertListEqual(dset[:].tolist(), list(range(10)))

    def test_write_dataset_data_chunk_iterator_with_compression(self):
        dci = DataChunkIterator(data=np.arange(10), buffer_size=2)
        compressor = Blosc(cname='zstd', clevel=3, shuffle=Blosc.BITSHUFFLE)
        wrapped_dci = ZarrDataIO(data=dci,
                                 compressor=compressor,
                                 chunks=(2,))
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', wrapped_dci, attributes={}))
        dset = self.f['test_dataset']
        self.assertListEqual(dset[:].tolist(), list(range(10)))
        self.assertTrue(dset.compressor == compressor)
        self.assertEqual(dset.chunks, (2,))

    def test_pass_through_of_recommended_chunks(self):

        class DC(DataChunkIterator):
            def recommended_chunk_shape(self):
                return (5, 1, 1)
        dci = DC(data=np.arange(30).reshape(5, 2, 3))
        compressor = Blosc(cname='zstd', clevel=3, shuffle=Blosc.BITSHUFFLE)
        wrapped_dci = ZarrDataIO(data=dci,
                                 compressor=compressor)
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', wrapped_dci, attributes={}))
        dset = self.f['test_dataset']
        self.assertEqual(dset.chunks, (5, 1, 1))
        self.assertTrue(dset.compressor == compressor)

    #############################################
    #  Copy/Link h5py.Dataset object
    #############################################
    def test_link_zarr_dataset_input(self):
        dset = DatasetBuilder('test_dataset', np.arange(10), attributes={})
        self.io.write_dataset(self.f, builder=dset)
        softlink = DatasetBuilder('test_softlink', self.f['test_dataset'], attributes={})
        self.io.write_dataset(self.f, builder=softlink)
        tempf = zarr.open(store=self.path, mode='r')
        expected_link = {'name': 'test_softlink',
                         'path': '/test_dataset',
                         'source': os.path.abspath(self.path)}
        self.assertEqual(len(tempf.attrs['zarr_link']), 1)
        self.assertDictEqual(tempf.attrs['zarr_link'][0], expected_link)

    def test_copy_zarr_dataset_input(self):
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', np.arange(10), attributes={}))
        self.io.write_dataset(self.f,
                              DatasetBuilder('test_copy', self.f['test_dataset'], attributes={}),
                              link_data=False)
        # NOTE: In HDF5 this would be a HardLink. Since Zarr does not support links, this will be a copy instead.
        self.assertListEqual(self.f['test_dataset'][:].tolist(),
                             self.f['test_copy'][:].tolist())

    def test_link_dataset_zarrdataio_input(self):
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', np.arange(10), attributes={}))
        self.io.write_dataset(self.f, DatasetBuilder('test_softlink',
                                                     ZarrDataIO(data=self.f['test_dataset'],
                                                                link_data=True),
                                                     attributes={}))
        tempf = zarr.open(store=self.path, mode='r')
        expected_link = {'name': 'test_softlink',
                         'path': '/test_dataset',
                         'source': os.path.abspath(self.path)}
        self.assertEqual(len(tempf.attrs['zarr_link']), 1)
        self.assertDictEqual(tempf.attrs['zarr_link'][0], expected_link)

    def test_copy_dataset_zarrdataio_input(self):
        self.io.write_dataset(self.f, DatasetBuilder('test_dataset', np.arange(10), attributes={}))
        self.io.write_dataset(self.f,
                              DatasetBuilder('test_copy',
                                             ZarrDataIO(data=self.f['test_dataset'],
                                                        link_data=False),  # Force dataset copy
                                             attributes={}),
                              link_data=True)  # Make sure the default behavior is set to link the data
        # NOTE: In HDF5 this would be a HardLink. Since Zarr does not support links, this will be a copy instead.
        self.assertListEqual(self.f['test_dataset'][:].tolist(),
                             self.f['test_copy'][:].tolist())

    def test_list_fill_empty(self):
        dset = self.io.__list_fill__(self.f, 'empty_dataset', [], options={'dtype': int, 'io_settings': {}})
        self.assertTupleEqual(dset.shape, (0,))

    def test_list_fill_empty_no_dtype(self):
        with self.assertRaisesRegex(Exception, r"cannot add empty_dataset to / - could not determine type"):
            self.io.__list_fill__(self.f, 'empty_dataset', [])


@unittest.skipIf(DISABLE_ALL_ZARR_TESTS, "Skipping TestExportZarrToZarr because Zarr is not installed")
class TestExportZarrToZarr(TestCase):
    """Test exporting Zarr to Zarr."""

    def setUp(self):
        self.paths = [
            get_temp_filepath(),
            get_temp_filepath(),
            get_temp_filepath(),
            get_temp_filepath()
        ]

    def tearDown(self):
        for p in self.paths:
            if os.path.exists(p):
                shutil.rmtree(p)

    def test_basic(self):
        """Test that exporting a written container works between Zarr and Zarr."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket])

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as write_io:
            write_io.write(foofile)

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='r') as read_io:
            with ZarrIO(self.paths[1], mode='w') as export_io:
                export_io.export(src_io=read_io)

        self.assertTrue(os.path.exists(self.paths[1]))
        self.assertEqual(foofile.container_source, self.paths[0])

        with ZarrIO(self.paths[1], manager=get_foo_buildmanager(), mode='r') as read_io:
            read_foofile = read_io.read()
            self.assertEqual(read_foofile.container_source, self.paths[1])
            self.assertContainerEqual(foofile, read_foofile, ignore_hdmf_attrs=True)

    def test_basic_container(self):
        """Test that exporting a written container, passing in the container arg, works."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket])

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as write_io:
            write_io.write(foofile)

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='r') as read_io:
            read_foofile = read_io.read()
            with ZarrIO(self.paths[1], mode='w') as export_io:
                export_io.export(src_io=read_io, container=read_foofile)

        self.assertTrue(os.path.exists(self.paths[1]))
        self.assertEqual(foofile.container_source, self.paths[0])

        with ZarrIO(self.paths[1], manager=get_foo_buildmanager(), mode='r') as read_io:
            read_foofile = read_io.read()
            self.assertEqual(read_foofile.container_source, self.paths[1])
            self.assertContainerEqual(foofile, read_foofile, ignore_hdmf_attrs=True)

    def test_container_part(self):
        """Test that exporting a part of a written container raises an error."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket])

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as write_io:
            write_io.write(foofile)

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='r') as read_io:
            read_foofile = read_io.read()
            with ZarrIO(self.paths[1], mode='w') as export_io:
                msg = ("The provided container must be the root of the hierarchy of the source used to read the "
                       "container.")
                with self.assertRaisesWith(ValueError, msg):
                    export_io.export(src_io=read_io, container=read_foofile.buckets['bucket1'])

    def test_container_unknown(self):
        """Test that exporting a container that did not come from the src_io object raises an error."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket])

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as write_io:
            write_io.write(foofile)

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='r') as read_io:
            with ZarrIO(self.paths[1], mode='w') as export_io:
                dummy_file = FooFile(buckets=[])
                msg = "The provided container must have been read by the provided src_io."
                with self.assertRaisesWith(ValueError, msg):
                    export_io.export(src_io=read_io, container=dummy_file)

    def test_cache_spec_disabled(self):
        """Test that exporting with cache_spec disabled works."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket])

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as write_io:
            write_io.write(foofile, cache_spec=False)

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='r') as read_io:
            read_foofile = read_io.read()

            with ZarrIO(self.paths[1], mode='w') as export_io:
                export_io.export(
                    src_io=read_io,
                    container=read_foofile,
                    cache_spec=False)
        self.assertFalse(os.path.exists(os.path.join(self.paths[1], 'specifications')))

    def test_cache_spec_enabled(self):
        """Test that exporting with cache_spec works."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket])

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as write_io:
            write_io.write(foofile)

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='r') as read_io:
            read_foofile = read_io.read()

            with ZarrIO(self.paths[1], mode='w') as export_io:
                export_io.export(
                    src_io=read_io,
                    container=read_foofile,
                    cache_spec=True)
        self.assertTrue(os.path.exists(os.path.join(self.paths[1], 'specifications')))

    def test_soft_link_group(self):
        """
        Test that exporting a written file with soft linked groups keeps links within the file." ,i.e, we have
        a group that links to a group in the same file and the new file after export should then have a link to the
        same group but in the new file """
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket], foo_link=foo1)
        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as write_io:
            write_io.write(foofile)
        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='r') as read_io:
            with ZarrIO(self.paths[1], mode='w') as export_io:
                export_io.export(src_io=read_io, write_args=dict(link_data=False))
        with ZarrIO(self.paths[1], manager=get_foo_buildmanager(), mode='r') as read_io:
            read_foofile2 = read_io.read()
            # make sure the linked group is within the same file
            self.assertEqual(read_foofile2.foo_link.container_source, self.paths[1])
            zarr_linkspec1 = zarr.open(self.paths[0])['links'].attrs.asdict()['zarr_link'][0]
            zarr_linkspec2 = zarr.open(self.paths[1])['links'].attrs.asdict()['zarr_link'][0]
            self.assertEqual(zarr_linkspec1.pop('source'), self.paths[0])
            self.assertEqual(zarr_linkspec2.pop('source'), self.paths[1])
            self.assertDictEqual(zarr_linkspec1, zarr_linkspec2)

    def test_soft_link_dataset(self):
        """Test that exporting a written file with soft linked datasets keeps links within the file."""
        """Link to a dataset in the same file should have a link to the same new dataset in the new file """
        pass  # TODO this test currently fails. It does not create a SoftLink in the original file.
        """
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket], foofile_data=foo1.my_data)

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as write_io:
            write_io.write(foofile, link_data=True)
        print ("WRITE DONE")

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='r') as read_io:
            with ZarrIO(self.paths[1], mode='w') as export_io:
                export_io.export(src_io=read_io, write_args=dict(link_data=False))

        print(zarr.open(self.paths[0]).tree())
        print(zarr.open(self.paths[1]).tree())

        with ZarrIO(self.paths[1], manager=get_foo_buildmanager(), mode='r') as read_io:
            read_foofile2 = read_io.read()

            # make sure the linked dataset is within the same file
            print(open(self.paths[1]+"/buckets/bucket1/foo_holder/foo1/.zattrs", 'r').read())
            self.assertEqual(read_foofile2.foofile_data.path, self.paths[1])
        """

    def test_external_link_group(self):
        """Test that exporting a written file with external linked groups maintains the links."""
        """External links remain"""
        pass  # TODO this test currently fails. The external link is changed to point to File 2 instead of File 1
        """
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket])

        # Create File 1 with the full data
        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as read_io:
            read_io.write(foofile)

        # Create file 2 with an external link to File 1
        manager = get_foo_buildmanager()
        with ZarrIO(self.paths[0], manager=manager, mode='r') as read_io:
            read_foofile = read_io.read()
            # make external link to existing group
            foofile2 = FooFile(foo_link=read_foofile.buckets['bucket1'].foos['foo1'])
            print("-------------------Write File 2----------------------------")
            with ZarrIO(self.paths[1], manager=manager, mode='w') as write_io:
                write_io.write(foofile2)
            self.assertDictEqual(zarr.open(self.paths[1])['links'].attrs.asdict(),
                                 {'zarr_link': [{'name': 'foo_link',
                                                 'path': '/buckets/bucket1/foo_holder/foo1',
                                                 'source': self.paths[0]}]})

        # Export File 2 to a new File 3 and make sure the external link from File 2 is being preserved
        with ZarrIO(self.paths[1], manager=get_foo_buildmanager(), mode='r') as read_io:
             with ZarrIO(self.paths[2], mode='w') as export_io:
                print("-------------------Write File 3----------------------------")
                export_io.export(src_io=read_io)

        #print()
        print(zarr.open(self.paths[1])['links'].attrs.asdict())
        print(zarr.open(self.paths[2])['links'].attrs.asdict())

        with ZarrIO(self.paths[2], manager=get_foo_buildmanager(), mode='r') as read_io:
            read_foofile2 = read_io.read()
            # make sure the linked group is read from the first file
            self.assertEqual(read_foofile2.foo_link.container_source, self.paths[0])
        """

    def test_external_link_dataset(self):
        """Test that exporting a written file with external linked datasets maintains the links."""
        pass  # TODO this test currently fails
        """
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket], foofile_data=[1, 2, 3])

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as write_io:
            write_io.write(foofile)

        manager = get_foo_buildmanager()
        with ZarrIO(self.paths[0], manager=manager, mode='r') as read_io:
            read_foofile = read_io.read()
            # make external link to existing dataset
            foofile2 = FooFile(foofile_data=read_foofile.foofile_data)

            with ZarrIO(self.paths[1], manager=manager, mode='w') as write_io:
                write_io.write(foofile2)

        with ZarrIO(self.paths[1], manager=get_foo_buildmanager(), mode='r') as read_io:
            self.ios.append(read_io)  # track IO objects for tearDown

            with ZarrIO(self.paths[2], mode='w') as export_io:
                export_io.export(src_io=read_io)

        with ZarrIO(self.paths[2], manager=get_foo_buildmanager(), mode='r') as read_io:
            self.ios.append(read_io)  # track IO objects for tearDown
            read_foofile2 = read_io.read()

            # make sure the linked dataset is read from the first file
            self.assertEqual(read_foofile2.foofile_data.file.filename, self.paths[0])
        """

    def test_external_link_link(self):
        """Test that exporting a written file with external links to external links maintains the links."""
        pass  # TODO this test currently fails
        """
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket])

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as write_io:
            write_io.write(foofile)

        manager = get_foo_buildmanager()
        with ZarrIO(self.paths[0], manager=manager, mode='r') as read_io:
            read_foofile = read_io.read()
            # make external link to existing group
            foofile2 = FooFile(foo_link=read_foofile.buckets['bucket1'].foos['foo1'])

            with ZarrIO(self.paths[1], manager=manager, mode='w') as write_io:
                write_io.write(foofile2)

        manager = get_foo_buildmanager()
        with ZarrIO(self.paths[1], manager=manager, mode='r') as read_io:
            read_foofile2 = read_io.read()
            # make external link to external link
            foofile3 = FooFile(foo_link=read_foofile2.foo_link)

            with ZarrIO(self.paths[2], manager=manager, mode='w') as write_io:
                write_io.write(foofile3)

        with ZarrIO(self.paths[2], manager=get_foo_buildmanager(), mode='r') as read_io:

            with ZarrIO(self.paths[3], mode='w') as export_io:
                export_io.export(src_io=read_io)

        with ZarrIO(self.paths[3], manager=get_foo_buildmanager(), mode='r') as read_io:
            read_foofile3 = read_io.read()

            # make sure the linked group is read from the first file
            self.assertEqual(read_foofile3.foo_link.container_source, self.paths[0])
        """

    def test_attr_reference(self):
        """Test that exporting a written file with attribute references maintains the references."""
        """Attribute with object reference needs to point to the new object in the new file"""
        pass  # TODO this test currently fails because the paths in the attribute still points to the first file
        """
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket], foo_ref_attr=foo1)

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as read_io:
            read_io.write(foofile)

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='r') as read_io:
            with ZarrIO(self.paths[1], mode='w') as export_io:
                export_io.export(src_io=read_io,  write_args=dict(link_data=False))

        #with ZarrIO(self.paths[1], manager=get_foo_buildmanager(), mode='r') as read_io:
        #    read_foofile2 = read_io.read()
            #self.assertTupleEqual(ZarrIO.get_zarr_paths(read_foofile2.foo_ref_attr.my_data),
            #                     (self.paths[1], '/buckets/bucket1/foo_holder/foo1/my_data'))
            # make sure the attribute reference resolves to the container within the same file
            #self.assertIs(read_foofile2.foo_ref_attr, read_foofile2.buckets['bucket1'].foos['foo1'])

        expected_ref = {'value': {'path': '/buckets/bucket1/foo_holder/foo1', 'source': self.paths[1]},
                        'zarr_dtype': 'object'}
        real_ref = zarr.open(self.paths[1]).attrs['foo_ref_attr']
        self.assertDictEqual(real_ref, expected_ref)
        """

    def test_pop_data(self):
        """Test that exporting a written container after removing an element from it works."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket])

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as write_io:
            write_io.write(foofile)

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='r') as read_io:
            read_foofile = read_io.read()
            read_foofile.remove_bucket('bucket1')  # remove child group

            with ZarrIO(self.paths[1], mode='w') as export_io:
                export_io.export(src_io=read_io, container=read_foofile)

        with ZarrIO(self.paths[1], manager=get_foo_buildmanager(), mode='r') as read_io:
            read_foofile2 = read_io.read()

            # make sure the read foofile has no buckets
            self.assertDictEqual(read_foofile2.buckets, {})

        # check that file size of file 2 is smaller
        dirsize1 = total_directory_size(self.paths[0])
        dirsize2 = total_directory_size(self.paths[1])
        self.assertTrue(dirsize1 > dirsize2)

    def test_pop_linked_group(self):
        """Test that exporting a written container after removing a linked element from it works."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket], foo_link=foo1)

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as write_io:
            write_io.write(foofile)

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='r') as read_io:
            read_foofile = read_io.read()
            read_foofile.buckets['bucket1'].remove_foo('foo1')  # remove child group

            with ZarrIO(self.paths[1], mode='w') as export_io:
                msg = ("links (links): Linked Foo 'foo1' has no parent. Remove the link or ensure the linked "
                       "container is added properly.")
                with self.assertRaisesWith(OrphanContainerBuildError, msg):
                    export_io.export(src_io=read_io, container=read_foofile)

    def test_append_data(self):
        """Test that exporting a written container after adding groups, links, and references to it works."""
        # TODO: This test currently fails because I do not understand how the link to my_data is expected to be
        #       created here and currently fails. I.e,. it fails in list_fill but instead we should actually
        #       create an external link instead
        pass
        """
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket])

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as write_io:
            write_io.write(foofile)

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='r') as read_io:
            read_foofile = read_io.read()

            # create a foo with link to existing dataset my_data, add the foo to new foobucket
            # this should make a soft link within the exported file
            # TODO Assigning my_data is the problem. Which in turn causes the export to fail because the Zarr
            # DataType is not being understood. This is where the External link should be cerated instead?
            foo2 = Foo('foo2', read_foofile.buckets['bucket1'].foos['foo1'].my_data, "I am foo2", 17, 3.14)
            foobucket2 = FooBucket('bucket2', [foo2])
            read_foofile.add_bucket(foobucket2)

            # also add link from foofile to new foo2 container
            read_foofile.foo_link = foo2

            # also add link from foofile to new foo2.my_data dataset which is a link to foo1.my_data dataset
            read_foofile.foofile_data = foo2.my_data

            # also add reference from foofile to new foo2
            read_foofile.foo_ref_attr = foo2

            with ZarrIO(self.paths[1], mode='w') as export_io:
                export_io.export(src_io=read_io, container=read_foofile)

        with ZarrIO(self.paths[1], manager=get_foo_buildmanager(), mode='r') as read_io:
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

        #with File(self.paths[1], 'r') as f:
        #    self.assertEqual(f['foofile_data'].file.filename, self.paths[1])
        #    self.assertIsInstance(f.attrs['foo_ref_attr'], h5py.Reference)
        """

    def test_append_external_link_data(self):
        """Test that exporting a written container after adding a link with link_data=True creates external links."""
        pass  # TODO: This test currently fails
        """
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket])

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as write_io:
            write_io.write(foofile)

        foofile2 = FooFile(buckets=[])

        with ZarrIO(self.paths[1], manager=get_foo_buildmanager(), mode='w') as write_io:
            write_io.write(foofile2)

        manager = get_foo_buildmanager()
        with ZarrIO(self.paths[0], manager=manager, mode='r') as read_io1:
            read_foofile1 = read_io1.read()

            with ZarrIO(self.paths[1], manager=manager, mode='r') as read_io2:
                read_foofile2 = read_io2.read()

                # create a foo with link to existing dataset my_data (not in same file), add the foo to new foobucket
                # this should make an external link within the exported file
                foo2 = Foo('foo2', read_foofile1.buckets['bucket1'].foos['foo1'].my_data, "I am foo2", 17, 3.14)
                foobucket2 = FooBucket('bucket2', [foo2])
                read_foofile2.add_bucket(foobucket2)

                # also add link from foofile to new foo2.my_data dataset which is a link to foo1.my_data dataset
                # this should make an external link within the exported file
                read_foofile2.foofile_data = foo2.my_data

                with ZarrIO(self.paths[2], mode='w') as export_io:
                    export_io.export(src_io=read_io2, container=read_foofile2)

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='r') as read_io1:
            self.ios.append(read_io1)  # track IO objects for tearDown
            read_foofile3 = read_io1.read()

            with ZarrIO(self.paths[2], manager=get_foo_buildmanager(), mode='r') as read_io2:
                read_foofile4 = read_io2.read()

                self.assertEqual(read_foofile4.buckets['bucket2'].foos['foo2'].my_data,
                                 read_foofile3.buckets['bucket1'].foos['foo1'].my_data)
                self.assertEqual(read_foofile4.foofile_data, read_foofile3.buckets['bucket1'].foos['foo1'].my_data)

        #with File(self.paths[2], 'r') as f:
        #    self.assertEqual(f['buckets/bucket2/foo_holder/foo2/my_data'].file.filename, self.paths[0])
        #    self.assertEqual(f['foofile_data'].file.filename, self.paths[0])
        #    self.assertIsInstance(f.get('buckets/bucket2/foo_holder/foo2/my_data', getlink=True),
        #                          h5py.ExternalLink)
        #    self.assertIsInstance(f.get('foofile_data', getlink=True), h5py.ExternalLink)
        """

    def test_append_external_link_copy_data(self):
        """Test that exporting a written container after adding a link with link_data=False copies the data."""
        pass  # TODO: This test currently fails
        """
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket])

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as write_io:
            write_io.write(foofile)

        foofile2 = FooFile(buckets=[])

        with ZarrIO(self.paths[1], manager=get_foo_buildmanager(), mode='w') as write_io:
            write_io.write(foofile2)

        manager = get_foo_buildmanager()
        with ZarrIO(self.paths[0], manager=manager, mode='r') as read_io1:
            read_foofile1 = read_io1.read()

            with ZarrIO(self.paths[1], manager=manager, mode='r') as read_io2:
                read_foofile2 = read_io2.read()

                # create a foo with link to existing dataset my_data (not in same file), add the foo to new foobucket
                # this would normally make an external link but because link_data=False, data will be copied
                foo2 = Foo('foo2', read_foofile1.buckets['bucket1'].foos['foo1'].my_data, "I am foo2", 17, 3.14)
                foobucket2 = FooBucket('bucket2', [foo2])
                read_foofile2.add_bucket(foobucket2)

                # also add link from foofile to new foo2.my_data dataset which is a link to foo1.my_data dataset
                # this would normally make an external link but because link_data=False, data will be copied
                read_foofile2.foofile_data = foo2.my_data

                with ZarrIO(self.paths[2], mode='w') as export_io:
                    export_io.export(src_io=read_io2, container=read_foofile2, write_args={'link_data': False})

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='r') as read_io1:
            read_foofile3 = read_io1.read()

            with ZarrIO(self.paths[2], manager=get_foo_buildmanager(), mode='r') as read_io2:
                read_foofile4 = read_io2.read()

                # check that file can be read
                self.assertNotEqual(read_foofile4.buckets['bucket2'].foos['foo2'].my_data,
                                    read_foofile3.buckets['bucket1'].foos['foo1'].my_data)
                self.assertNotEqual(read_foofile4.foofile_data, read_foofile3.buckets['bucket1'].foos['foo1'].my_data)
                self.assertNotEqual(read_foofile4.foofile_data, read_foofile4.buckets['bucket2'].foos['foo2'].my_data)

        # with File(self.paths[2], 'r') as f:
        #    self.assertEqual(f['buckets/bucket2/foo_holder/foo2/my_data'].file.filename, self.paths[2])
        #    self.assertEqual(f['foofile_data'].file.filename, self.paths[2])
        """

    def test_export_dset_refs(self):
        """Test that exporting a written container with a dataset of references works."""
        pass  # TODO: This test currently fails
        """
        bazs = []
        num_bazs = 10
        for i in range(num_bazs):
            bazs.append(Baz(name='baz%d' % i))
        baz_data = BazData(name='baz_data1', data=bazs)
        bucket = BazBucket(name='bucket1', bazs=bazs.copy(), baz_data=baz_data)

        with ZarrIO(self.paths[0], manager=_get_baz_manager(), mode='w') as write_io:
            write_io.write(bucket)

        with ZarrIO(self.paths[0], manager=_get_baz_manager(), mode='r') as read_io:
            read_bucket1 = read_io.read()

            # NOTE: reference IDs might be the same between two identical files
            # adding a Baz with a smaller name should change the reference IDs on export
            new_baz = Baz(name='baz000')
            read_bucket1.add_baz(new_baz)

            with ZarrIO(self.paths[1], mode='w') as export_io:
                export_io.export(src_io=read_io, container=read_bucket1)

        with ZarrIO(self.paths[1], manager=_get_baz_manager(), mode='r') as read_io:
            read_bucket2 = read_io.read()

            # remove and check the appended child, then compare the read container with the original
            read_new_baz = read_bucket2.remove_baz('baz000')
            self.assertContainerEqual(new_baz, read_new_baz, ignore_hdmf_attrs=True)

            self.assertContainerEqual(bucket, read_bucket2, ignore_name=True, ignore_hdmf_attrs=True)
            for i in range(num_bazs):
                baz_name = 'baz%d' % i
                self.assertIs(read_bucket2.baz_data.data[i], read_bucket2.bazs[baz_name])
        """

    def test_export_cpd_dset_refs(self):
        """Test that exporting a written container with a compound dataset with references works."""
        pass  # TODO: This test currently fails
        """
        bazs = []
        baz_pairs = []
        num_bazs = 10
        for i in range(num_bazs):
            b = Baz(name='baz%d' % i)
            bazs.append(b)
            baz_pairs.append((i, b))
        baz_cpd_data = BazCpdData(name='baz_cpd_data1', data=baz_pairs)
        bucket = BazBucket(name='bucket1', bazs=bazs.copy(), baz_cpd_data=baz_cpd_data)

        with ZarrIO(self.paths[0], manager=_get_baz_manager(), mode='w') as write_io:
            write_io.write(bucket)

        with ZarrIO(self.paths[0], manager=_get_baz_manager(), mode='r') as read_io:
            read_bucket1 = read_io.read()

            # NOTE: reference IDs might be the same between two identical files
            # adding a Baz with a smaller name should change the reference IDs on export
            new_baz = Baz(name='baz000')
            read_bucket1.add_baz(new_baz)

            with ZarrIO(self.paths[1], mode='w') as export_io:
                export_io.export(src_io=read_io, container=read_bucket1)

        with ZarrIO(self.paths[1], manager=_get_baz_manager(), mode='r') as read_io:
            read_bucket2 = read_io.read()

            # remove and check the appended child, then compare the read container with the original
            read_new_baz = read_bucket2.remove_baz(new_baz.name)
            self.assertContainerEqual(new_baz, read_new_baz, ignore_hdmf_attrs=True)

            self.assertContainerEqual(bucket, read_bucket2, ignore_name=True, ignore_hdmf_attrs=True)
            for i in range(num_bazs):
                baz_name = 'baz%d' % i
                self.assertEqual(read_bucket2.baz_cpd_data.data[i][0], i)
                self.assertIs(read_bucket2.baz_cpd_data.data[i][1], read_bucket2.bazs[baz_name])
        """

    def test_non_manager_container(self):
        """Test that exporting with a src_io without a manager raises an error."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket])

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as write_io:
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
            with ZarrIO(self.paths[1], mode='w') as export_io:
                msg = 'When a container is provided, src_io must have a non-None manager (BuildManager) property.'
                with self.assertRaisesWith(ValueError, msg):
                    export_io.export(src_io=read_io, container=foofile, write_args={'link_data': False})

    def test_non_Zarr_src_link_data_true(self):
        """Test that exporting with a src_io without a manager raises an error."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket])

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as write_io:
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

        with OtherIO(manager=get_foo_buildmanager()) as read_io:
            with ZarrIO(self.paths[1], mode='w') as export_io:
                msg = "Cannot export from non-Zarr backend OtherIO to Zarr with write argument link_data=True."
                with self.assertRaisesWith(UnsupportedOperation, msg):
                    export_io.export(src_io=read_io, container=foofile)

    def test_wrong_mode(self):
        """Test that exporting with a src_io without a manager raises an error."""
        foo1 = Foo('foo1', [1, 2, 3, 4, 5], "I am foo1", 17, 3.14)
        foobucket = FooBucket('bucket1', [foo1])
        foofile = FooFile(buckets=[foobucket])

        with ZarrIO(self.paths[0], manager=get_foo_buildmanager(), mode='w') as write_io:
            write_io.write(foofile)

        with ZarrIO(self.paths[0], mode='r') as read_io:
            with ZarrIO(self.paths[1], mode='a') as export_io:
                msg = "Cannot export to file %s in mode 'a'. Please use mode 'w'." % self.paths[1]
                with self.assertRaisesWith(UnsupportedOperation, msg):
                    export_io.export(src_io=read_io)
