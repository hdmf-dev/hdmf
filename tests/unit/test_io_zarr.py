import unittest2 as unittest
import os
import numpy as np
import shutil
from six import text_type
import zarr

from hdmf.build import GroupBuilder, DatasetBuilder, ReferenceBuilder  # , LinkBuilder
from hdmf.backends.zarr import ZarrIO
from hdmf.backends.zarr import ZarrDataIO
from tests.unit.test_io_hdf5_h5tools import _get_manager


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
