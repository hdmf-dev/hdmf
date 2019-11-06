import unittest

from hdmf.spec import GroupSpec, DatasetSpec, DimSpec, SpecCatalog, SpecNamespace, NamespaceCatalog
from hdmf.build import ObjectMapper, TypeMap, GroupBuilder, DatasetBuilder, BuildManager
from hdmf import Container, docval, getargs

from tests.unit.test_utils import CORE_NAMESPACE


class Bar(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Bar'},
            {'name': 'data1', 'type': ('data', 'array_data'), 'doc': 'some data'},
            {'name': 'data2', 'type': ('data', 'array_data'), 'doc': 'more data'})
    def __init__(self, **kwargs):
        name, data1, data2 = getargs('name', 'data1', 'data2', kwargs)
        super(Bar, self).__init__(name=name)
        self.__data1 = data1
        self.__data2 = data2

    def __eq__(self, other):
        attrs = ('name', 'data1', 'data2')
        return all(getattr(self, a) == getattr(other, a) for a in attrs)

    def __str__(self):
        attrs = ('name', 'data1', 'data2')
        return ','.join('%s=%s' % (a, getattr(self, a)) for a in attrs)

    @property
    def data_type(self):
        return 'Bar'

    @property
    def data1(self):
        return self.__data1

    @property
    def data2(self):
        return self.__data2


class TestMapSimple(unittest.TestCase):

    def customSetUp(self, bar_spec):
        spec_catalog = SpecCatalog()
        spec_catalog.register_spec(bar_spec, 'test.yaml')
        namespace = SpecNamespace('a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}], catalog=spec_catalog)
        namespace_catalog = NamespaceCatalog()
        namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)
        type_map = TypeMap(namespace_catalog)
        type_map.register_container_type(CORE_NAMESPACE, 'Bar', Bar)
        type_map.register_map(Bar, ObjectMapper)
        return type_map

    def test_build_dims(self):
        dimspec = DimSpec(label='my_label', coord='data2', dimtype='coord')
        dset1_spec = DatasetSpec('an example dataset1', 'int', name='data1', shape=(None,), dims=(dimspec,))
        dset2_spec = DatasetSpec('an example dataset2', 'text', name='data2', shape=(None,))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset2_spec])
        type_map = self.customSetUp(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4], ['a', 'b', 'c', 'd'])
        builder = type_map.build(bar_inst)

        self.assertEqual(builder.get('data1').dims['my_label'], builder.get('data2'))
        self.assertEqual(builder.get('data2').dims, dict())

    def test_build_dims_unknown_name(self):
        dimspec = DimSpec(label='my_label', coord='data3', dimtype='coord')
        dset1_spec = DatasetSpec('an example dataset1', 'int', name='data1', shape=(None,), dims=(dimspec,))
        dset2_spec = DatasetSpec('an example dataset2', 'text', name='data2', shape=(None,))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset2_spec])
        type_map = self.customSetUp(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4], ['a', 'b', 'c', 'd'])
        msg = "Dimension coord 'data3' for spec 'data1' not found in group 'my_bar'"
        with self.assertRaisesRegex(ValueError, msg):
            type_map.build(bar_inst)

    def test_construct_dims(self):
        dimspec = DimSpec(label='my_label', coord='data2', dimtype='coord')
        dset1_spec = DatasetSpec('an example dataset1', 'int', name='data1', shape=(None,), dims=(dimspec,))
        dset2_spec = DatasetSpec('an example dataset2', 'text', name='data2', shape=(None,))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset2_spec])
        type_map = self.customSetUp(bar_spec)
        manager = BuildManager(type_map)

        bar_inst = Bar('my_bar', [1, 2, 3, 4], ['a', 'b', 'c', 'd'])
        builder = type_map.build(bar_inst)

        dset_builder2 = DatasetBuilder(name='data2', data=['a', 'b', 'c', 'd'])
        dset_builder1 = DatasetBuilder(name='data1', data=[1, 2, 3, 4], dims={'my_label': dset_builder2})
        datasets = {'data1': dset_builder1, 'data2': dset_builder2}
        attributes = {'data_type': 'Bar', 'namespace': CORE_NAMESPACE, 'object_id': bar_inst.object_id}
        builder_expected = GroupBuilder('my_bar', datasets=datasets, attributes=attributes)
        self.assertEqual(builder, builder_expected)

        container = type_map.construct(builder_expected, manager)
        self.assertEqual(container, bar_inst)
