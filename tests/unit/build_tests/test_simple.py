import unittest

from hdmf.spec import GroupSpec, DatasetSpec, CoordSpec, SpecCatalog, SpecNamespace, NamespaceCatalog, DimSpec
from hdmf.build import ObjectMapper, TypeMap, GroupBuilder, DatasetBuilder, BuildManager
from hdmf import Container

from tests.unit.test_utils import CORE_NAMESPACE


class Bar(Container):

    __fields__ = ('data1', 'data2', 'data3')

    def __init__(self, name, data1, data2, data3=None):
        super(Bar, self).__init__(name=name)
        self.data1 = data1
        self.data2 = data2
        self.data3 = data3

    def __eq__(self, other):
        return (self.name == other.name and
                self.fields == other.fields and
                self.dim_coords == other.dim_coords)


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

    def test_build_dims_1d(self):
        """
        Test that given a Spec for an AbstractContainer class (Bar) that includes a DimSpec, the type map can create
        a builder from an instance of the AbstractContainer, with dimensions. Start with the simple case of 1-D arrays.
        """
        dim_spec = DimSpec(name='x', required=True)
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1', dims=(dim_spec, ))
        dset2_spec = DatasetSpec(doc='an example dataset2', dtype='text', name='data2')
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset2_spec])
        type_map = self.customSetUp(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4], ['a', 'b', 'c', 'd'])
        builder = type_map.build(bar_inst)

        self.assertTupleEqual(builder.get('data1').dims,
                              ({'name': 'x', 'required': True, 'length': None, 'doc': None}, ))
        self.assertTupleEqual(builder.get('data1').shape, (None, ))

    def test_build_dims_2d(self):
        """
        Test that given a Spec for an AbstractContainer class (Bar) that includes two DimSpecs, the type map can create
        a builder from an instance of the AbstractContainer, with dimensions. Here, with a 2-D dataset with dimensions.
        """
        x_spec = DimSpec(name='x', required=True, length=3)
        y_spec = DimSpec(name='y', required=True, doc='test_doc')
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1', dims=(x_spec, y_spec))
        dset2_spec = DatasetSpec('an example dataset2', 'text', name='data2')
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset2_spec])
        type_map = self.customSetUp(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4], ['a', 'b', 'c', 'd'])
        builder = type_map.build(bar_inst)

        self.assertTupleEqual(builder.get('data1').dims,
                              ({'name': 'x', 'required': True, 'length': 3, 'doc': None},
                               {'name': 'y', 'required': True, 'length': None, 'doc': 'test_doc'}))
        self.assertTupleEqual(builder.get('data1').shape, (3, None))



class TestMapSimpleOld(unittest.TestCase):

    def test_build_coords(self):
        """Test that given a Spec for an AbstractContainer class, the type map can create a builder from an instance
        of the AbstractContainer, with dimensions and coordinates. Start with the simple use case of specs for 1-D
        arrays.
        """
        # TODO handle multiple dims, shapes, and coords
        coord_spec = CoordSpec(label='my_label', dims='x', coord='data2', type='coord')
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1', shape=(None,), dims=('x',),
                                 coords=(coord_spec,))
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
        dimspec = CoordSpec(label='my_label', coord='data3', type='coord')
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
        dimspec = CoordSpec(label='my_label', coord='data2', type='coord')
        dset1_spec = DatasetSpec('an example dataset1', 'int', name='data1', shape=(None,), dims=(dimspec,))
        dset2_spec = DatasetSpec('an example dataset2', 'text', name='data2', shape=(None,))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset2_spec])
        type_map = self.customSetUp(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4], ['a', 'b', 'c', 'd'])
        bar_inst.set_dim_coord('data1', 0, 'my_label', 'data2')

        dset_builder2 = DatasetBuilder(name='data2', data=['a', 'b', 'c', 'd'])
        dset_builder1 = DatasetBuilder(name='data1', data=[1, 2, 3, 4], dims={'my_label': dset_builder2})
        datasets = {'data1': dset_builder1, 'data2': dset_builder2}
        attributes = {'data_type': 'Bar', 'namespace': CORE_NAMESPACE, 'object_id': bar_inst.object_id}
        builder_expected = GroupBuilder('my_bar', datasets=datasets, attributes=attributes)

        # builder = type_map.build(bar_inst)
        # self.assertEqual(builder, builder_expected)

        manager = BuildManager(type_map)
        container_expected = type_map.construct(builder_expected, manager)

        self.assertEqual(container_expected, bar_inst)

    # TODO test dynamic class generation with dim coord spec
