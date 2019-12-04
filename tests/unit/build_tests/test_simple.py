import unittest

from hdmf.spec import GroupSpec, DatasetSpec, CoordSpec, SpecCatalog, SpecNamespace, NamespaceCatalog, DimSpec
from hdmf.build import ObjectMapper, TypeMap, GroupBuilder, DatasetBuilder, BuildManager
from hdmf import Container
from hdmf.testing import TestCase

from tests.unit.utils import CORE_NAMESPACE


class Bar(Container):

    __fields__ = ('data1', 'data2', 'data3')

    def __init__(self, name, data1, data2, data3=None):
        super().__init__(name=name)
        self.data1 = data1
        self.data2 = data2
        self.data3 = data3

    def __eq__(self, other):
        return (self.name == other.name and
                self.fields == other.fields and
                self.dim_coords == other.dim_coords)


def _create_typemap(bar_spec):
    spec_catalog = SpecCatalog()
    spec_catalog.register_spec(bar_spec, 'test.yaml')
    namespace = SpecNamespace('a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}], catalog=spec_catalog)
    namespace_catalog = NamespaceCatalog()
    namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)
    type_map = TypeMap(namespace_catalog)
    type_map.register_container_type(CORE_NAMESPACE, 'Bar', Bar)
    type_map.register_map(Bar, ObjectMapper)
    return type_map


class TestMapDims(TestCase):

    def test_build_no_dims(self):
        """
        Test that given a Spec for an Container class (Bar) with no DimSpec, the DatasetBuilder dims is None.
        """
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1')
        dset2_spec = DatasetSpec(doc='an example dataset2', dtype='text', name='data2')
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset2_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4], ['a', 'b', 'c', 'd'])
        builder = type_map.build(bar_inst)

        self.assertIsNone(builder.get('data1').dims)

    def test_build_dims_1d(self):
        """
        Test that given a Spec for an Container class (Bar) that includes a DimSpec, the type map can create
        a builder from an instance of the Container, with dimensions. Start with the simple case of a 1-D array.
        """
        dim_spec = DimSpec(name='x', required=True)
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1', dims=(dim_spec, ))
        dset2_spec = DatasetSpec(doc='an example dataset2', dtype='text', name='data2')
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset2_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4], ['a', 'b', 'c', 'd'])
        builder = type_map.build(bar_inst)

        self.assertTupleEqual(builder.get('data1').dims,
                              ({'name': 'x', 'required': True, 'length': None, 'doc': None}, ))
        self.assertTrue(isinstance(builder.get('data1').dims[0], DimSpec))

    def test_build_dims_2d(self):
        """
        Test that given a Spec for an Container class (Bar) that includes two DimSpecs, the type map can create
        a builder from an instance of the Container, with dimensions. Here, with a 2-D dataset with dimensions
        with a length and doc.
        """
        x_spec = DimSpec(name='x', required=True, length=3)
        y_spec = DimSpec(name='y', required=False, doc='test_doc')
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1', dims=(x_spec, y_spec))
        dset2_spec = DatasetSpec('an example dataset2', 'text', name='data2')
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset2_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4], ['a', 'b', 'c', 'd'])
        builder = type_map.build(bar_inst)

        self.assertTupleEqual(builder.get('data1').dims,
                              ({'name': 'x', 'required': True, 'length': 3, 'doc': None},
                               {'name': 'y', 'required': False, 'length': None, 'doc': 'test_doc'}))
        self.assertTrue(isinstance(builder.get('data1').dims[0], DimSpec))
        self.assertTrue(isinstance(builder.get('data1').dims[1], DimSpec))


class TestMapCoords(TestCase):

    def test_build_coords(self):
        """
        Test that given a DimSpec and CoordSpec for an Container class, the type map can create a builder from an
        instance of the Container, with dimensions and coordinates. Start with the simple case of a 1-D array.
        """
        # TODO handle multiple dims, shapes, and coords
        dim_spec = DimSpec(name='x', required=True)
        coord_spec = CoordSpec(name='letters', coord_dataset='data2', coord_axes=(0, ), dims=('x', ),
                               coord_type='aligned')
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1',
                                 dims=(dim_spec, ), coords=(coord_spec, ))
        dset2_spec = DatasetSpec('an example dataset2', 'text', name='data2')
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset2_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4], ['a', 'b', 'c', 'd'])
        builder = type_map.build(bar_inst)

        self.assertTupleEqual(builder.get('data1').coords,
                              ({'name': 'letters', 'coord_dataset': 'data2', 'coord_axes': (0, ), 'dims': ('x', ),
                                'coord_type': 'aligned'}, ))
        self.assertTrue(isinstance(builder.get('data1').coords[0], CoordSpec))




class TestMapSimpleOld(TestCase):

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
