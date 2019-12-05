from hdmf.spec import GroupSpec, DatasetSpec, CoordSpec, SpecCatalog, SpecNamespace, NamespaceCatalog, DimSpec
from hdmf.build import ObjectMapper, TypeMap, GroupBuilder, DatasetBuilder, BuildManager, BuildException, CoordBuilder
from hdmf.build import ConstructException
from hdmf import Container
from hdmf.utils import docval
from hdmf.testing import TestCase

from tests.unit.utils import CORE_NAMESPACE


class Bar(Container):

    __fields__ = ('data1', 'data2', 'data3')

    @docval({'name': 'name', 'type': str, 'doc': 'bar name'},
            {'name': 'data1', 'type': 'array_data', 'doc': 'bar data1'},
            {'name': 'data2', 'type': 'array_data', 'doc': 'bar data2', 'default': None},
            {'name': 'data3', 'type': 'array_data', 'doc': 'bar data3', 'default': None})
    def __init__(self, **kwargs):
        super().__init__(name=kwargs['name'])
        self.data1 = kwargs['data1']
        self.data2 = kwargs['data2']
        self.data3 = kwargs['data3']


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


class TestBuildDims(TestCase):

    def test_build_no_dims(self):
        """
        Test that given a Spec for an Container class (Bar) with no DimSpec, the DatasetBuilder dims is None.
        """
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1')
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4])
        with self.assertRaisesWith(BuildException, ("Could not convert 'data1' for Bar 'my_bar'. Data must be a scalar"
                                                    " but has shape (4,).")):
            type_map.build(bar_inst)

    def test_build_dims_1d(self):
        """
        Test that given a Spec for an Container class (Bar) that includes a DimSpec, the type map can create
        a builder from an instance of the Container, with dimensions. Start with the simple case of a 1-D array.
        """
        dim_spec = DimSpec(name='x', required=True)
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1', dims=(dim_spec, ))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4])
        group_builder = type_map.build(bar_inst)

        self.assertEqual(group_builder.get('data1').dims, ('x', ))

    def test_build_dims_1d_length_none(self):
        """
        Test that given a Spec for an Container class (Bar) that includes a DimSpec, the type map can create
        a builder from an instance of the Container, with dimensions. Start with the simple case of a 1-D array.
        """
        dim_spec = DimSpec(name='x', required=True, length=None)
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1', dims=(dim_spec, ))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4])
        group_builder = type_map.build(bar_inst)

        self.assertEqual(group_builder.get('data1').dims, ('x', ))

    def test_build_dims_1d_wrong_length(self):
        """
        Test that given a Spec for an Container class (Bar) that includes a DimSpec, the type map can create
        a builder from an instance of the Container, with dimensions. Start with the simple case of a 1-D array.
        """
        dim_spec = DimSpec(name='x', required=True, length=3)
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1', dims=(dim_spec, ))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4])
        with self.assertRaisesWith(BuildException, ("Could not convert 'data1' for Bar 'my_bar'. Data dimension 'x' "
                                                    "(axis 0) must have length 3 but has length 4.")):
            type_map.build(bar_inst)

    def test_build_dims_1d_opt_wrong_length(self):
        """
        Test that given a Spec for an Container class (Bar) that includes a DimSpec, the type map can create
        a builder from an instance of the Container, with dimensions. Start with the simple case of a 1-D array.
        """
        dim_spec = DimSpec(name='x', required=False, length=3)
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1', dims=(dim_spec, ))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4])
        with self.assertRaisesWith(BuildException, ("Could not convert 'data1' for Bar 'my_bar'. Data dimension 'x' "
                                                    "(axis 0) must have length 3 but has length 4.")):
            type_map.build(bar_inst)

    def test_build_dims_2d(self):
        """
        Test that given a Spec for an Container class (Bar) that includes two DimSpecs, the type map can create
        a builder from an instance of the Container, with dimensions. Here, with a 2-D dataset with dimensions
        with a length and doc.
        """
        x_spec = DimSpec(name='x', required=True, length=4)
        y_spec = DimSpec(name='y', required=False, doc='test_doc')
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1', dims=(x_spec, y_spec))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [[1, 2], [3, 4], [5, 6], [7, 8]])
        group_builder = type_map.build(bar_inst)

        self.assertEqual(group_builder.get('data1').dims, ('x', 'y'))

    def test_build_dims_1d_with_2d_dims(self):
        """
        Test that given a Spec for an Container class (Bar) that includes two DimSpecs, the type map can create
        a builder from an instance of the Container, with dimensions. Here, with a 2-D dataset with dimensions
        with a length and doc.
        """
        x_spec = DimSpec(name='x', required=True, length=4)
        y_spec = DimSpec(name='y', required=False, doc='test_doc')
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1', dims=(x_spec, y_spec))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4])
        group_builder = type_map.build(bar_inst)

        self.assertEqual(group_builder.get('data1').dims, ('x', ))

    def test_build_dims_1d_with_2d_dims_req(self):
        """
        Test that given a Spec for an Container class (Bar) that includes two DimSpecs, the type map can create
        a builder from an instance of the Container, with dimensions. Here, with a 2-D dataset with dimensions
        with a length and doc.
        """
        x_spec = DimSpec(name='x', required=True, length=4)
        y_spec = DimSpec(name='y', required=True, doc='test_doc')
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1', dims=(x_spec, y_spec))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4])
        with self.assertRaisesWith(BuildException, ("Could not convert 'data1' for Bar 'my_bar'. Data must have at "
                                                    "least 2 dimensions but has 1.")):
            type_map.build(bar_inst)


class TestBuildCoords(TestCase):

    def test_build_coords_1d(self):
        """
        Test that given a DimSpec and CoordSpec for an Container class, the type map can create a builder from an
        instance of the Container, with dimensions and coordinates for a 1-D array.
        """
        dim1_spec = DimSpec(name='x', required=True)
        dim2_spec = DimSpec(name='chars', required=True)
        coord_spec = CoordSpec(name='letters', coord_dataset='data2', coord_axes=(0, ), dims=(0, ),
                               coord_type='aligned')
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1',
                                 dims=(dim1_spec, ), coords=(coord_spec, ))
        dset2_spec = DatasetSpec('an example dataset2', 'text', name='data2', dims=(dim2_spec, ))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset2_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4], ['a', 'b', 'c', 'd'])
        group_builder = type_map.build(bar_inst)

        expected = (CoordBuilder(name='letters', coord_dataset='data2', coord_axes=(0, ), dims=(0, ),
                                 coord_type='aligned'), )
        self.assertEqual(group_builder.get('data1').coords, expected)

    def test_build_coords_2d(self):
        """
        Test that given a DimSpec and CoordSpec for an Container class, the type map can create a builder from an
        instance of the Container, with dimensions and coordinates for a 2-D array.
        """
        x_spec = DimSpec(name='x', required=True, length=4)
        y_spec = DimSpec(name='y', required=False, doc='test_doc')
        dim2_spec = DimSpec(name='chars', required=True)
        x_coord_spec = CoordSpec(name='letters', coord_dataset='data2', coord_axes=(0, ), dims=(0, ),
                                 coord_type='aligned')
        y_coord_spec = CoordSpec(name='letters', coord_dataset='data2', coord_axes=(0, ), dims=(1, ),
                                 coord_type='aligned')
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1',
                                 dims=(x_spec, y_spec), coords=(x_coord_spec, y_coord_spec))
        dset2_spec = DatasetSpec('an example dataset2', 'text', name='data2', dims=(dim2_spec, ))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset2_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [[1, 2], [3, 4], [5, 6], [7, 8]], ['a', 'b', 'c', 'd'])
        group_builder = type_map.build(bar_inst)

        expected = (CoordBuilder(name='letters', coord_dataset='data2', coord_axes=(0, ), dims=(0, ),
                                 coord_type='aligned'),
                    CoordBuilder(name='letters', coord_dataset='data2', coord_axes=(0, ), dims=(1, ),
                                 coord_type='aligned'))
        self.assertEqual(group_builder.get('data1').coords, expected)

    def test_build_dims_unknown_name(self):
        """
        Test that given a DimSpec and CoordSpec for an Container class, the type map raises an error when the CoordSpec
        references an invalid coord_dataset.
        """
        dim1_spec = DimSpec(name='x', required=True)
        dim2_spec = DimSpec(name='chars', required=True)
        coord_spec = CoordSpec(name='letters', coord_dataset='data3', coord_axes=(0, ), dims=(0, ),
                               coord_type='aligned')
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1',
                                 dims=(dim1_spec, ), coords=(coord_spec, ))
        dset2_spec = DatasetSpec('an example dataset2', 'text', name='data2', dims=(dim2_spec, ))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset2_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4], ['a', 'b', 'c', 'd'])

        msg = "Could not convert 'data1' for Bar 'my_bar'. Coord dataset 'data3' of coord 'letters' does not exist."
        with self.assertRaisesWith(BuildException, msg):
            type_map.build(bar_inst)


class TestConstructDims(TestCase):

    def test_construct_dims(self):
        """
        Test that given a Spec for an Container class (Bar) that includes a DimSpec, the type map can create
        a builder from an instance of the Container, with dimensions. Start with the simple case of a 1-D array.
        """
        dim_spec = DimSpec(name='x', required=True)
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1', dims=(dim_spec, ))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec])
        type_map = _create_typemap(bar_spec)

        dset_builder1 = DatasetBuilder(name='data1', data=[1, 2, 3, 4], dims=('x', ))
        datasets = [dset_builder1, ]
        attributes = {'data_type': 'Bar', 'namespace': CORE_NAMESPACE, 'object_id': "doesn't matter"}
        group_builder = GroupBuilder('my_bar', datasets=datasets, attributes=attributes)

        manager = BuildManager(type_map)
        constructed_bar = type_map.construct(group_builder, manager)
        self.assertEqual(constructed_bar.dims, {'data1': ('x', )})

        expected_bar = Bar('my_bar', [1, 2, 3, 4])
        self.assertContainerEqual(constructed_bar, expected_bar)


class TestConstructCoords(TestCase):

    def test_construct_dims(self):
        dimspec = CoordSpec(label='my_label', coord='data2', type='coord')
        dset1_spec = DatasetSpec('an example dataset1', 'int', name='data1', shape=(None,), dims=(dimspec,))
        dset2_spec = DatasetSpec('an example dataset2', 'text', name='data2', shape=(None,))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset2_spec])
        type_map = _create_typemap(bar_spec)
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
