from hdmf.spec import GroupSpec, DatasetSpec, InnerCoordSpec, CoordSpec, SpecCatalog, SpecNamespace, NamespaceCatalog
from hdmf.spec import DimSpec
from hdmf.build import ObjectMapper, TypeMap, GroupBuilder, DatasetBuilder, BuildManager, BuildError, CoordBuilder
from hdmf.build import ConstructError, ConvertError
from hdmf import Container
from hdmf.container import Coordinates
from hdmf.utils import docval
from hdmf.testing import TestCase

from tests.unit.utils import CORE_NAMESPACE


class Bar(Container):

    __fields__ = ('data1', 'data2', 'data3')

    @docval({'name': 'name', 'type': str, 'doc': 'bar name'},
            {'name': 'data1', 'type': 'array_data', 'doc': 'bar data1'},
            {'name': 'data2', 'type': 'array_data', 'doc': 'bar data2', 'default': None},
            {'name': 'data3', 'type': ('scalar_data', 'array_data'), 'doc': 'bar data3', 'default': None})
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

    # TODO legacy tests

    # TODO make hdf5 back end write dims and coords and read dims and coords
    # TODO update documentation
    # TODO do not write new attributes if dims and coords do not exist

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
        Test that given a Spec for an Container class (Bar) that includes a DimSpec with length none, the type map can
        create a builder from an instance of the Container, with dimensions.
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
        Test that given a Spec for an Container class (Bar) that includes a DimSpec with a length, when the type map
        tries to create a builder from an instance of the Container with a different length, an error is raised.
        """
        dim_spec = DimSpec(name='x', required=True, length=3)
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1', dims=(dim_spec, ))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4])

        with self.assertRaisesWith(BuildError, "Could not build 'data1' for Bar 'my_bar'."):
            with self.assertRaisesWith(ConvertError, "Data dimension 'x' must have length 3 but has length 4."):
                type_map.build(bar_inst)

    def test_build_dims_1d_opt_wrong_length(self):
        """
        Test that given a Spec for an Container class (Bar) that includes an optional DimSpec with a length, when the
        type map tries to create a builder from an instance of the Container with a different length, an error is
        raised.
        """
        dim_spec = DimSpec(name='x', required=False, length=3)
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1', dims=(dim_spec, ))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4])

        with self.assertRaisesWith(BuildError, "Could not build 'data1' for Bar 'my_bar'."):
            with self.assertRaisesWith(ConvertError, "Data dimension 'x' must have length 3 but has length 4."):
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

    def test_build_dims_2d_wrong_length(self):
        """
        Test that given a Spec for an Container class (Bar) that includes two DimSpecs, the type map can create
        a builder from an instance of the Container, with dimensions. Here, with a 2-D dataset with dimensions
        with a length and doc.
        """
        x_spec = DimSpec(name='x', required=True, length=4)
        y_spec = DimSpec(name='y', required=False, doc='test_doc', length=3)
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1', dims=(x_spec, y_spec))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [[1, 2], [3, 4], [5, 6], [7, 8]])
        with self.assertRaisesWith(BuildError, "Could not build 'data1' for Bar 'my_bar'."):
            with self.assertRaisesWith(ConvertError, "Data dimension 'y' must have length 3 but has length 4."):
                type_map.build(bar_inst)

    def test_build_dims_1d_with_2d_dims(self):
        """
        Test that given a Spec for an Container class (Bar) that includes two DimSpecs, the type map can create
        a builder from an instance of the Container, with dimensions. Here, with a 2-D dataset with dimensions
        with a length and doc.
        """
        x_spec = DimSpec(name='x', required=True, length=4)
        y_spec = DimSpec(name='y', required=False, doc='test_doc', length=3)
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
        y_spec = DimSpec(name='y', required=True, doc='test_doc', length=3)
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1', dims=(x_spec, y_spec))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4])

        with self.assertRaisesWith(BuildError, "Could not build 'data1' for Bar 'my_bar'."):
            with self.assertRaisesWith(ConvertError, "Data must have at least 2 dimensions but has 1."):
                type_map.build(bar_inst)


class TestBuildCoords(TestCase):

    def test_build_coords_1d(self):
        """
        Test that given a DimSpec and CoordSpec for an Container class, the type map can create a builder from an
        instance of the Container, with dimensions and coordinates for a 1-D array.
        """
        dim1_spec = DimSpec(name='x', required=True)
        dim2_spec = DimSpec(name='chars', required=True)
        icoord_spec = InnerCoordSpec(dataset_name='data2', dims_index=(0, ), type='aligned')
        coord_spec = CoordSpec(name='letters', dims_index=(0, ), coord=icoord_spec)
        # TODO datasetspec add_dim(...)
        # TODO datasetspec add_coord(...)
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1',
                                 dims=(dim1_spec, ), coords=(coord_spec, ))
        dset2_spec = DatasetSpec('an example dataset2', 'text', name='data2', dims=(dim2_spec, ))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset2_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4], ['a', 'b', 'c', 'd'])
        group_builder = type_map.build(bar_inst)

        expected = {'letters': CoordBuilder(name='letters', coord_dataset_name='data2', coord_axes=(0, ), axes=(0, ),
                                            coord_type='aligned')}
        self.assertEqual(group_builder.get('data1').coords, expected)

    def test_build_coords_2d(self):
        """
        Test that given a DimSpec and CoordSpec for an Container class, the type map can create a builder from an
        instance of the Container, with dimensions and coordinates for a 2-D array.
        """
        x_spec = DimSpec(name='x', required=True, length=4)
        y_spec = DimSpec(name='y', required=False, doc='test_doc')
        dim2_spec = DimSpec(name='chars', required=True)
        icoord_spec = InnerCoordSpec(dataset_name='data2', dims_index=(0, ), type='aligned')
        x_coord_spec = CoordSpec(name='xletters', dims_index=(0, ), coord=icoord_spec)
        y_coord_spec = CoordSpec(name='yletters', dims_index=(1, ), coord=icoord_spec)
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1',
                                 dims=(x_spec, y_spec), coords=(x_coord_spec, y_coord_spec))
        dset2_spec = DatasetSpec('an example dataset2', 'text', name='data2', dims=(dim2_spec, ))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset2_spec])
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [[1, 2], [3, 4], [5, 6], [7, 8]], ['a', 'b', 'c', 'd'])
        group_builder = type_map.build(bar_inst)

        expected = {'xletters': CoordBuilder(name='xletters', coord_dataset_name='data2', coord_axes=(0, ), axes=(0, ),
                                             coord_type='aligned'),
                    'yletters': CoordBuilder(name='yletters', coord_dataset_name='data2', coord_axes=(0, ), axes=(1, ),
                                             coord_type='aligned')}
        self.assertEqual(group_builder.get('data1').coords, expected)

    def test_build_coords_unknown_name(self):
        """
        Test that given a DimSpec and CoordSpec for an Container class, the type map raises an error when the CoordSpec
        references an invalid coord_dataset.
        """
        dim1_spec = DimSpec(name='x', required=True)
        dim2_spec = DimSpec(name='chars', required=True)
        # TODO require coord_dataset to be a datasetspec, validate axes
        icoord_spec = InnerCoordSpec(dataset_name='data4', dims_index=(0, ), type='aligned')
        coord_spec = CoordSpec(name='letters', dims_index=(0, ), coord=icoord_spec)
        # TODO validate coord type is an allowed value
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1',
                                 dims=(dim1_spec, ), coords=(coord_spec, ))
        # TODO constructor, and add coords validate axes.
        dset2_spec = DatasetSpec('an example dataset2', 'text', name='data2', dims=(dim2_spec, ))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset2_spec])
        # TODO on write to yaml, validate that coord references exist.
        type_map = _create_typemap(bar_spec)
        bar_inst = Bar('my_bar', [1, 2, 3, 4], ['a', 'b', 'c', 'd'])

        with self.assertRaisesWith(BuildError, "Could not build 'data1' for Bar 'my_bar'."):
            with self.assertRaisesWith(ConvertError, "Coord dataset 'data4' of coord 'letters' does not exist."):
                type_map.build(bar_inst)


class TestConstructDims(TestCase):

    # NOTE: if a DatasetBuilder does not match its DatasetSpec in dtype or dims/shape, the object can still be
    # constructed

    def test_construct_dims_1d(self):
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
        manager = BuildManager(type_map)

        dset_builder1 = DatasetBuilder(name='data1', data=[1, 2, 3, 4])
        datasets = [dset_builder1, ]
        attributes = {'data_type': 'Bar', 'namespace': CORE_NAMESPACE, 'object_id': "doesn't matter"}
        group_builder = GroupBuilder('my_bar', datasets=datasets, attributes=attributes)

        constructed_bar = type_map.construct(group_builder, manager)
        self.assertEqual(constructed_bar.dims, {'data1': ('x', )})

        expected_bar = Bar('my_bar', [1, 2, 3, 4])
        self.assertContainerEqual(constructed_bar, expected_bar, ignore_hdmf_attrs=True)

    def test_construct_dims_1d_length_none(self):
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
        manager = BuildManager(type_map)

        dset_builder1 = DatasetBuilder(name='data1', data=[1, 2, 3, 4])
        datasets = [dset_builder1, ]
        attributes = {'data_type': 'Bar', 'namespace': CORE_NAMESPACE, 'object_id': "doesn't matter"}
        group_builder = GroupBuilder('my_bar', datasets=datasets, attributes=attributes)

        constructed_bar = type_map.construct(group_builder, manager)
        self.assertEqual(constructed_bar.dims, {'data1': ('x', )})

        expected_bar = Bar('my_bar', [1, 2, 3, 4])
        self.assertContainerEqual(constructed_bar, expected_bar, ignore_hdmf_attrs=True)

    def test_construct_dims_1d_wrong_length(self):
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
        manager = BuildManager(type_map)

        dset_builder1 = DatasetBuilder(name='data1', data=[1, 2, 3, 4])
        datasets = [dset_builder1, ]
        attributes = {'data_type': 'Bar', 'namespace': CORE_NAMESPACE, 'object_id': "doesn't matter"}
        group_builder = GroupBuilder('my_bar', datasets=datasets, attributes=attributes)

        # on read - warnings instead of errors, same for dtype
        with self.assertRaisesWith(ConstructError, "Could not construct 'data1' for Bar 'my_bar'."):
            with self.assertRaisesWith(ConvertError, "Data dimension 'x' must have length 3 but has length 4."):
                type_map.construct(group_builder, manager)

    def test_construct_dims_2d(self):
        """
        Test that given a Spec for an Container class (Bar) that includes a DimSpec, the type map can create
        a builder from an instance of the Container, with dimensions for a 2-D array.
        """
        x_spec = DimSpec(name='x', required=True)
        y_spec = DimSpec(name='y', required=True)
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1', dims=(x_spec, y_spec))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec])
        type_map = _create_typemap(bar_spec)
        manager = BuildManager(type_map)

        dset_builder1 = DatasetBuilder(name='data1', data=[[1, 2, 3, 4], [5, 6, 7, 8]])
        datasets = [dset_builder1, ]
        attributes = {'data_type': 'Bar', 'namespace': CORE_NAMESPACE, 'object_id': "doesn't matter"}
        group_builder = GroupBuilder('my_bar', datasets=datasets, attributes=attributes)

        constructed_bar = type_map.construct(group_builder, manager)
        self.assertEqual(constructed_bar.dims, {'data1': ('x', 'y')})

        expected_bar = Bar('my_bar', [[1, 2, 3, 4], [5, 6, 7, 8]])
        self.assertContainerEqual(constructed_bar, expected_bar, ignore_hdmf_attrs=True)


class TestConstructCoords(TestCase):

    def test_construct_coords_1d(self):
        x_spec = DimSpec(name='x', required=True)
        char_spec = DimSpec(name='chars', required=True)
        icoord_spec = InnerCoordSpec(dataset_name='data2', dims_index=(0, ), type='aligned')
        coord_spec = CoordSpec(name='letters', dims_index=(0, ), coord=icoord_spec)
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1',
                                 dims=(x_spec, ), coords=(coord_spec, ))
        dset2_spec = DatasetSpec('an example dataset2', 'text', name='data2', dims=(char_spec, ))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset2_spec])
        type_map = _create_typemap(bar_spec)
        manager = BuildManager(type_map)

        # on read, the dataset builders will not have dims or coords. these need to be added in construct
        # TODO builder SHOULD know dims and coords from the backend
        # warning on read if dims and coords do not match spec
        dset_builder2 = DatasetBuilder(name='data2', data=['a', 'b', 'c', 'd'])
        dset_builder1 = DatasetBuilder(name='data1', data=[1, 2, 3, 4])
        datasets = {'data1': dset_builder1, 'data2': dset_builder2}
        attributes = {'data_type': 'Bar', 'namespace': CORE_NAMESPACE, 'object_id': "doesn't matter"}
        group_builder = GroupBuilder('my_bar', datasets=datasets, attributes=attributes)

        constructed_bar = type_map.construct(group_builder, manager)

        expected_coords = Coordinates(constructed_bar)
        expected_coords.add(name='letters', dims=('x', ), coord_array=constructed_bar.data2, coord_array_axes=(0, ),
                            coord_type='aligned')
        self.assertEqual(constructed_bar.coords, {'data1': expected_coords})

        expected_bar = Bar('my_bar', [1, 2, 3, 4], ['a', 'b', 'c', 'd'])
        self.assertContainerEqual(constructed_bar, expected_bar, ignore_hdmf_attrs=True)

    def test_construct_coords_2d(self):
        frame_spec = DimSpec(name='frame', required=True)
        x1_spec = DimSpec(name='x1', required=True, length=2)
        y1_spec = DimSpec(name='y1', required=True, length=4)
        x2_spec = DimSpec(name='x2', required=False)
        y2_spec = DimSpec(name='y2', required=False)
        icoord_spec = InnerCoordSpec(dataset_name='data2', dims_index=(0, 1), type='aligned')
        coord_spec = CoordSpec(name='dorsal-ventral', dims_index=(1, 2), coord=icoord_spec)
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1',
                                 dims=(frame_spec, x1_spec, y1_spec), coords=(coord_spec, ))
        dset2_spec = DatasetSpec('an example dataset2', 'int', name='data2', dims=(x2_spec, y2_spec))
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset2_spec])
        type_map = _create_typemap(bar_spec)
        manager = BuildManager(type_map)

        # on read, the dataset builders will not have dims or coords. these need to be added in construct
        dset_builder2 = DatasetBuilder(name='data2', data=[[-1, -2, -3, -4], [-5, -6, -7, -8]])
        dset_builder1 = DatasetBuilder(name='data1', data=[[[1, 2, 3, 4], [5, 6, 7, 8]],
                                                           [[1, 2, 3, 4], [5, 6, 7, 8]],
                                                           [[1, 2, 3, 4], [5, 6, 7, 8]]])
        datasets = {'data1': dset_builder1, 'data2': dset_builder2}
        attributes = {'data_type': 'Bar', 'namespace': CORE_NAMESPACE, 'object_id': "doesn't matter"}
        group_builder = GroupBuilder('my_bar', datasets=datasets, attributes=attributes)

        constructed_bar = type_map.construct(group_builder, manager)

        expected_coords = Coordinates(constructed_bar)
        expected_coords.add(name='dorsal-ventral', dims=('x1', 'y1'), coord_array=constructed_bar.data2,
                            coord_array_axes=(0, 1), coord_type='aligned')
        self.assertEqual(constructed_bar.coords, {'data1': expected_coords})

        expected_bar = Bar('my_bar',
                           [[[1, 2, 3, 4], [5, 6, 7, 8]],
                            [[1, 2, 3, 4], [5, 6, 7, 8]],
                            [[1, 2, 3, 4], [5, 6, 7, 8]]],
                           [[-1, -2, -3, -4], [-5, -6, -7, -8]])
        self.assertContainerEqual(constructed_bar, expected_bar, ignore_hdmf_attrs=True)

# TODO test dynamic class generation with dim coord spec


class TestConstructCheckType(TestCase):

    def _test_construct_helper(self, spec_dtype, builder_data):
        dim_spec = DimSpec(name='x', required=True)
        dset1_spec = DatasetSpec(doc='an example dataset1', dtype='int', name='data1', dims=(dim_spec, ))  # not used
        dset3_spec = DatasetSpec(doc='an example dataset3', dtype=spec_dtype, name='data3')
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[dset1_spec, dset3_spec])
        type_map = _create_typemap(bar_spec)
        manager = BuildManager(type_map)

        dset_builder1 = DatasetBuilder(name='data1', data=[10])  # not used
        dset_builder3 = DatasetBuilder(name='data3', data=builder_data)
        datasets = {'data1': dset_builder1, 'data3': dset_builder3}
        attributes = {'data_type': 'Bar', 'namespace': CORE_NAMESPACE, 'object_id': "doesn't matter"}
        group_builder = GroupBuilder('my_bar', datasets=datasets, attributes=attributes)

        return type_map, group_builder, manager

    def test_construct_int(self):
        type_map, group_builder, manager = self._test_construct_helper('int', 10)
        type_map.construct(group_builder, manager)

    def test_construct_intstring_for_int(self):
        type_map, group_builder, manager = self._test_construct_helper('int', '10')
        bar = type_map.construct(group_builder, manager)
        self.assertEqual(bar.data3, 10)

    def test_construct_floatstring_for_int(self):
        type_map, group_builder, manager = self._test_construct_helper('int', '10.5')
        with self.assertRaisesWith(ConstructError, "Could not construct Bar object."):
            with self.assertRaisesWith(ConvertError, "Could not convert data 'data3' to dtype 'int' in spec."):
                type_map.construct(group_builder, manager)

    def test_construct_float_for_int(self):
        type_map, group_builder, manager = self._test_construct_helper('int', 10.5)
        with self.assertRaisesWith(ConstructError, "Could not construct Bar object."):
            with self.assertRaisesWith(ConvertError, "Could not convert data 'data3' to dtype 'int' in spec."):
                type_map.construct(group_builder, manager)

    def test_construct_int_list_for_int(self):
        type_map, group_builder, manager = self._test_construct_helper('int', [10])
        bar = type_map.construct(group_builder, manager)
        self.assertEqual(bar.data3, [10])

    def test_construct_text(self):
        type_map, group_builder, manager = self._test_construct_helper('text', '10')
        type_map.construct(group_builder, manager)

    def test_construct_int_for_text(self):
        type_map, group_builder, manager = self._test_construct_helper('text', 10)
        with self.assertRaisesWith(ConstructError, "Could not construct Bar object."):
            with self.assertRaisesWith(ConvertError, "Could not convert data 'data3' to dtype 'text' in spec."):
                type_map.construct(group_builder, manager)

    def test_construct_int_list_for_text(self):
        type_map, group_builder, manager = self._test_construct_helper('text', [10])
        with self.assertRaisesWith(ConstructError, "Could not construct Bar object."):
            with self.assertRaisesWith(ConvertError, "Could not convert data 'data3' to dtype 'text' in spec."):
                type_map.construct(group_builder, manager)
