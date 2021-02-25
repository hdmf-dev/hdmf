import h5py
import numpy as np
import os

from hdmf import Container, Data
from hdmf.backends.hdf5 import H5DataIO
from hdmf.build import (GroupBuilder, DatasetBuilder, ObjectMapper, BuildManager, TypeMap, ReferenceBuilder,
                        ReferenceTargetNotBuiltError)
from hdmf.data_utils import DataChunkIterator
from hdmf.spec import (AttributeSpec, DatasetSpec, DtypeSpec, GroupSpec, SpecCatalog, SpecNamespace, NamespaceCatalog,
                       RefSpec)
from hdmf.spec.spec import ZERO_OR_MANY
from hdmf.testing import TestCase
from hdmf.utils import docval, getargs, call_docval_func

from tests.unit.utils import Foo, CORE_NAMESPACE, create_test_type_map


class Baz(Data):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Baz'},
            {'name': 'data', 'type': (list, h5py.Dataset, 'data', 'array_data'), 'doc': 'some data'},
            {'name': 'baz_attr', 'type': str, 'doc': 'an attribute'})
    def __init__(self, **kwargs):
        name, data, baz_attr = getargs('name', 'data', 'baz_attr', kwargs)
        super().__init__(name=name, data=data)
        self.__baz_attr = baz_attr

    @property
    def baz_attr(self):
        return self.__baz_attr


class BazHolder(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Baz'},
            {'name': 'bazs', 'type': list, 'doc': 'some Baz data', 'default': list()})
    def __init__(self, **kwargs):
        name, bazs = getargs('name', 'bazs', kwargs)
        super().__init__(name=name)
        self.__bazs = {b.name: b for b in bazs}  # note: collections of groups are unordered in HDF5
        for b in bazs:
            b.parent = self

    @property
    def bazs(self):
        return self.__bazs


class TestDataMap(TestCase):

    def setUp(self):
        baz_spec = self.setUpSpec()
        self.type_map = create_test_type_map([baz_spec], {'Baz': Baz})
        self.spec_catalog = self.type_map.namespace_catalog.get_namespace(CORE_NAMESPACE).catalog
        self.manager = BuildManager(self.type_map)
        self.mapper = ObjectMapper(baz_spec)

    def setUpSpec(self):
        spec = DatasetSpec(
            doc='an Baz type',
            dtype='int',
            name='MyBaz',
            data_type_def='Baz',
            shape=[None],
            attributes=[AttributeSpec('baz_attr', 'an example string attribute', 'text')]
        )
        return spec

    def test_build(self):
        ''' Test default mapping functionality when no attributes are nested '''
        container = Baz('MyBaz', list(range(10)), 'abcdefghijklmnopqrstuvwxyz')
        builder = self.mapper.build(container, self.manager)
        expected = DatasetBuilder('MyBaz', list(range(10)), attributes={'baz_attr': 'abcdefghijklmnopqrstuvwxyz'})
        self.assertBuilderEqual(builder, expected)

    def test_build_empty_data(self):
        """Test building of a Data object with empty data."""
        baz_inc_spec = DatasetSpec(doc='doc', data_type_inc='Baz', quantity=ZERO_OR_MANY)
        baz_holder_spec = GroupSpec(doc='doc', data_type_def='BazHolder', datasets=[baz_inc_spec])
        self.spec_catalog.register_spec(baz_holder_spec, 'test.yaml')
        self.type_map.register_container_type(CORE_NAMESPACE, 'BazHolder', BazHolder)
        holder_mapper = ObjectMapper(baz_holder_spec)

        baz = Baz('MyBaz', [], 'abcdefghijklmnopqrstuvwxyz')
        holder = BazHolder('holder', [baz])

        builder = holder_mapper.build(holder, self.manager)
        expected = GroupBuilder(
            name='holder',
            datasets=[DatasetBuilder(
                name='MyBaz',
                data=[],
                attributes={'baz_attr': 'abcdefghijklmnopqrstuvwxyz',
                            'data_type': 'Baz',
                            'namespace': 'test_core',
                            'object_id': baz.object_id}
                )]
        )
        self.assertBuilderEqual(builder, expected)

    def test_append(self):
        with h5py.File('test.h5', 'w') as file:
            test_ds = file.create_dataset('test_ds', data=[1, 2, 3], chunks=True, maxshape=(None,))
            container = Baz('MyBaz', test_ds, 'abcdefghijklmnopqrstuvwxyz')
            container.append(4)
            np.testing.assert_array_equal(container[:], [1, 2, 3, 4])
        os.remove('test.h5')

    def test_extend(self):
        with h5py.File('test.h5', 'w') as file:
            test_ds = file.create_dataset('test_ds', data=[1, 2, 3], chunks=True, maxshape=(None,))
            container = Baz('MyBaz', test_ds, 'abcdefghijklmnopqrstuvwxyz')
            container.extend([4, 5])
            np.testing.assert_array_equal(container[:], [1, 2, 3, 4, 5])
        os.remove('test.h5')


class BazScalar(Data):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this BazScalar'},
            {'name': 'data', 'type': int, 'doc': 'some data'})
    def __init__(self, **kwargs):
        call_docval_func(super().__init__, kwargs)


class TestDataMapScalar(TestCase):

    def setUp(self):
        baz_scalar_spec = self.setUpSpec()
        self.type_map = create_test_type_map([baz_scalar_spec], {'BazScalar': BazScalar})
        self.spec_catalog = self.type_map.namespace_catalog.get_namespace(CORE_NAMESPACE).catalog
        self.manager = BuildManager(self.type_map)
        self.mapper = ObjectMapper(baz_scalar_spec)

    def setUpSpec(self):
        spec = DatasetSpec(
            doc='a BazScalar type',
            dtype='int',
            name='MyBaz',
            data_type_def='BazScalar'
        )
        return spec

    def test_construct_scalar_dataset(self):
        """Test constructing a Data object with an h5py.Dataset with shape (1, ) for scalar spec."""
        with h5py.File('test.h5', 'w') as file:
            test_ds = file.create_dataset('test_ds', data=[1])
            expected = BazScalar(
                name='MyBaz',
                data=1,
            )
            builder = DatasetBuilder(
                name='MyBaz',
                data=test_ds,
                attributes={'data_type': 'BazScalar',
                            'namespace': CORE_NAMESPACE,
                            'object_id': expected.object_id},
            )
            container = self.mapper.construct(builder, self.manager)
            self.assertTrue(np.issubdtype(type(container.data), np.integer))  # as opposed to h5py.Dataset
            self.assertContainerEqual(container, expected)
        os.remove('test.h5')


class BazScalarCompound(Data):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this BazScalar'},
            {'name': 'data', 'type': 'array_data', 'doc': 'some data'})
    def __init__(self, **kwargs):
        call_docval_func(super().__init__, kwargs)


class TestDataMapScalarCompound(TestCase):

    def setUp(self):
        baz_scalar_cpd_spec = self.setUpSpec()
        self.type_map = create_test_type_map([baz_scalar_cpd_spec], {'BazScalarCompound': BazScalarCompound})
        self.spec_catalog = self.type_map.namespace_catalog.get_namespace(CORE_NAMESPACE).catalog
        self.manager = BuildManager(self.type_map)
        self.mapper = ObjectMapper(baz_scalar_cpd_spec)

    def setUpSpec(self):
        spec = DatasetSpec(
            doc='a BazScalarCompound type',
            dtype=[
                DtypeSpec(
                    name='id',
                    dtype='uint64',
                    doc='The unique identifier in this table.'
                ),
                DtypeSpec(
                    name='attr1',
                    dtype='text',
                    doc='A text attribute.'
                ),
            ],
            name='MyBaz',
            data_type_def='BazScalarCompound',
        )
        return spec

    def test_construct_scalar_compound_dataset(self):
        """Test construct on a compound h5py.Dataset with shape (1, ) for scalar spec does not resolve the data."""
        with h5py.File('test.h5', 'w') as file:
            comp_type = np.dtype([('id', np.uint64), ('attr1', h5py.special_dtype(vlen=str))])
            test_ds = file.create_dataset(
                name='test_ds',
                data=np.array((1, 'text'), dtype=comp_type),
                shape=(1, ),
                dtype=comp_type
            )
            expected = BazScalarCompound(
                name='MyBaz',
                data=(1, 'text'),
            )
            builder = DatasetBuilder(
                name='MyBaz',
                data=test_ds,
                attributes={'data_type': 'BazScalarCompound',
                            'namespace': CORE_NAMESPACE,
                            'object_id': expected.object_id},
            )
            container = self.mapper.construct(builder, self.manager)
            self.assertEqual(type(container.data), h5py.Dataset)
            self.assertContainerEqual(container, expected)
        os.remove('test.h5')


class BuildDatasetOfReferencesMixin:

    def setUp(self):
        baz_spec = self.setUpSpec()
        foo_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Foo',
            datasets=[
                DatasetSpec(name='my_data', doc='an example dataset', dtype='int')
            ],
            attributes=[
                AttributeSpec(name='attr1', doc='an example string attribute', dtype='text'),
                AttributeSpec(name='attr2', doc='an example int attribute', dtype='int'),
                AttributeSpec(name='attr3', doc='an example float attribute', dtype='float')
            ]
        )

        self.type_map = create_test_type_map([baz_spec, foo_spec], {'Baz': Baz, 'Foo': Foo})
        self.spec_catalog = self.type_map.namespace_catalog.get_namespace(CORE_NAMESPACE).catalog
        self.manager = BuildManager(self.type_map)


class TestBuildUntypedDatasetOfReferences(BuildDatasetOfReferencesMixin, TestCase):

    def setUpSpec(self):
        spec = DatasetSpec(
            doc='a list of references to Foo objects',
            dtype=None,
            name='MyBaz',
            shape=[None],
            data_type_def='Baz',
            attributes=[AttributeSpec('baz_attr', 'an example string attribute', 'text')]
        )
        return spec

    def test_build(self):
        ''' Test default mapping functionality when no attributes are nested '''
        foo = Foo('my_foo1', [1, 2, 3], 'string', 10)
        baz = Baz('MyBaz', [foo, None], 'abcdefghijklmnopqrstuvwxyz')
        foo_builder = self.manager.build(foo)
        baz_builder = self.manager.build(baz, root=True)
        expected = DatasetBuilder('MyBaz', [ReferenceBuilder(foo_builder), None],
                                  attributes={'baz_attr': 'abcdefghijklmnopqrstuvwxyz',
                                              'data_type': 'Baz',
                                              'namespace': CORE_NAMESPACE,
                                              'object_id': baz.object_id})
        self.assertBuilderEqual(baz_builder, expected)


class TestBuildCompoundDatasetOfReferences(BuildDatasetOfReferencesMixin, TestCase):

    def setUpSpec(self):
        spec = DatasetSpec(
            doc='a list of references to Foo objects',
            dtype=[
                DtypeSpec(
                    name='id',
                    dtype='uint64',
                    doc='The unique identifier in this table.'
                ),
                DtypeSpec(
                    name='foo',
                    dtype=RefSpec('Foo', 'object'),
                    doc='The foo in this table.'
                ),
            ],
            name='MyBaz',
            shape=[None],
            data_type_def='Baz',
            attributes=[AttributeSpec('baz_attr', 'an example string attribute', 'text')]
        )
        return spec

    def test_build(self):
        ''' Test default mapping functionality when no attributes are nested '''
        foo = Foo('my_foo1', [1, 2, 3], 'string', 10)
        baz = Baz('MyBaz', [(1, foo)], 'abcdefghijklmnopqrstuvwxyz')
        foo_builder = self.manager.build(foo)
        baz_builder = self.manager.build(baz, root=True)
        expected = DatasetBuilder('MyBaz', [(1, ReferenceBuilder(foo_builder))],
                                  attributes={'baz_attr': 'abcdefghijklmnopqrstuvwxyz',
                                              'data_type': 'Baz',
                                              'namespace': CORE_NAMESPACE,
                                              'object_id': baz.object_id})
        self.assertBuilderEqual(baz_builder, expected)


class TestBuildTypedDatasetOfReferences(BuildDatasetOfReferencesMixin, TestCase):

    def setUpSpec(self):
        spec = DatasetSpec(
            doc='a list of references to Foo objects',
            dtype=RefSpec('Foo', 'object'),
            name='MyBaz',
            shape=[None],
            data_type_def='Baz',
            attributes=[AttributeSpec('baz_attr', 'an example string attribute', 'text')]
        )
        return spec

    def test_build(self):
        ''' Test default mapping functionality when no attributes are nested '''
        foo = Foo('my_foo1', [1, 2, 3], 'string', 10)
        baz = Baz('MyBaz', [foo], 'abcdefghijklmnopqrstuvwxyz')
        foo_builder = self.manager.build(foo)
        baz_builder = self.manager.build(baz, root=True)
        expected = DatasetBuilder('MyBaz', [ReferenceBuilder(foo_builder)],
                                  attributes={'baz_attr': 'abcdefghijklmnopqrstuvwxyz',
                                              'data_type': 'Baz',
                                              'namespace': CORE_NAMESPACE,
                                              'object_id': baz.object_id})
        self.assertBuilderEqual(baz_builder, expected)


class TestBuildDatasetOfReferencesUnbuiltTarget(BuildDatasetOfReferencesMixin, TestCase):

    def setUpSpec(self):
        spec = DatasetSpec(
            doc='a list of references to Foo objects',
            dtype=None,
            name='MyBaz',
            shape=[None],
            data_type_def='Baz',
            attributes=[AttributeSpec('baz_attr', 'an example string attribute', 'text')]
        )
        return spec

    def test_build(self):
        ''' Test default mapping functionality when no attributes are nested '''
        foo = Foo('my_foo1', [1, 2, 3], 'string', 10)
        baz = Baz('MyBaz', [foo], 'abcdefghijklmnopqrstuvwxyz')
        msg = "MyBaz (MyBaz): Could not find already-built Builder for Foo 'my_foo1' in BuildManager"
        with self.assertRaisesWith(ReferenceTargetNotBuiltError, msg):
            self.manager.build(baz, root=True)


class BazContainer(Container):
    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Baz'},
            {'name': 'baz', 'type': Data, 'doc': 'some data'})
    def __init__(self, **kwargs):
        name, baz = getargs('name', 'baz', kwargs)
        super().__init__(name=name)
        self.baz = baz


class StringBaz(Data):
    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Baz'},
            {'name': 'data', 'type': str, 'doc': 'some data'},
            {'name': 'baz_attr', 'type': str, 'doc': 'an attribute'})
    def __init__(self, **kwargs):
        name, data, baz_attr = getargs('name', 'data', 'baz_attr', kwargs)
        super().__init__(name=name, data=data)
        self.baz_attr = baz_attr


class UntypedDataContainer(Container):
    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Baz'},
            {'name': 'test_str_dataset', 'type': str, 'doc': 'a test string'},
            {'name': 'test_str_attr', 'type': str, 'doc': 'a test string', 'default': None},)
    def __init__(self, **kwargs):
        name, test_str_dataset, test_str_attr = getargs('name', 'test_str_dataset', 'test_str_attr', kwargs)
        super().__init__(name=name)
        self.test_str_dataset = test_str_dataset
        self.test_str_attr = test_str_attr


class TestGetAttrValueConvertString(TestCase):
    """Test getting an attribute from a container that needs to be converted to str/bytes/datetime."""

    def map_get_attr_value(self, dataset_spec):
        inc_spec = DatasetSpec('included dataset', name='baz', data_type_inc='StringBaz')

        container_spec = GroupSpec(
            doc='a group',
            data_type_def='BazContainer',
            datasets=[inc_spec]
        )

        container_classes = {'BazContainer': BazContainer,
                             'StringBaz': StringBaz}
        type_map = create_test_type_map([container_spec, dataset_spec], container_classes)
        manager = BuildManager(type_map)
        mapper = ObjectMapper(container_spec)

        baz = StringBaz(name='MyBaz', data='value', baz_attr='my attr')
        container = BazContainer('MyBazContainer', baz)

        # TODO problem is that inc_spec has no dtype so it does not trigger convert_string
        # NamespaceCatalog.load_namespaces() needs to be called in order to resolve the spec
        ret = mapper.get_attr_value(inc_spec, container, manager)
        return ret

    def test_dataset_attribute_text(self):
        spec = DatasetSpec(
            doc='a dataset',
            dtype='text',
            data_type_def='StringBaz',
            attributes=[AttributeSpec('baz_attr', 'an example string attribute', 'text')]
        )
        ret = self.map_get_attr_value(spec)

        # inc_spec maps to an instance of StringBaz, so ret should be an instance of StringBaz and its data should
        # not be converted as it is already text
        self.assertIsInstance(ret.data, str)
        self.assertIsInstance(ret.baz_attr, str)

    # see TODO above in map_get_attr_value
    # def test_dataset_attribute_ascii(self):
    #     spec = DatasetSpec(
    #         doc='a dataset',
    #         dtype='ascii',  # <--
    #         data_type_def='StringBaz',
    #         attributes=[AttributeSpec('baz_attr', 'an example string attribute', 'ascii')]
    #     )
    #     ret = self.map_get_attr_value(spec)
    #
    #     # inc_spec says that the data should have dtype ascii and the attribute should have dtype ascii
    #     # the mapper should convert these within get_attr_value
    #     self.assertIsInstance(ret.data, bytes)
    #     self.assertIsInstance(ret.baz_attr, bytes)

    def test_untyped_dataset_attribute_text(self):
        dataset_spec = DatasetSpec(
            name='test_str_dataset',
            doc='a dataset',
            dtype='text'
        )
        attribute_spec = AttributeSpec(
            name='test_str_attr',
            doc='an attribute',
            dtype='text'
        )
        container_spec = GroupSpec(
            doc='a group',
            data_type_def='BazContainer',
            datasets=[dataset_spec],
            attributes=[attribute_spec]
        )

        container_classes = {'BazContainer': BazContainer}
        type_map = create_test_type_map([container_spec], container_classes)
        manager = BuildManager(type_map)
        mapper = ObjectMapper(container_spec)

        container = UntypedDataContainer('MyBazContainer', test_str_dataset='value', test_str_attr='value')
        ret = mapper.get_attr_value(dataset_spec, container, manager)
        self.assertIsInstance(ret, str)
        ret = mapper.get_attr_value(attribute_spec, container, manager)
        self.assertIsInstance(ret, str)

    def test_untyped_dataset_attribute_ascii(self):
        dataset_spec = DatasetSpec(
            name='test_str_dataset',
            doc='a dataset',
            dtype='ascii'  # <--
        )
        attribute_spec = AttributeSpec(
            name='test_str_attr',
            doc='an attribute',
            dtype='ascii'  # <--
        )
        container_spec = GroupSpec(
            doc='a group',
            data_type_def='BazContainer',
            datasets=[dataset_spec],
            attributes=[attribute_spec]
        )

        container_classes = {'BazContainer': BazContainer}
        type_map = create_test_type_map([container_spec], container_classes)
        manager = BuildManager(type_map)
        mapper = ObjectMapper(container_spec)

        container = UntypedDataContainer('MyBazContainer', test_str_dataset='value', test_str_attr='value')
        ret = mapper.get_attr_value(dataset_spec, container, manager)
        self.assertIsInstance(ret, bytes)
        ret = mapper.get_attr_value(attribute_spec, container, manager)
        self.assertIsInstance(ret, bytes)


class TestDataIOEdgeCases(TestCase):

    def setUp(self):
        self.setUpBazSpec()
        self.spec_catalog = SpecCatalog()
        self.spec_catalog.register_spec(self.baz_spec, 'test.yaml')
        self.namespace = SpecNamespace('a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}],
                                       version='0.1.0',
                                       catalog=self.spec_catalog)
        self.namespace_catalog = NamespaceCatalog()
        self.namespace_catalog.add_namespace(CORE_NAMESPACE, self.namespace)
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Baz', Baz)
        self.type_map.register_map(Baz, ObjectMapper)
        self.manager = BuildManager(self.type_map)
        self.mapper = ObjectMapper(self.baz_spec)

    def setUpBazSpec(self):
        self.baz_spec = DatasetSpec(
            doc='an Baz type',
            dtype=None,
            name='MyBaz',
            data_type_def='Baz',
            shape=[None],
            attributes=[AttributeSpec('baz_attr', 'an example string attribute', 'text')]
        )

    def test_build_dataio(self):
        """Test building of a dataset with data_type and no dtype with value DataIO."""
        container = Baz('my_baz', H5DataIO(['a', 'b', 'c', 'd'], chunks=True), 'value1')
        builder = self.type_map.build(container)
        self.assertIsInstance(builder.get('data'), H5DataIO)

    def test_build_datachunkiterator(self):
        """Test building of a dataset with data_type and no dtype with value DataChunkIterator."""
        container = Baz('my_baz', DataChunkIterator(['a', 'b', 'c', 'd']), 'value1')
        builder = self.type_map.build(container)
        self.assertIsInstance(builder.get('data'), DataChunkIterator)

    def test_build_dataio_datachunkiterator(self):  # hdmf#512
        """Test building of a dataset with no dtype and no data_type with value DataIO wrapping a DCI."""
        container = Baz('my_baz', H5DataIO(DataChunkIterator(['a', 'b', 'c', 'd']), chunks=True), 'value1')
        builder = self.type_map.build(container)
        self.assertIsInstance(builder.get('data'), H5DataIO)
        self.assertIsInstance(builder.get('data').data, DataChunkIterator)
