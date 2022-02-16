import os

import h5py
import numpy as np
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

from tests.unit.utils import Foo, CORE_NAMESPACE


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


class BazSpecMixin:

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
        raise NotImplementedError('Test must implement this method.')


class TestDataMap(BazSpecMixin, TestCase):

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
            dtype='int',
            name='MyBaz',
            data_type_def='Baz',
            shape=[None],
            attributes=[AttributeSpec('baz_attr', 'an example string attribute', 'text')]
        )

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
        self.holder_mapper = ObjectMapper(baz_holder_spec)

        baz = Baz('MyBaz', [], 'abcdefghijklmnopqrstuvwxyz')
        holder = BazHolder('holder', [baz])

        builder = self.holder_mapper.build(holder, self.manager)
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
        self.setUpBazSpec()
        self.spec_catalog = SpecCatalog()
        self.spec_catalog.register_spec(self.baz_spec, 'test.yaml')
        self.namespace = SpecNamespace('a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}],
                                       version='0.1.0',
                                       catalog=self.spec_catalog)
        self.namespace_catalog = NamespaceCatalog()
        self.namespace_catalog.add_namespace(CORE_NAMESPACE, self.namespace)
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'BazScalar', BazScalar)
        self.type_map.register_map(BazScalar, ObjectMapper)
        self.manager = BuildManager(self.type_map)
        self.mapper = ObjectMapper(self.baz_spec)

    def setUpBazSpec(self):
        self.baz_spec = DatasetSpec(
            doc='a BazScalar type',
            dtype='int',
            name='MyBaz',
            data_type_def='BazScalar'
        )

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
        self.setUpBazSpec()
        self.spec_catalog = SpecCatalog()
        self.spec_catalog.register_spec(self.baz_spec, 'test.yaml')
        self.namespace = SpecNamespace('a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}],
                                       version='0.1.0',
                                       catalog=self.spec_catalog)
        self.namespace_catalog = NamespaceCatalog()
        self.namespace_catalog.add_namespace(CORE_NAMESPACE, self.namespace)
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'BazScalarCompound', BazScalarCompound)
        self.type_map.register_map(BazScalarCompound, ObjectMapper)
        self.manager = BuildManager(self.type_map)
        self.mapper = ObjectMapper(self.baz_spec)

    def setUpBazSpec(self):
        self.baz_spec = DatasetSpec(
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
        self.setUpBazSpec()
        self.foo_spec = GroupSpec(
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
        self.spec_catalog = SpecCatalog()
        self.spec_catalog.register_spec(self.baz_spec, 'test.yaml')
        self.spec_catalog.register_spec(self.foo_spec, 'test.yaml')
        self.namespace = SpecNamespace('a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}],
                                       version='0.1.0',
                                       catalog=self.spec_catalog)
        self.namespace_catalog = NamespaceCatalog()
        self.namespace_catalog.add_namespace(CORE_NAMESPACE, self.namespace)
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Baz', Baz)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Foo', Foo)
        self.type_map.register_map(Baz, ObjectMapper)
        self.type_map.register_map(Foo, ObjectMapper)
        self.manager = BuildManager(self.type_map)


class TestBuildUntypedDatasetOfReferences(BuildDatasetOfReferencesMixin, TestCase):

    def setUpBazSpec(self):
        self.baz_spec = DatasetSpec(
            doc='a list of references to Foo objects',
            dtype=None,
            name='MyBaz',
            shape=[None],
            data_type_def='Baz',
            attributes=[AttributeSpec('baz_attr', 'an example string attribute', 'text')]
        )

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

    def setUpBazSpec(self):
        self.baz_spec = DatasetSpec(
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

    def setUpBazSpec(self):
        self.baz_spec = DatasetSpec(
            doc='a list of references to Foo objects',
            dtype=RefSpec('Foo', 'object'),
            name='MyBaz',
            shape=[None],
            data_type_def='Baz',
            attributes=[AttributeSpec('baz_attr', 'an example string attribute', 'text')]
        )

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

    def setUpBazSpec(self):
        self.baz_spec = DatasetSpec(
            doc='a list of references to Foo objects',
            dtype=None,
            name='MyBaz',
            shape=[None],
            data_type_def='Baz',
            attributes=[AttributeSpec('baz_attr', 'an example string attribute', 'text')]
        )

    def test_build(self):
        ''' Test default mapping functionality when no attributes are nested '''
        foo = Foo('my_foo1', [1, 2, 3], 'string', 10)
        baz = Baz('MyBaz', [foo], 'abcdefghijklmnopqrstuvwxyz')
        msg = "MyBaz (MyBaz): Could not find already-built Builder for Foo 'my_foo1' in BuildManager"
        with self.assertRaisesWith(ReferenceTargetNotBuiltError, msg):
            self.manager.build(baz, root=True)


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
