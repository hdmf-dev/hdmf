import unittest
from abc import ABCMeta, abstractmethod
from datetime import datetime

import h5py
import numpy as np
from hdmf import Container
from hdmf.backends.hdf5 import H5DataIO
from hdmf.build import (GroupBuilder, DatasetBuilder, ObjectMapper, BuildManager, TypeMap, LinkBuilder,
                        ReferenceBuilder, MissingRequiredBuildWarning, OrphanContainerBuildError,
                        ContainerConfigurationError)
from hdmf.container import MultiContainerInterface
from hdmf.data_utils import DataChunkIterator, DataIO, AbstractDataChunkIterator
from hdmf.query import HDMFDataset
from hdmf.spec import (GroupSpec, AttributeSpec, DatasetSpec, SpecCatalog, SpecNamespace, NamespaceCatalog, RefSpec,
                       DtypeSpec, LinkSpec)
from hdmf.testing import TestCase
from hdmf.utils import docval, getargs, get_docval

from tests.unit.utils import CORE_NAMESPACE


class Bar(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Bar'},
            {'name': 'data', 'type': ('data', 'array_data'), 'doc': 'some data'},
            {'name': 'attr1', 'type': str, 'doc': 'an attribute'},
            {'name': 'attr2', 'type': int, 'doc': 'another attribute'},
            {'name': 'attr3', 'type': float, 'doc': 'a third attribute', 'default': 3.14},
            {'name': 'foo', 'type': 'Foo', 'doc': 'a group', 'default': None})
    def __init__(self, **kwargs):
        name, data, attr1, attr2, attr3, foo = getargs('name', 'data', 'attr1', 'attr2', 'attr3', 'foo', kwargs)
        super().__init__(name=name)
        self.__data = data
        self.__attr1 = attr1
        self.__attr2 = attr2
        self.__attr3 = attr3
        self.__foo = foo
        if self.__foo is not None and self.__foo.parent is None:
            self.__foo.parent = self

    def __eq__(self, other):
        attrs = ('name', 'data', 'attr1', 'attr2', 'attr3', 'foo')
        return all(getattr(self, a) == getattr(other, a) for a in attrs)

    def __str__(self):
        attrs = ('name', 'data', 'attr1', 'attr2', 'attr3', 'foo')
        return ','.join('%s=%s' % (a, getattr(self, a)) for a in attrs)

    @property
    def data_type(self):
        return 'Bar'

    @property
    def data(self):
        return self.__data

    @property
    def attr1(self):
        return self.__attr1

    @property
    def attr2(self):
        return self.__attr2

    @property
    def attr3(self):
        return self.__attr3

    @property
    def foo(self):
        return self.__foo

    def remove_foo(self):
        if self is self.__foo.parent:
            self._remove_child(self.__foo)


class Foo(Container):

    @property
    def data_type(self):
        return 'Foo'


class TestGetSubSpec(TestCase):

    def setUp(self):
        self.bar_spec = GroupSpec('A test group specification with a data type', data_type_def='Bar')
        spec_catalog = SpecCatalog()
        spec_catalog.register_spec(self.bar_spec, 'test.yaml')
        namespace = SpecNamespace('a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}],
                                  version='0.1.0',
                                  catalog=spec_catalog)
        namespace_catalog = NamespaceCatalog()
        namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)
        self.type_map = TypeMap(namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Bar', Bar)

    def test_get_subspec_data_type_noname(self):
        parent_spec = GroupSpec('Something to hold a Bar', 'bar_bucket', groups=[self.bar_spec])
        sub_builder = GroupBuilder('my_bar', attributes={'data_type': 'Bar', 'namespace': CORE_NAMESPACE,
                                                         'object_id': -1})
        GroupBuilder('bar_bucket', groups={'my_bar': sub_builder})
        result = self.type_map.get_subspec(parent_spec, sub_builder)
        self.assertIs(result, self.bar_spec)

    def test_get_subspec_named(self):
        child_spec = GroupSpec('A test group specification with a data type', 'my_subgroup')
        parent_spec = GroupSpec('Something to hold a Bar', 'my_group', groups=[child_spec])
        sub_builder = GroupBuilder('my_subgroup', attributes={'data_type': 'Bar', 'namespace': CORE_NAMESPACE,
                                                              'object_id': -1})
        GroupBuilder('my_group', groups={'my_bar': sub_builder})
        result = self.type_map.get_subspec(parent_spec, sub_builder)
        self.assertIs(result, child_spec)


class TestTypeMap(TestCase):

    def setUp(self):
        self.bar_spec = GroupSpec('A test group specification with a data type', data_type_def='Bar')
        self.foo_spec = GroupSpec('A test group specification with data type Foo', data_type_def='Foo')
        self.spec_catalog = SpecCatalog()
        self.spec_catalog.register_spec(self.bar_spec, 'test.yaml')
        self.spec_catalog.register_spec(self.foo_spec, 'test.yaml')
        self.namespace = SpecNamespace('a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}],
                                       version='0.1.0',
                                       catalog=self.spec_catalog)
        self.namespace_catalog = NamespaceCatalog()
        self.namespace_catalog.add_namespace(CORE_NAMESPACE, self.namespace)
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Bar', Bar)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Foo', Foo)

    def test_get_map_unique_mappers(self):
        bar_inst = Bar('my_bar', list(range(10)), 'value1', 10)
        foo_inst = Foo(name='my_foo')
        bar_mapper = self.type_map.get_map(bar_inst)
        foo_mapper = self.type_map.get_map(foo_inst)
        self.assertIsNot(bar_mapper, foo_mapper)

    def test_get_map(self):
        container_inst = Bar('my_bar', list(range(10)), 'value1', 10)
        mapper = self.type_map.get_map(container_inst)
        self.assertIsInstance(mapper, ObjectMapper)
        self.assertIs(mapper.spec, self.bar_spec)
        mapper2 = self.type_map.get_map(container_inst)
        self.assertIs(mapper, mapper2)

    def test_get_map_register(self):
        class MyMap(ObjectMapper):
            pass
        self.type_map.register_map(Bar, MyMap)

        container_inst = Bar('my_bar', list(range(10)), 'value1', 10)
        mapper = self.type_map.get_map(container_inst)
        self.assertIs(mapper.spec, self.bar_spec)
        self.assertIsInstance(mapper, MyMap)


class BarMapper(ObjectMapper):
    def __init__(self, spec):
        super().__init__(spec)
        data_spec = spec.get_dataset('data')
        self.map_spec('attr2', data_spec.get_attribute('attr2'))


class TestMapStrings(TestCase):

    def customSetUp(self, bar_spec):
        spec_catalog = SpecCatalog()
        spec_catalog.register_spec(bar_spec, 'test.yaml')
        namespace = SpecNamespace('a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}], version='0.1.0',
                                  catalog=spec_catalog)
        namespace_catalog = NamespaceCatalog()
        namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)
        type_map = TypeMap(namespace_catalog)
        type_map.register_container_type(CORE_NAMESPACE, 'Bar', Bar)
        return type_map

    def test_build_1d(self):
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[DatasetSpec('an example dataset', 'text', name='data', shape=(None,),
                                                   attributes=[AttributeSpec(
                                                       'attr2', 'an example integer attribute', 'int')])],
                             attributes=[AttributeSpec('attr1', 'an example string attribute', 'text')])
        type_map = self.customSetUp(bar_spec)
        type_map.register_map(Bar, BarMapper)
        bar_inst = Bar('my_bar', ['a', 'b', 'c', 'd'], 'value1', 10)
        builder = type_map.build(bar_inst)
        self.assertEqual(builder.get('data').data, ['a', 'b', 'c', 'd'])

    def test_build_scalar(self):
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[DatasetSpec('an example dataset', 'text', name='data',
                                                   attributes=[AttributeSpec(
                                                       'attr2', 'an example integer attribute', 'int')])],
                             attributes=[AttributeSpec('attr1', 'an example string attribute', 'text')])
        type_map = self.customSetUp(bar_spec)
        type_map.register_map(Bar, BarMapper)
        bar_inst = Bar('my_bar', ['a', 'b', 'c', 'd'], 'value1', 10)
        builder = type_map.build(bar_inst)
        self.assertEqual(builder.get('data').data, "['a', 'b', 'c', 'd']")

    def test_build_dataio(self):
        bar_spec = GroupSpec('A test group specification with a data type',
                             data_type_def='Bar',
                             datasets=[DatasetSpec('an example dataset', 'text', name='data', shape=(None,),
                                                   attributes=[AttributeSpec(
                                                       'attr2', 'an example integer attribute', 'int')])],
                             attributes=[AttributeSpec('attr1', 'an example string attribute', 'text')])
        type_map = self.customSetUp(bar_spec)
        type_map.register_map(Bar, BarMapper)
        bar_inst = Bar('my_bar', H5DataIO(['a', 'b', 'c', 'd'], chunks=True), 'value1', 10)
        builder = type_map.build(bar_inst)
        self.assertIsInstance(builder.get('data').data, H5DataIO)


class TestDynamicContainer(TestCase):

    def setUp(self):
        self.bar_spec = GroupSpec('A test group specification with a data type',
                                  data_type_def='Bar',
                                  datasets=[DatasetSpec('an example dataset', 'int', name='data',
                                                        attributes=[AttributeSpec(
                                                            'attr2', 'an example integer attribute', 'int')])],
                                  attributes=[AttributeSpec('attr1', 'an example string attribute', 'text')])
        self.spec_catalog = SpecCatalog()
        self.spec_catalog.register_spec(self.bar_spec, 'test.yaml')
        self.namespace = SpecNamespace('a test namespace', CORE_NAMESPACE,
                                       [{'source': 'test.yaml'}],
                                       version='0.1.0',
                                       catalog=self.spec_catalog)
        self.namespace_catalog = NamespaceCatalog()
        self.namespace_catalog.add_namespace(CORE_NAMESPACE, self.namespace)
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Bar', Bar)
        self.manager = BuildManager(self.type_map)
        self.mapper = ObjectMapper(self.bar_spec)

    def test_dynamic_container_creation(self):
        baz_spec = GroupSpec('A test extension with no Container class',
                             data_type_def='Baz', data_type_inc=self.bar_spec,
                             attributes=[AttributeSpec('attr3', 'an example float attribute', 'float'),
                                         AttributeSpec('attr4', 'another example float attribute', 'float')])
        self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
        cls = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz')
        expected_args = {'name', 'data', 'attr1', 'attr2', 'attr3', 'attr4'}
        received_args = set()
        for x in get_docval(cls.__init__):
            if x['name'] != 'foo':
                received_args.add(x['name'])
                with self.subTest(name=x['name']):
                    self.assertNotIn('default', x)
        self.assertSetEqual(expected_args, received_args)
        self.assertEqual(cls.__name__, 'Baz')
        self.assertTrue(issubclass(cls, Bar))

    def test_dynamic_container_default_name(self):
        baz_spec = GroupSpec('doc', default_name='bingo', data_type_def='Baz',
                             attributes=[AttributeSpec('attr4', 'another example float attribute', 'float')])
        self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
        cls = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz')
        inst = cls(attr4=10.)
        self.assertEqual(inst.name, 'bingo')

    def test_dynamic_container_creation_defaults(self):
        baz_spec = GroupSpec('A test extension with no Container class',
                             data_type_def='Baz', data_type_inc=self.bar_spec,
                             attributes=[AttributeSpec('attr3', 'an example float attribute', 'float'),
                                         AttributeSpec('attr4', 'another example float attribute', 'float')])
        self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
        cls = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz')
        expected_args = {'name', 'data', 'attr1', 'attr2', 'attr3', 'attr4', 'foo'}
        received_args = set(map(lambda x: x['name'], get_docval(cls.__init__)))
        self.assertSetEqual(expected_args, received_args)
        self.assertEqual(cls.__name__, 'Baz')
        self.assertTrue(issubclass(cls, Bar))

    def test_dynamic_container_constructor(self):
        baz_spec = GroupSpec('A test extension with no Container class',
                             data_type_def='Baz', data_type_inc=self.bar_spec,
                             attributes=[AttributeSpec('attr3', 'an example float attribute', 'float'),
                                         AttributeSpec('attr4', 'another example float attribute', 'float')])
        self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
        cls = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz')
        # TODO: test that constructor works!
        inst = cls('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0)
        self.assertEqual(inst.name, 'My Baz')
        self.assertEqual(inst.data, [1, 2, 3, 4])
        self.assertEqual(inst.attr1, 'string attribute')
        self.assertEqual(inst.attr2, 1000)
        self.assertEqual(inst.attr3, 98.6)
        self.assertEqual(inst.attr4, 1.0)

    def test_dynamic_container_constructor_name(self):
        # name is specified in spec and cannot be changed
        baz_spec = GroupSpec('A test extension with no Container class',
                             data_type_def='Baz', data_type_inc=self.bar_spec,
                             name='A fixed name',
                             attributes=[AttributeSpec('attr3', 'an example float attribute', 'float'),
                                         AttributeSpec('attr4', 'another example float attribute', 'float')])
        self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
        cls = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz')

        with self.assertRaises(TypeError):
            inst = cls('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0)

        inst = cls([1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0)
        self.assertEqual(inst.name, 'A fixed name')
        self.assertEqual(inst.data, [1, 2, 3, 4])
        self.assertEqual(inst.attr1, 'string attribute')
        self.assertEqual(inst.attr2, 1000)
        self.assertEqual(inst.attr3, 98.6)
        self.assertEqual(inst.attr4, 1.0)

    def test_dynamic_container_constructor_name_default_name(self):
        # if both name and default_name are specified, name should be used
        with self.assertWarns(Warning):
            baz_spec = GroupSpec('A test extension with no Container class',
                                 data_type_def='Baz', data_type_inc=self.bar_spec,
                                 name='A fixed name',
                                 default_name='A default name',
                                 attributes=[AttributeSpec('attr3', 'an example float attribute', 'float'),
                                             AttributeSpec('attr4', 'another example float attribute', 'float')])
            self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
            cls = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz')

            inst = cls([1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0)
            self.assertEqual(inst.name, 'A fixed name')

    def test_dynamic_container_composition(self):
        baz_spec2 = GroupSpec('A composition inside', data_type_def='Baz2',
                              data_type_inc=self.bar_spec,
                              attributes=[
                                  AttributeSpec('attr3', 'an example float attribute', 'float'),
                                  AttributeSpec('attr4', 'another example float attribute', 'float')])

        baz_spec1 = GroupSpec('A composition test outside', data_type_def='Baz1', data_type_inc=self.bar_spec,
                              attributes=[AttributeSpec('attr3', 'an example float attribute', 'float'),
                                          AttributeSpec('attr4', 'another example float attribute', 'float')],
                              groups=[GroupSpec('A composition inside', data_type_inc='Baz2')])
        self.spec_catalog.register_spec(baz_spec1, 'extension.yaml')
        self.spec_catalog.register_spec(baz_spec2, 'extension.yaml')
        Baz2 = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz2')
        Baz1 = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz1')
        Baz1('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0,
             baz2=Baz2('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0))

        Bar = self.type_map.get_container_cls(CORE_NAMESPACE, 'Bar')
        bar = Bar('My Bar', [1, 2, 3, 4], 'string attribute', 1000)

        with self.assertRaises(TypeError):
            Baz1('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0, baz2=bar)

    def test_dynamic_container_composition_reverse_order(self):
        baz_spec2 = GroupSpec('A composition inside', data_type_def='Baz2',
                              data_type_inc=self.bar_spec,
                              attributes=[
                                  AttributeSpec('attr3', 'an example float attribute', 'float'),
                                  AttributeSpec('attr4', 'another example float attribute', 'float')])

        baz_spec1 = GroupSpec('A composition test outside', data_type_def='Baz1', data_type_inc=self.bar_spec,
                              attributes=[AttributeSpec('attr3', 'an example float attribute', 'float'),
                                          AttributeSpec('attr4', 'another example float attribute', 'float')],
                              groups=[GroupSpec('A composition inside', data_type_inc='Baz2')])
        self.spec_catalog.register_spec(baz_spec1, 'extension.yaml')
        self.spec_catalog.register_spec(baz_spec2, 'extension.yaml')
        Baz1 = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz1')
        Baz2 = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz2')
        Baz1('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0,
             baz2=Baz2('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0))

        Bar = self.type_map.get_container_cls(CORE_NAMESPACE, 'Bar')
        bar = Bar('My Bar', [1, 2, 3, 4], 'string attribute', 1000)

        with self.assertRaises(TypeError):
            Baz1('My Baz', [1, 2, 3, 4], 'string attribute', 1000, attr3=98.6, attr4=1.0, baz2=bar)

    def test_dynamic_container_composition_missing_type(self):
        baz_spec1 = GroupSpec('A composition test outside', data_type_def='Baz1', data_type_inc=self.bar_spec,
                              attributes=[AttributeSpec('attr3', 'an example float attribute', 'float'),
                                          AttributeSpec('attr4', 'another example float attribute', 'float')],
                              groups=[GroupSpec('A composition inside', data_type_inc='Baz2')])
        self.spec_catalog.register_spec(baz_spec1, 'extension.yaml')

        msg = "No specification for 'Baz2' in namespace 'test_core'"
        with self.assertRaisesWith(ValueError, msg):
            self.manager.type_map.get_container_cls(CORE_NAMESPACE, 'Baz1')

    def test_dynamic_container_fixed_name(self):
        """Test that dynamic class generation for an extended type with a fixed name works."""
        baz_spec = GroupSpec('A test extension with no Container class',
                             data_type_def='Baz', data_type_inc=self.bar_spec, name='Baz')
        self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
        Baz = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz')
        obj = Baz([1, 2, 3, 4], 'string attribute', attr2=1000)
        self.assertEqual(obj.name, 'Baz')

    def test_multi_container_spec(self):
        multi_spec = GroupSpec(
            'A test extension that contains a multi',
            data_type_def='Multi',
            groups=[
                GroupSpec(
                    data_type_inc=self.bar_spec,
                    doc='test multi',
                    quantity='*')],
            attributes=[
                AttributeSpec('attr3', 'an example float attribute', 'float')]
        )
        self.spec_catalog.register_spec(multi_spec, 'extension.yaml')
        Bar = self.type_map.get_container_cls(CORE_NAMESPACE, 'Bar')
        Multi = self.type_map.get_container_cls(CORE_NAMESPACE, 'Multi')
        assert issubclass(Multi, MultiContainerInterface)
        assert Multi.__clsconf__[0] == dict(
            attr='bars',
            type=Bar,
            add='add_bars',
            get='get_bars',
            create='create_bars'
        )

        multi = Multi(name='my_multi',
                      bars=[Bar('my_bar', list(range(10)), 'value1', 10)],
                      attr3=5.)
        assert multi.bars['my_bar'] == Bar('my_bar', list(range(10)), 'value1', 10)
        assert multi.attr3 == 5.

    def test_build_docval(self):
        Bar = self.type_map.get_container_cls(CORE_NAMESPACE, 'Bar')
        addl_fields = dict(
            attr3=AttributeSpec('attr3', 'an example numeric attribute', 'numeric'),
            attr4=AttributeSpec('attr4', 'another example float attribute', 'float')
        )
        docval = self.type_map._build_docval(Bar, addl_fields)

        expected = [
            {'doc': 'the name of this Bar', 'name': 'name', 'type': str},
            {'name': 'data',
             'type': (DataIO, np.ndarray, list, tuple, h5py.Dataset,
                      HDMFDataset, AbstractDataChunkIterator),
             'doc': 'some data'},
            {'name': 'attr1', 'type': str,
             'doc': 'an attribute'},
            {'name': 'attr2', 'type': int,
             'doc': 'another attribute'},
            {'name': 'attr3', 'doc': 'an example numeric attribute',
             'type': (float, np.float32, np.float64, np.int8, np.int16,
                      np.int32, np.int64, int, np.uint8, np.uint16,
                      np.uint32, np.uint64)},
            {'name': 'foo', 'type': 'Foo', 'doc': 'a group', 'default': None},
            {'name': 'attr4', 'doc': 'another example float attribute',
             'type': (float, np.float32, np.float64)}
        ]

        self.assertListEqual(docval, expected)

    def test_build_docval_shape(self):
        """Test that docval generation for a class with shape has the shape set."""
        Bar = self.type_map.get_container_cls(CORE_NAMESPACE, 'Bar')
        addl_fields = dict(attr3=AttributeSpec('attr3', 'an example numeric attribute', 'numeric', shape=[None]))
        docval = self.type_map._build_docval(Bar, addl_fields)

        for arg in docval:
            if arg['name'] == 'attr3':
                self.assertListEqual(arg['shape'], [None])

    def test_build_docval_default_value(self):
        """Test that docval generation for a class with an additional optional field has the default value set."""
        Bar = self.type_map.get_container_cls(CORE_NAMESPACE, 'Bar')
        addl_fields = dict(attr3=AttributeSpec('attr3', 'an example numeric attribute', 'float',
                                               required=False, default_value=10.0))
        docval = self.type_map._build_docval(Bar, addl_fields)

        for arg in docval:
            if arg['name'] == 'attr3':
                self.assertEqual(arg['default'], 10.0)

    def test_build_docval_default_value_none(self):
        """Test that docval generation for a class with an additional optional field has default: None."""
        Bar = self.type_map.get_container_cls(CORE_NAMESPACE, 'Bar')
        addl_fields = dict(attr3=AttributeSpec('attr3', 'an example numeric attribute', 'float',
                                               required=False))
        docval = self.type_map._build_docval(Bar, addl_fields)

        for arg in docval:
            if arg['name'] == 'attr3':
                self.assertIsNone(arg['default'])

    def test_build_docval_fixed_name(self):
        """Test that docval generation for a class with a fixed name does not contain a docval arg for name."""
        docval = self.type_map._build_docval(Bar, {}, name='Baz')

        found = False
        for arg in docval:
            if arg['name'] == 'name':
                found = True
        self.assertFalse(found)

    def test_build_docval_default_name(self):
        """Test that docval generation for a class with a default name has the default value for name set."""
        docval = self.type_map._build_docval(Bar, {}, default_name='MyBaz')

        for arg in docval:
            if arg['name'] == 'name':
                self.assertEqual(arg['default'], 'MyBaz')

    def test_build_docval_link(self):
        Bar = self.type_map.get_container_cls(CORE_NAMESPACE, 'Bar')
        addl_fields = dict(
            attr3=LinkSpec(name='attr3', target_type='Bar', doc='an example link'),
        )
        docval = self.type_map._build_docval(Bar, addl_fields)

        for arg in docval:
            if arg['name'] == 'attr3':
                self.assertIs(arg['type'], Bar)


class ObjectMapperMixin(metaclass=ABCMeta):

    def setUp(self):
        self.setUpBarSpec()
        self.spec_catalog = SpecCatalog()
        self.spec_catalog.register_spec(self.bar_spec, 'test.yaml')
        self.namespace = SpecNamespace('a test namespace', CORE_NAMESPACE,
                                       [{'source': 'test.yaml'}],
                                       version='0.1.0',
                                       catalog=self.spec_catalog)
        self.namespace_catalog = NamespaceCatalog()
        self.namespace_catalog.add_namespace(CORE_NAMESPACE, self.namespace)
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Bar', Bar)
        self.manager = BuildManager(self.type_map)
        self.mapper = ObjectMapper(self.bar_spec)

    @abstractmethod
    def setUpBarSpec(self):
        raise NotImplementedError('Cannot run test unless setUpBarSpec is implemented')

    def test_default_mapping(self):
        attr_map = self.mapper.get_attr_names(self.bar_spec)
        keys = set(attr_map.keys())
        for key in keys:
            with self.subTest(key=key):
                self.assertIs(attr_map[key], self.mapper.get_attr_spec(key))
                self.assertIs(attr_map[key], self.mapper.get_carg_spec(key))


class TestObjectMapperNested(ObjectMapperMixin, TestCase):

    def setUpBarSpec(self):
        self.bar_spec = GroupSpec('A test group specification with a data type',
                                  data_type_def='Bar',
                                  datasets=[DatasetSpec('an example dataset', 'int', name='data',
                                                        attributes=[AttributeSpec(
                                                            'attr2', 'an example integer attribute', 'int')])],
                                  attributes=[AttributeSpec('attr1', 'an example string attribute', 'text')])

    def test_build(self):
        ''' Test default mapping functionality when object attributes map to an  attribute deeper
        than top-level Builder '''
        container_inst = Bar('my_bar', list(range(10)), 'value1', 10)
        expected = GroupBuilder(
            name='my_bar',
            datasets={'data': DatasetBuilder(
                name='data',
                data=list(range(10)),
                attributes={'attr2': 10}
            )},
            attributes={'attr1': 'value1'}
        )
        self._remap_nested_attr()
        builder = self.mapper.build(container_inst, self.manager)
        self.assertBuilderEqual(builder, expected)

    def test_construct(self):
        ''' Test default mapping functionality when object attributes map to an attribute
        deeper than top-level Builder '''
        expected = Bar('my_bar', list(range(10)), 'value1', 10)
        builder = GroupBuilder(
            name='my_bar',
            datasets={'data': DatasetBuilder(
                name='data',
                data=list(range(10)),
                attributes={'attr2': 10}
            )},
            attributes={'attr1': 'value1',
                        'data_type': 'Bar',
                        'namespace': CORE_NAMESPACE,
                        'object_id': expected.object_id}
        )
        self._remap_nested_attr()
        container = self.mapper.construct(builder, self.manager)
        self.assertEqual(container, expected)

    def test_default_mapping_keys(self):
        attr_map = self.mapper.get_attr_names(self.bar_spec)
        keys = set(attr_map.keys())
        expected = {'attr1', 'data', 'data__attr2'}
        self.assertSetEqual(keys, expected)

    def test_remap_keys(self):
        self._remap_nested_attr()
        self.assertEqual(self.mapper.get_attr_spec('attr2'),
                         self.mapper.spec.get_dataset('data').get_attribute('attr2'))
        self.assertEqual(self.mapper.get_attr_spec('attr1'), self.mapper.spec.get_attribute('attr1'))
        self.assertEqual(self.mapper.get_attr_spec('data'), self.mapper.spec.get_dataset('data'))

    def _remap_nested_attr(self):
        data_spec = self.mapper.spec.get_dataset('data')
        self.mapper.map_spec('attr2', data_spec.get_attribute('attr2'))


class TestObjectMapperNoNesting(ObjectMapperMixin, TestCase):

    def setUpBarSpec(self):
        self.bar_spec = GroupSpec('A test group specification with a data type',
                                  data_type_def='Bar',
                                  datasets=[DatasetSpec('an example dataset', 'int', name='data')],
                                  attributes=[AttributeSpec('attr1', 'an example string attribute', 'text'),
                                              AttributeSpec('attr2', 'an example integer attribute', 'int')])

    def test_build(self):
        ''' Test default mapping functionality when no attributes are nested '''
        container = Bar('my_bar', list(range(10)), 'value1', 10)
        builder = self.mapper.build(container, self.manager)
        expected = GroupBuilder('my_bar', datasets={'data': DatasetBuilder('data', list(range(10)))},
                                attributes={'attr1': 'value1', 'attr2': 10})
        self.assertBuilderEqual(builder, expected)

    def test_build_empty(self):
        ''' Test default mapping functionality when no attributes are nested '''
        container = Bar('my_bar', [], 'value1', 10)
        builder = self.mapper.build(container, self.manager)
        expected = GroupBuilder('my_bar', datasets={'data': DatasetBuilder('data', [])},
                                attributes={'attr1': 'value1', 'attr2': 10})
        self.assertBuilderEqual(builder, expected)

    def test_construct(self):
        expected = Bar('my_bar', list(range(10)), 'value1', 10)
        builder = GroupBuilder('my_bar', datasets={'data': DatasetBuilder('data', list(range(10)))},
                               attributes={'attr1': 'value1', 'attr2': 10, 'data_type': 'Bar',
                                           'namespace': CORE_NAMESPACE, 'object_id': expected.object_id})
        container = self.mapper.construct(builder, self.manager)
        self.assertEqual(container, expected)

    def test_default_mapping_keys(self):
        attr_map = self.mapper.get_attr_names(self.bar_spec)
        keys = set(attr_map.keys())
        expected = {'attr1', 'data', 'attr2'}
        self.assertSetEqual(keys, expected)


class TestObjectMapperContainer(ObjectMapperMixin, TestCase):

    def setUpBarSpec(self):
        self.bar_spec = GroupSpec('A test group specification with a data type',
                                  data_type_def='Bar',
                                  groups=[GroupSpec('an example group', data_type_def='Foo')],
                                  attributes=[AttributeSpec('attr1', 'an example string attribute', 'text'),
                                              AttributeSpec('attr2', 'an example integer attribute', 'int')])

    def test_default_mapping_keys(self):
        attr_map = self.mapper.get_attr_names(self.bar_spec)
        keys = set(attr_map.keys())
        expected = {'attr1', 'foo', 'attr2'}
        self.assertSetEqual(keys, expected)


class TestLinkedContainer(TestCase):

    def setUp(self):
        self.foo_spec = GroupSpec('A test group specification with data type Foo', data_type_def='Foo')
        self.bar_spec = GroupSpec('A test group specification with a data type Bar',
                                  data_type_def='Bar',
                                  groups=[self.foo_spec],
                                  datasets=[DatasetSpec('an example dataset', 'int', name='data')],
                                  attributes=[AttributeSpec('attr1', 'an example string attribute', 'text'),
                                              AttributeSpec('attr2', 'an example integer attribute', 'int')])

        self.spec_catalog = SpecCatalog()
        self.spec_catalog.register_spec(self.foo_spec, 'test.yaml')
        self.spec_catalog.register_spec(self.bar_spec, 'test.yaml')
        self.namespace = SpecNamespace('a test namespace', CORE_NAMESPACE,
                                       [{'source': 'test.yaml'}],
                                       version='0.1.0',
                                       catalog=self.spec_catalog)
        self.namespace_catalog = NamespaceCatalog()
        self.namespace_catalog.add_namespace(CORE_NAMESPACE, self.namespace)
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Foo', Foo)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Bar', Bar)
        self.manager = BuildManager(self.type_map)
        self.foo_mapper = ObjectMapper(self.foo_spec)
        self.bar_mapper = ObjectMapper(self.bar_spec)

    def test_build_child_link(self):
        ''' Test default mapping functionality when one container contains a child link to another container '''
        foo_inst = Foo('my_foo')
        bar_inst1 = Bar('my_bar1', list(range(10)), 'value1', 10, foo=foo_inst)
        # bar_inst2.foo should link to bar_inst1.foo
        bar_inst2 = Bar('my_bar2', list(range(10)), 'value1', 10, foo=foo_inst)

        foo_builder = self.foo_mapper.build(foo_inst, self.manager)
        bar1_builder = self.bar_mapper.build(bar_inst1, self.manager)
        bar2_builder = self.bar_mapper.build(bar_inst2, self.manager)

        foo_expected = GroupBuilder('my_foo')

        inner_foo_builder = GroupBuilder('my_foo',
                                         attributes={'data_type': 'Foo',
                                                     'namespace': CORE_NAMESPACE,
                                                     'object_id': foo_inst.object_id})
        bar1_expected = GroupBuilder('my_bar1',
                                     datasets={'data': DatasetBuilder('data', list(range(10)))},
                                     groups={'foo': inner_foo_builder},
                                     attributes={'attr1': 'value1', 'attr2': 10})
        link_foo_builder = LinkBuilder(builder=inner_foo_builder)
        bar2_expected = GroupBuilder('my_bar2',
                                     datasets={'data': DatasetBuilder('data', list(range(10)))},
                                     links={'foo': link_foo_builder},
                                     attributes={'attr1': 'value1', 'attr2': 10})
        self.assertBuilderEqual(foo_builder, foo_expected)
        self.assertBuilderEqual(bar1_builder, bar1_expected)
        self.assertBuilderEqual(bar2_builder, bar2_expected)

    @unittest.expectedFailure
    def test_build_broken_link_parent(self):
        ''' Test that building a container with a broken link that has a parent raises an error. '''
        foo_inst = Foo('my_foo')
        Bar('my_bar1', list(range(10)), 'value1', 10, foo=foo_inst)  # foo_inst.parent is this bar
        # bar_inst2.foo should link to bar_inst1.foo
        bar_inst2 = Bar('my_bar2', list(range(10)), 'value1', 10, foo=foo_inst)

        # TODO bar_inst.foo.parent exists but is never built - this is a tricky edge case that should raise an error
        with self.assertRaises(OrphanContainerBuildError):
            self.bar_mapper.build(bar_inst2, self.manager)

    def test_build_broken_link_no_parent(self):
        ''' Test that building a container with a broken link that has no parent raises an error. '''
        foo_inst = Foo('my_foo')
        bar_inst1 = Bar('my_bar1', list(range(10)), 'value1', 10, foo=foo_inst)  # foo_inst.parent is this bar
        # bar_inst2.foo should link to bar_inst1.foo
        bar_inst2 = Bar('my_bar2', list(range(10)), 'value1', 10, foo=foo_inst)
        bar_inst1.remove_foo()

        msg = ("my_bar2 (my_bar2): Linked Foo 'my_foo' has no parent. Remove the link or ensure the linked container "
               "is added properly.")
        with self.assertRaisesWith(OrphanContainerBuildError, msg):
            self.bar_mapper.build(bar_inst2, self.manager)


class TestReference(TestCase):

    def setUp(self):
        self.foo_spec = GroupSpec('A test group specification with data type Foo', data_type_def='Foo')
        self.bar_spec = GroupSpec('A test group specification with a data type Bar',
                                  data_type_def='Bar',
                                  datasets=[DatasetSpec('an example dataset', 'int', name='data')],
                                  attributes=[AttributeSpec('attr1', 'an example string attribute', 'text'),
                                              AttributeSpec('attr2', 'an example integer attribute', 'int'),
                                              AttributeSpec('foo', 'a referenced foo', RefSpec('Foo', 'object'),
                                                            required=False)])

        self.spec_catalog = SpecCatalog()
        self.spec_catalog.register_spec(self.foo_spec, 'test.yaml')
        self.spec_catalog.register_spec(self.bar_spec, 'test.yaml')
        self.namespace = SpecNamespace('a test namespace', CORE_NAMESPACE,
                                       [{'source': 'test.yaml'}],
                                       version='0.1.0',
                                       catalog=self.spec_catalog)
        self.namespace_catalog = NamespaceCatalog()
        self.namespace_catalog.add_namespace(CORE_NAMESPACE, self.namespace)
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Foo', Foo)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Bar', Bar)
        self.manager = BuildManager(self.type_map)
        self.foo_mapper = ObjectMapper(self.foo_spec)
        self.bar_mapper = ObjectMapper(self.bar_spec)

    def test_build_attr_ref(self):
        ''' Test default mapping functionality when one container contains an attribute reference to another container.
        '''
        foo_inst = Foo('my_foo')
        bar_inst1 = Bar('my_bar1', list(range(10)), 'value1', 10, foo=foo_inst)
        bar_inst2 = Bar('my_bar2', list(range(10)), 'value1', 10)

        foo_builder = self.manager.build(foo_inst, root=True)
        bar1_builder = self.manager.build(bar_inst1, root=True)  # adds refs
        bar2_builder = self.manager.build(bar_inst2, root=True)

        foo_expected = GroupBuilder('my_foo',
                                    attributes={'data_type': 'Foo',
                                                'namespace': CORE_NAMESPACE,
                                                'object_id': foo_inst.object_id})
        bar1_expected = GroupBuilder('n/a',  # name doesn't matter
                                     datasets={'data': DatasetBuilder('data', list(range(10)))},
                                     attributes={'attr1': 'value1',
                                                 'attr2': 10,
                                                 'foo': ReferenceBuilder(foo_expected),
                                                 'data_type': 'Bar',
                                                 'namespace': CORE_NAMESPACE,
                                                 'object_id': bar_inst1.object_id})
        bar2_expected = GroupBuilder('n/a',  # name doesn't matter
                                     datasets={'data': DatasetBuilder('data', list(range(10)))},
                                     attributes={'attr1': 'value1',
                                                 'attr2': 10,
                                                 'data_type': 'Bar',
                                                 'namespace': CORE_NAMESPACE,
                                                 'object_id': bar_inst2.object_id})
        self.assertDictEqual(foo_builder, foo_expected)
        self.assertDictEqual(bar1_builder, bar1_expected)
        self.assertDictEqual(bar2_builder, bar2_expected)

    def test_build_attr_ref_invalid(self):
        ''' Test default mapping functionality when one container contains an attribute reference to another container.
        '''
        bar_inst1 = Bar('my_bar1', list(range(10)), 'value1', 10)
        bar_inst1._Bar__foo = object()  # make foo object a non-container type

        msg = "invalid type for reference 'foo' (<class 'object'>) - must be AbstractContainer"
        with self.assertRaisesWith(ValueError, msg):
            self.bar_mapper.build(bar_inst1, self.manager)


class TestMissingRequiredAttribute(ObjectMapperMixin, TestCase):

    def setUpBarSpec(self):
        self.bar_spec = GroupSpec(
            doc='A test group specification with a data type Bar',
            data_type_def='Bar',
            attributes=[AttributeSpec('attr1', 'an example string attribute', 'text'),
                        AttributeSpec('attr2', 'an example integer attribute', 'int')]
        )

    def test_required_attr_missing(self):
        """Test mapping when one container is missing a required attribute."""

        bar_inst1 = Bar('my_bar1', list(range(10)), 'value1', 10)
        bar_inst1._Bar__attr1 = None  # make attr1 attribute None

        msg = "Bar 'my_bar1' is missing required value for attribute 'attr1'."
        with self.assertWarnsWith(MissingRequiredBuildWarning, msg):
            builder = self.mapper.build(bar_inst1, self.manager)

        expected = GroupBuilder(
            name='my_bar1',
            attributes={'attr2': 10}
        )
        self.assertBuilderEqual(expected, builder)


class TestMissingRequiredAttributeRef(ObjectMapperMixin, TestCase):

    def setUpBarSpec(self):
        self.bar_spec = GroupSpec(
            doc='A test group specification with a data type Bar',
            data_type_def='Bar',
            attributes=[AttributeSpec('foo', 'a referenced foo', RefSpec('Foo', 'object'))]
        )

    def test_required_attr_ref_missing(self):
        """Test mapping when one container is missing a required attribute reference."""

        bar_inst1 = Bar('my_bar1', list(range(10)), 'value1', 10)

        msg = "Bar 'my_bar1' is missing required value for attribute 'foo'."
        with self.assertWarnsWith(MissingRequiredBuildWarning, msg):
            builder = self.mapper.build(bar_inst1, self.manager)

        expected = GroupBuilder(
            name='my_bar1',
        )
        self.assertBuilderEqual(expected, builder)


class TestMissingRequiredDataset(ObjectMapperMixin, TestCase):

    def setUpBarSpec(self):
        self.bar_spec = GroupSpec(
            doc='A test group specification with a data type Bar',
            data_type_def='Bar',
            datasets=[DatasetSpec('an example dataset', 'int', name='data')]
        )

    def test_required_dataset_missing(self):
        """Test mapping when one container is missing a required dataset."""

        bar_inst1 = Bar('my_bar1', list(range(10)), 'value1', 10)
        bar_inst1._Bar__data = None  # make data dataset None

        msg = "Bar 'my_bar1' is missing required value for attribute 'data'."
        with self.assertWarnsWith(MissingRequiredBuildWarning, msg):
            builder = self.mapper.build(bar_inst1, self.manager)

        expected = GroupBuilder(
            name='my_bar1',
        )
        self.assertBuilderEqual(expected, builder)


class TestMissingRequiredGroup(ObjectMapperMixin, TestCase):

    def setUpBarSpec(self):
        self.bar_spec = GroupSpec(
            doc='A test group specification with a data type Bar',
            data_type_def='Bar',
            groups=[GroupSpec('foo', data_type_inc='Foo')]
        )

    def test_required_group_missing(self):
        """Test mapping when one container is missing a required group."""

        bar_inst1 = Bar('my_bar1', list(range(10)), 'value1', 10)
        msg = "Bar 'my_bar1' is missing required value for attribute 'foo'."
        with self.assertWarnsWith(MissingRequiredBuildWarning, msg):
            builder = self.mapper.build(bar_inst1, self.manager)

        expected = GroupBuilder(
            name='my_bar1',
        )
        self.assertBuilderEqual(expected, builder)


class TestRequiredEmptyGroup(ObjectMapperMixin, TestCase):

    def setUpBarSpec(self):
        self.bar_spec = GroupSpec(
            doc='A test group specification with a data type Bar',
            data_type_def='Bar',
            groups=[GroupSpec(name='empty', doc='empty group')],
        )

    def test_required_group_empty(self):
        """Test mapping when one container has a required empty group."""

        bar_inst1 = Bar('my_bar1', list(range(10)), 'value1', 10)
        builder = self.mapper.build(bar_inst1, self.manager)

        expected = GroupBuilder(
            name='my_bar1',
            groups={'empty': GroupBuilder('empty')},
        )
        self.assertBuilderEqual(expected, builder)


class TestOptionalEmptyGroup(ObjectMapperMixin, TestCase):

    def setUpBarSpec(self):
        self.bar_spec = GroupSpec(
            doc='A test group specification with a data type Bar',
            data_type_def='Bar',
            groups=[GroupSpec(
                name='empty',
                doc='empty group',
                quantity='?',
                attributes=[AttributeSpec('attr3', 'an optional float attribute', 'float', required=False)]
            )]
        )

    def test_optional_group_empty(self):
        """Test mapping when one container has an optional empty group."""

        self.mapper.map_spec('attr3', self.mapper.spec.get_group('empty').get_attribute('attr3'))

        bar_inst1 = Bar('my_bar1', list(range(10)), 'value1', 10)
        bar_inst1._Bar__attr3 = None  # force attr3 to be None
        builder = self.mapper.build(bar_inst1, self.manager)

        expected = GroupBuilder(
            name='my_bar1',
        )
        self.assertBuilderEqual(expected, builder)

    def test_optional_group_not_empty(self):
        """Test mapping when one container has an optional not empty group."""

        self.mapper.map_spec('attr3', self.mapper.spec.get_group('empty').get_attribute('attr3'))

        bar_inst1 = Bar('my_bar1', list(range(10)), 'value1', 10, attr3=1.23)
        builder = self.mapper.build(bar_inst1, self.manager)

        expected = GroupBuilder(
            name='my_bar1',
            groups={'empty': GroupBuilder(
                name='empty',
                attributes={'attr3': 1.23},
            )},
        )
        self.assertBuilderEqual(expected, builder)


class TestFixedAttributeValue(ObjectMapperMixin, TestCase):

    def setUpBarSpec(self):
        self.bar_spec = GroupSpec(
            doc='A test group specification with a data type Bar',
            data_type_def='Bar',
            attributes=[AttributeSpec('attr1', 'an example string attribute', 'text', value='hi'),
                        AttributeSpec('attr2', 'an example integer attribute', 'int')]
        )

    def test_required_attr_missing(self):
        """Test mapping when one container has a required attribute with a fixed value."""

        bar_inst1 = Bar('my_bar1', list(range(10)), 'value1', 10)  # attr1=value1 is not processed
        builder = self.mapper.build(bar_inst1, self.manager)

        expected = GroupBuilder(
            name='my_bar1',
            attributes={'attr1': 'hi', 'attr2': 10}
        )
        self.assertBuilderEqual(builder, expected)


class TestConvertDtype(TestCase):

    def test_value_none(self):
        spec = DatasetSpec('an example dataset', 'int', name='data')
        self.assertTupleEqual(ObjectMapper.convert_dtype(spec, None), (None, 'int'))

        spec = DatasetSpec('an example dataset', RefSpec(reftype='object', target_type='int'), name='data')
        self.assertTupleEqual(ObjectMapper.convert_dtype(spec, None), (None, 'object'))

    # do full matrix test of given value x and spec y, what does convert_dtype return?
    def test_convert_to_64bit_spec(self):
        """
        Test that if given any value for a spec with a 64-bit dtype, convert_dtype will convert to the spec type.
        Also test that if the given value is not the same as the spec, convert_dtype raises a warning.
        """
        spec_type = 'float64'
        value_types = ['double', 'float64']
        self._test_convert_alias(spec_type, value_types)

        spec_type = 'float64'
        value_types = ['float', 'float32', 'long', 'int64', 'int', 'int32', 'int16', 'short', 'int8', 'uint64', 'uint',
                       'uint32', 'uint16', 'uint8', 'bool']
        self._test_convert_higher_precision_helper(spec_type, value_types)

        spec_type = 'int64'
        value_types = ['long', 'int64']
        self._test_convert_alias(spec_type, value_types)

        spec_type = 'int64'
        value_types = ['double', 'float64', 'float', 'float32', 'int', 'int32', 'int16', 'short', 'int8', 'uint64',
                       'uint', 'uint32', 'uint16', 'uint8', 'bool']
        self._test_convert_higher_precision_helper(spec_type, value_types)

        spec_type = 'uint64'
        value_types = ['uint64']
        self._test_convert_alias(spec_type, value_types)

        spec_type = 'uint64'
        value_types = ['double', 'float64', 'float', 'float32', 'long', 'int64', 'int', 'int32', 'int16', 'short',
                       'int8', 'uint', 'uint32', 'uint16', 'uint8', 'bool']
        self._test_convert_higher_precision_helper(spec_type, value_types)

    def test_convert_to_float32_spec(self):
        """Test conversion of various types to float32.
        If given a value with precision > float32 and float base type, convert_dtype will keep the higher precision.
        If given a value with 64-bit precision and different base type, convert_dtype will convert to float64.
        If given a value that is float32, convert_dtype will convert to float32.
        If given a value with precision <= float32, convert_dtype will convert to float32 and raise a warning.
        """
        spec_type = 'float32'
        value_types = ['double', 'float64']
        self._test_keep_higher_precision_helper(spec_type, value_types)

        value_types = ['long', 'int64', 'uint64']
        expected_type = 'float64'
        self._test_change_basetype_helper(spec_type, value_types, expected_type)

        value_types = ['float', 'float32']
        self._test_convert_alias(spec_type, value_types)

        value_types = ['int', 'int32', 'int16', 'short', 'int8', 'uint', 'uint32', 'uint16', 'uint8', 'bool']
        self._test_convert_higher_precision_helper(spec_type, value_types)

    def test_convert_to_int32_spec(self):
        """Test conversion of various types to int32.
        If given a value with precision > int32 and int base type, convert_dtype will keep the higher precision.
        If given a value with 64-bit precision and different base type, convert_dtype will convert to int64.
        If given a value that is int32, convert_dtype will convert to int32.
        If given a value with precision <= int32, convert_dtype will convert to int32 and raise a warning.
        """
        spec_type = 'int32'
        value_types = ['int64', 'long']
        self._test_keep_higher_precision_helper(spec_type, value_types)

        value_types = ['double', 'float64', 'uint64']
        expected_type = 'int64'
        self._test_change_basetype_helper(spec_type, value_types, expected_type)

        value_types = ['int', 'int32']
        self._test_convert_alias(spec_type, value_types)

        value_types = ['float', 'float32', 'int16', 'short', 'int8', 'uint', 'uint32', 'uint16', 'uint8', 'bool']
        self._test_convert_higher_precision_helper(spec_type, value_types)

    def test_convert_to_uint32_spec(self):
        """Test conversion of various types to uint32.
        If given a value with precision > uint32 and uint base type, convert_dtype will keep the higher precision.
        If given a value with 64-bit precision and different base type, convert_dtype will convert to uint64.
        If given a value that is uint32, convert_dtype will convert to uint32.
        If given a value with precision <= uint32, convert_dtype will convert to uint32 and raise a warning.
        """
        spec_type = 'uint32'
        value_types = ['uint64']
        self._test_keep_higher_precision_helper(spec_type, value_types)

        value_types = ['double', 'float64', 'long', 'int64']
        expected_type = 'uint64'
        self._test_change_basetype_helper(spec_type, value_types, expected_type)

        value_types = ['uint', 'uint32']
        self._test_convert_alias(spec_type, value_types)

        value_types = ['float', 'float32', 'int', 'int32', 'int16', 'short', 'int8', 'uint16', 'uint8', 'bool']
        self._test_convert_higher_precision_helper(spec_type, value_types)

    def test_convert_to_int16_spec(self):
        """Test conversion of various types to int16.
        If given a value with precision > int16 and int base type, convert_dtype will keep the higher precision.
        If given a value with 64-bit precision and different base type, convert_dtype will convert to int64.
        If given a value with 32-bit precision and different base type, convert_dtype will convert to int32.
        If given a value that is int16, convert_dtype will convert to int16.
        If given a value with precision <= int16, convert_dtype will convert to int16 and raise a warning.
        """
        spec_type = 'int16'
        value_types = ['long', 'int64', 'int', 'int32']
        self._test_keep_higher_precision_helper(spec_type, value_types)

        value_types = ['double', 'float64', 'uint64']
        expected_type = 'int64'
        self._test_change_basetype_helper(spec_type, value_types, expected_type)

        value_types = ['float', 'float32', 'uint', 'uint32']
        expected_type = 'int32'
        self._test_change_basetype_helper(spec_type, value_types, expected_type)

        value_types = ['int16', 'short']
        self._test_convert_alias(spec_type, value_types)

        value_types = ['int8', 'uint16', 'uint8', 'bool']
        self._test_convert_higher_precision_helper(spec_type, value_types)

    def test_convert_to_uint16_spec(self):
        """Test conversion of various types to uint16.
        If given a value with precision > uint16 and uint base type, convert_dtype will keep the higher precision.
        If given a value with 64-bit precision and different base type, convert_dtype will convert to uint64.
        If given a value with 32-bit precision and different base type, convert_dtype will convert to uint32.
        If given a value that is uint16, convert_dtype will convert to uint16.
        If given a value with precision <= uint16, convert_dtype will convert to uint16 and raise a warning.
        """
        spec_type = 'uint16'
        value_types = ['uint64', 'uint', 'uint32']
        self._test_keep_higher_precision_helper(spec_type, value_types)

        value_types = ['double', 'float64', 'long', 'int64']
        expected_type = 'uint64'
        self._test_change_basetype_helper(spec_type, value_types, expected_type)

        value_types = ['float', 'float32', 'int', 'int32']
        expected_type = 'uint32'
        self._test_change_basetype_helper(spec_type, value_types, expected_type)

        value_types = ['uint16']
        self._test_convert_alias(spec_type, value_types)

        value_types = ['int16', 'short', 'int8', 'uint8', 'bool']
        self._test_convert_higher_precision_helper(spec_type, value_types)

    def test_convert_to_bool_spec(self):
        """Test conversion of various types to bool.
        If given a value with type bool, convert_dtype will convert to bool.
        If given a value with type int8/uint8, convert_dtype will convert to bool and raise a warning.
        Otherwise, convert_dtype will raise an error.
        """
        spec_type = 'bool'
        value_types = ['bool']
        self._test_convert_alias(spec_type, value_types)

        value_types = ['uint8', 'int8']
        self._test_convert_higher_precision_helper(spec_type, value_types)

        value_types = ['double', 'float64', 'float', 'float32', 'long', 'int64', 'int', 'int32', 'int16', 'short',
                       'uint64', 'uint', 'uint32', 'uint16']
        self._test_convert_mismatch_helper(spec_type, value_types)

    def _get_type(self, type_str):
        return ObjectMapper._ObjectMapper__dtypes[type_str]  # apply ObjectMapper mapping string to dtype

    def _test_convert_alias(self, spec_type, value_types):
        data = 1
        spec = DatasetSpec('an example dataset', spec_type, name='data')
        match = (self._get_type(spec_type)(data), self._get_type(spec_type))
        for dtype in value_types:
            value = self._get_type(dtype)(data)  # convert data to given dtype
            with self.subTest(dtype=dtype):
                ret = ObjectMapper.convert_dtype(spec, value)
                self.assertTupleEqual(ret, match)
                self.assertIs(ret[0].dtype.type, match[1])

    def _test_convert_higher_precision_helper(self, spec_type, value_types):
        data = 1
        spec = DatasetSpec('an example dataset', spec_type, name='data')
        match = (self._get_type(spec_type)(data), self._get_type(spec_type))
        for dtype in value_types:
            value = self._get_type(dtype)(data)  # convert data to given dtype
            with self.subTest(dtype=dtype):
                s = np.dtype(self._get_type(spec_type))
                g = np.dtype(self._get_type(dtype))
                msg = ("Spec 'data': Value with data type %s is being converted to data type %s as specified."
                       % (g.name, s.name))
                with self.assertWarnsWith(UserWarning, msg):
                    ret = ObjectMapper.convert_dtype(spec, value)
                self.assertTupleEqual(ret, match)
                self.assertIs(ret[0].dtype.type, match[1])

    def _test_keep_higher_precision_helper(self, spec_type, value_types):
        data = 1
        spec = DatasetSpec('an example dataset', spec_type, name='data')
        for dtype in value_types:
            value = self._get_type(dtype)(data)
            match = (value, self._get_type(dtype))
            with self.subTest(dtype=dtype):
                ret = ObjectMapper.convert_dtype(spec, value)
                self.assertTupleEqual(ret, match)
                self.assertIs(ret[0].dtype.type, match[1])

    def _test_change_basetype_helper(self, spec_type, value_types, exp_type):
        data = 1
        spec = DatasetSpec('an example dataset', spec_type, name='data')
        match = (self._get_type(exp_type)(data), self._get_type(exp_type))
        for dtype in value_types:
            value = self._get_type(dtype)(data)  # convert data to given dtype
            with self.subTest(dtype=dtype):
                s = np.dtype(self._get_type(spec_type))
                e = np.dtype(self._get_type(exp_type))
                g = np.dtype(self._get_type(dtype))
                msg = ("Spec 'data': Value with data type %s is being converted to data type %s "
                       "(min specification: %s)." % (g.name, e.name, s.name))
                with self.assertWarnsWith(UserWarning, msg):
                    ret = ObjectMapper.convert_dtype(spec, value)
                self.assertTupleEqual(ret, match)
                self.assertIs(ret[0].dtype.type, match[1])

    def _test_convert_mismatch_helper(self, spec_type, value_types):
        data = 1
        spec = DatasetSpec('an example dataset', spec_type, name='data')
        for dtype in value_types:
            value = self._get_type(dtype)(data)  # convert data to given dtype
            with self.subTest(dtype=dtype):
                s = np.dtype(self._get_type(spec_type))
                g = np.dtype(self._get_type(dtype))
                msg = "expected %s, received %s - must supply %s" % (s.name, g.name, s.name)
                with self.assertRaisesWith(ValueError, msg):
                    ObjectMapper.convert_dtype(spec, value)

    def test_dci_input(self):
        spec = DatasetSpec('an example dataset', 'int64', name='data')
        value = DataChunkIterator(np.array([1, 2, 3], dtype=np.int32))
        msg = "Spec 'data': Value with data type int32 is being converted to data type int64 as specified."
        with self.assertWarnsWith(UserWarning, msg):
            ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)  # no conversion
        self.assertIs(ret, value)
        self.assertEqual(ret_dtype, np.int64)

        spec = DatasetSpec('an example dataset', 'int16', name='data')
        value = DataChunkIterator(np.array([1, 2, 3], dtype=np.int32))
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)  # no conversion
        self.assertIs(ret, value)
        self.assertEqual(ret_dtype, np.int32)  # increase precision

    def test_text_spec(self):
        text_spec_types = ['text', 'utf', 'utf8', 'utf-8']
        for spec_type in text_spec_types:
            with self.subTest(spec_type=spec_type):
                spec = DatasetSpec('an example dataset', spec_type, name='data')

                value = 'a'
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                self.assertEqual(ret, value)
                self.assertIs(type(ret), str)
                self.assertEqual(ret_dtype, 'utf8')

                value = b'a'
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                self.assertEqual(ret, 'a')
                self.assertIs(type(ret), str)
                self.assertEqual(ret_dtype, 'utf8')

                value = ['a', 'b']
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                self.assertListEqual(ret, value)
                self.assertIs(type(ret[0]), str)
                self.assertEqual(ret_dtype, 'utf8')

                value = np.array(['a', 'b'])
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                np.testing.assert_array_equal(ret, value)
                self.assertEqual(ret_dtype, 'utf8')

                value = np.array(['a', 'b'], dtype='S1')
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                np.testing.assert_array_equal(ret, np.array(['a', 'b'], dtype='U1'))
                self.assertEqual(ret_dtype, 'utf8')

                value = []
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                self.assertListEqual(ret, value)
                self.assertEqual(ret_dtype, 'utf8')

                value = 1
                msg = "Expected unicode or ascii string, got <class 'int'>"
                with self.assertRaisesWith(ValueError, msg):
                    ObjectMapper.convert_dtype(spec, value)

                value = DataChunkIterator(np.array(['a', 'b']))
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)  # no conversion
                self.assertIs(ret, value)
                self.assertEqual(ret_dtype, 'utf8')

                value = DataChunkIterator(np.array(['a', 'b'], dtype='S1'))
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)  # no conversion
                self.assertIs(ret, value)
                self.assertEqual(ret_dtype, 'utf8')

    def test_ascii_spec(self):
        ascii_spec_types = ['ascii', 'bytes']
        for spec_type in ascii_spec_types:
            with self.subTest(spec_type=spec_type):
                spec = DatasetSpec('an example dataset', spec_type, name='data')

                value = 'a'
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                self.assertEqual(ret, b'a')
                self.assertIs(type(ret), bytes)
                self.assertEqual(ret_dtype, 'ascii')

                value = b'a'
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                self.assertEqual(ret, b'a')
                self.assertIs(type(ret), bytes)
                self.assertEqual(ret_dtype, 'ascii')

                value = ['a', 'b']
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                self.assertListEqual(ret, [b'a', b'b'])
                self.assertIs(type(ret[0]), bytes)
                self.assertEqual(ret_dtype, 'ascii')

                value = np.array(['a', 'b'])
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                np.testing.assert_array_equal(ret, np.array(['a', 'b'], dtype='S1'))
                self.assertEqual(ret_dtype, 'ascii')

                value = np.array(['a', 'b'], dtype='S1')
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                np.testing.assert_array_equal(ret, value)
                self.assertEqual(ret_dtype, 'ascii')

                value = []
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
                self.assertListEqual(ret, value)
                self.assertEqual(ret_dtype, 'ascii')

                value = 1
                msg = "Expected unicode or ascii string, got <class 'int'>"
                with self.assertRaisesWith(ValueError, msg):
                    ObjectMapper.convert_dtype(spec, value)

                value = DataChunkIterator(np.array(['a', 'b']))
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)  # no conversion
                self.assertIs(ret, value)
                self.assertEqual(ret_dtype, 'ascii')

                value = DataChunkIterator(np.array(['a', 'b'], dtype='S1'))
                ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)  # no conversion
                self.assertIs(ret, value)
                self.assertEqual(ret_dtype, 'ascii')

    def test_no_spec(self):
        spec_type = None
        spec = DatasetSpec('an example dataset', spec_type, name='data')

        value = [1, 2, 3]
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertListEqual(ret, value)
        self.assertIs(type(ret[0]), int)
        self.assertEqual(ret_dtype, int)

        value = np.uint64(4)
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(type(ret), np.uint64)
        self.assertEqual(ret_dtype, np.uint64)

        value = 'hello'
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(type(ret), str)
        self.assertEqual(ret_dtype, 'utf8')

        value = b'hello'
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(type(ret), bytes)
        self.assertEqual(ret_dtype, 'ascii')

        value = np.array(['aa', 'bb'])
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        np.testing.assert_array_equal(ret, value)
        self.assertEqual(ret_dtype, 'utf8')

        value = np.array(['aa', 'bb'], dtype='S2')
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        np.testing.assert_array_equal(ret, value)
        self.assertEqual(ret_dtype, 'ascii')

        value = DataChunkIterator(data=[1, 2, 3])
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(ret.dtype.type, np.dtype(int).type)
        self.assertIs(type(ret.data[0]), int)
        self.assertEqual(ret_dtype, np.dtype(int).type)

        value = DataChunkIterator(data=['a', 'b'])
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(ret.dtype.type, np.str_)
        self.assertIs(type(ret.data[0]), str)
        self.assertEqual(ret_dtype, 'utf8')

        value = H5DataIO(np.arange(30).reshape(5, 2, 3))
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(ret.data.dtype.type, np.dtype(int).type)
        self.assertEqual(ret_dtype, np.dtype(int).type)

        value = H5DataIO(['foo', 'bar'])
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(type(ret.data[0]), str)
        self.assertEqual(ret_dtype, 'utf8')

        value = H5DataIO([b'foo', b'bar'])
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(type(ret.data[0]), bytes)
        self.assertEqual(ret_dtype, 'ascii')

        value = []
        msg = "Cannot infer dtype of empty list or tuple. Please use numpy array with specified dtype."
        with self.assertRaisesWith(ValueError, msg):
            ObjectMapper.convert_dtype(spec, value)

    def test_numeric_spec(self):
        spec_type = 'numeric'
        spec = DatasetSpec('an example dataset', spec_type, name='data')

        value = np.uint64(4)
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(type(ret), np.uint64)
        self.assertEqual(ret_dtype, np.uint64)

        value = DataChunkIterator(data=[1, 2, 3])
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(ret.dtype.type, np.dtype(int).type)
        self.assertIs(type(ret.data[0]), int)
        self.assertEqual(ret_dtype, np.dtype(int).type)

        value = ['a', 'b']
        msg = "Cannot convert from <class 'str'> to 'numeric' specification dtype."
        with self.assertRaisesWith(ValueError, msg):
            ObjectMapper.convert_dtype(spec, value)

        value = np.array(['a', 'b'])
        msg = "Cannot convert from <class 'numpy.str_'> to 'numeric' specification dtype."
        with self.assertRaisesWith(ValueError, msg):
            ObjectMapper.convert_dtype(spec, value)

        value = []
        msg = "Cannot infer dtype of empty list or tuple. Please use numpy array with specified dtype."
        with self.assertRaisesWith(ValueError, msg):
            ObjectMapper.convert_dtype(spec, value)

    def test_bool_spec(self):
        spec_type = 'bool'
        spec = DatasetSpec('an example dataset', spec_type, name='data')

        value = np.bool_(True)
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(type(ret), np.bool_)
        self.assertEqual(ret_dtype, np.bool_)

        value = True
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, value)
        self.assertIs(type(ret), np.bool_)
        self.assertEqual(ret_dtype, np.bool_)

    def test_override_type_int_restrict_precision(self):
        spec = DatasetSpec('an example dataset', 'int8', name='data')
        res = ObjectMapper.convert_dtype(spec, np.int64(1), 'int64')
        self.assertTupleEqual(res, (np.int64(1), np.int64))

    def test_override_type_numeric_to_uint(self):
        spec = DatasetSpec('an example dataset', 'numeric', name='data')
        res = ObjectMapper.convert_dtype(spec, np.uint32(1), 'uint8')
        self.assertTupleEqual(res, (np.uint32(1), np.uint32))

    def test_override_type_numeric_to_uint_list(self):
        spec = DatasetSpec('an example dataset', 'numeric', name='data')
        res = ObjectMapper.convert_dtype(spec, np.uint32((1, 2, 3)), 'uint8')
        np.testing.assert_array_equal(res[0], np.uint32((1, 2, 3)))
        self.assertEqual(res[1], np.uint32)

    def test_override_type_none_to_bool(self):
        spec = DatasetSpec('an example dataset', None, name='data')
        res = ObjectMapper.convert_dtype(spec, True, 'bool')
        self.assertTupleEqual(res, (True, np.bool_))

    def test_compound_type(self):
        """Test that convert_dtype passes through arguments if spec dtype is a list without any validation."""
        spec_type = [DtypeSpec('an int field', 'f1', 'int'), DtypeSpec('a float field', 'f2', 'float')]
        spec = DatasetSpec('an example dataset', spec_type, name='data')
        value = ['a', 1, 2.2]
        res, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertListEqual(res, value)
        self.assertListEqual(ret_dtype, spec_type)

    def test_isodatetime_spec(self):
        spec_type = 'isodatetime'
        spec = DatasetSpec('an example dataset', spec_type, name='data')

        # NOTE: datetime.isoformat is called on all values with a datetime spec before conversion
        # see ObjectMapper.get_attr_value
        value = datetime.isoformat(datetime(2020, 11, 10))
        ret, ret_dtype = ObjectMapper.convert_dtype(spec, value)
        self.assertEqual(ret, b'2020-11-10T00:00:00')
        self.assertIs(type(ret), bytes)
        self.assertEqual(ret_dtype, 'ascii')


class TestObjectMapperBadValue(TestCase):

    def test_bad_value(self):
        """Test that an error is raised if the container attribute value for a spec with a data type is not a container
        or collection of containers.
        """
        class Qux(Container):
            @docval({'name': 'name', 'type': str, 'doc': 'the name of this Qux'},
                    {'name': 'foo', 'type': int, 'doc': 'a group'})
            def __init__(self, **kwargs):
                name, foo = getargs('name', 'foo', kwargs)
                super().__init__(name=name)
                self.__foo = foo
                if isinstance(foo, Foo):
                    self.__foo.parent = self

            @property
            def foo(self):
                return self.__foo

        self.qux_spec = GroupSpec(
            doc='A test group specification with data type Qux',
            data_type_def='Qux',
            groups=[GroupSpec('an example dataset', data_type_inc='Foo')]
        )
        self.foo_spec = GroupSpec('A test group specification with data type Foo', data_type_def='Foo')
        self.spec_catalog = SpecCatalog()
        self.spec_catalog.register_spec(self.qux_spec, 'test.yaml')
        self.spec_catalog.register_spec(self.foo_spec, 'test.yaml')
        self.namespace = SpecNamespace('a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}],
                                       version='0.1.0',
                                       catalog=self.spec_catalog)
        self.namespace_catalog = NamespaceCatalog()
        self.namespace_catalog.add_namespace(CORE_NAMESPACE, self.namespace)
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Qux', Qux)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Foo', Foo)
        self.manager = BuildManager(self.type_map)
        self.mapper = ObjectMapper(self.qux_spec)

        container = Qux('my_qux', foo=1)
        msg = "Qux 'my_qux' attribute 'foo' has unexpected type."
        with self.assertRaisesWith(ContainerConfigurationError, msg):
            self.mapper.build(container, self.manager)

    # TODO test passing a Container/Data/other object for a non-container/data array spec
