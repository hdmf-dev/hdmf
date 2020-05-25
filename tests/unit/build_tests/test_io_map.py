from hdmf.spec import GroupSpec, AttributeSpec, DatasetSpec, SpecCatalog, SpecNamespace, NamespaceCatalog, RefSpec
from hdmf.spec import Spec
from hdmf.build import GroupBuilder, DatasetBuilder, ObjectMapper, BuildManager, TypeMap, LinkBuilder
from hdmf import Container
from hdmf.utils import docval, getargs, get_docval
from hdmf.data_utils import DataChunkIterator
from hdmf.backends.hdf5 import H5DataIO
from hdmf.testing import TestCase

from abc import ABCMeta, abstractmethod
import numpy as np

from tests.unit.utils import CORE_NAMESPACE


class Bar(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Bar'},
            {'name': 'data', 'type': ('data', 'array_data'), 'doc': 'some data'},
            {'name': 'attr1', 'type': str, 'doc': 'an attribute'},
            {'name': 'attr2', 'type': int, 'doc': 'another attribute'},
            {'name': 'attr3', 'type': float, 'doc': 'a third attribute', 'default': 3.14},
            {'name': 'attr5', 'type': bool, 'doc': 'a fourth attribute', 'default': True},
            {'name': 'foo', 'type': 'Foo', 'doc': 'a group', 'default': None},
            {'name': 'bars', 'type': ('data', 'array_data'), 'doc': 'a group', 'default': list()})
    def __init__(self, **kwargs):
        name, data, attr1, attr2, attr3, attr5, foo, bars = getargs('name', 'data', 'attr1', 'attr2', 'attr3',
                                                                    'attr5', 'foo', 'bars', kwargs)
        super().__init__(name=name)
        self.__data = data
        self.__attr1 = attr1
        self.__attr2 = attr2
        self.__attr3 = attr3
        self.__attr5 = attr5
        self.__foo = foo
        if self.__foo is not None and self.__foo.parent is None:
            self.__foo.parent = self
        self.__bars = bars
        for b in bars:
            if b is not None and b.parent is None:
                b.parent = self

    def __eq__(self, other):
        attrs = ('name', 'data', 'attr1', 'attr2', 'attr3', 'attr5', 'foo', 'bars')
        return all(getattr(self, a) == getattr(other, a) for a in attrs)

    def __str__(self):
        attrs = ('name', 'data', 'attr1', 'attr2', 'attr3', 'attr5', 'foo', 'bars')
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
    def attr5(self):
        return self.__attr5

    @property
    def foo(self):
        return self.__foo

    @property
    def bars(self):
        return self.__bars


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
        # self.build_manager = BuildManager(self.type_map)

    def test_get_map_unique_mappers(self):
        self.type_map.register_map(Bar, ObjectMapper)
        self.type_map.register_map(Foo, ObjectMapper)
        bar_inst = Bar('my_bar', list(range(10)), 'value1', 10)
        foo_inst = Foo(name='my_foo')
        bar_mapper = self.type_map.get_map(bar_inst)
        foo_mapper = self.type_map.get_map(foo_inst)
        self.assertIsNot(bar_mapper, foo_mapper)

    def test_get_map(self):
        self.type_map.register_map(Bar, ObjectMapper)
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
        self.type_map.register_map(Bar, ObjectMapper)
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
            if x['name'] != 'foo' and x['name'] != 'attr5' and x['name'] != 'bars':
                received_args.add(x['name'])
                with self.subTest(name=x['name']):
                    self.assertNotIn('default', x)
        self.assertSetEqual(expected_args, received_args)
        self.assertEqual(cls.__name__, 'Baz')
        self.assertTrue(issubclass(cls, Bar))

    def test_dynamic_container_default_name(self):
        baz_spec = GroupSpec('doc', default_name='bingo', data_type_def='Baz')
        self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
        cls = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz')
        inst = cls()
        self.assertEqual(inst.name, 'bingo')

    def test_dynamic_container_creation_defaults(self):
        baz_spec = GroupSpec('A test extension with no Container class',
                             data_type_def='Baz', data_type_inc=self.bar_spec,
                             attributes=[AttributeSpec('attr3', 'an example float attribute', 'float'),
                                         AttributeSpec('attr4', 'another example float attribute', 'float')])
        self.spec_catalog.register_spec(baz_spec, 'extension.yaml')
        cls = self.type_map.get_container_cls(CORE_NAMESPACE, 'Baz')
        expected_args = {'name', 'data', 'attr1', 'attr2', 'attr3', 'attr4', 'attr5', 'foo', 'bars'}
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
        self.type_map.register_map(Bar, ObjectMapper)
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
        expected = GroupBuilder('my_bar', datasets={'data': DatasetBuilder(
            'data', list(range(10)), attributes={'attr2': 10})},
                                attributes={'attr1': 'value1'})
        self._remap_nested_attr()
        builder = self.mapper.build(container_inst, self.manager)
        self.assertDictEqual(builder, expected)

    def test_construct(self):
        ''' Test default mapping functionality when object attributes map to an attribute
        deeper than top-level Builder '''
        expected = Bar('my_bar', list(range(10)), 'value1', 10)
        builder = GroupBuilder('my_bar', datasets={'data': DatasetBuilder(
            'data', list(range(10)), attributes={'attr2': 10})},
                               attributes={'attr1': 'value1', 'data_type': 'Bar', 'namespace': CORE_NAMESPACE,
                                           'object_id': expected.object_id})
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
        self.assertDictEqual(builder, expected)

    def test_build_empty(self):
        ''' Test default mapping functionality when no attributes are nested '''
        container = Bar('my_bar', [], 'value1', 10)
        builder = self.mapper.build(container, self.manager)
        expected = GroupBuilder('my_bar', datasets={'data': DatasetBuilder('data', [])},
                                attributes={'attr1': 'value1', 'attr2': 10})
        self.assertDictEqual(builder, expected)

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


class BarHolder(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Bar'},
            {'name': 'bar_ext', 'type': Bar, 'doc': 'a bar', 'default': None},
            {'name': 'bars', 'type': ('data', 'array_data'), 'doc': 'bars', 'default': list()})
    def __init__(self, **kwargs):
        name, bar_ext, bars = getargs('name', 'bar_ext', 'bars', kwargs)
        super().__init__(name=name)
        self.__bar_ext = bar_ext
        self.__bars = bars
        for b in bars:
            if b is not None and b.parent is None:
                b.parent = self

    @property
    def bar_ext(self):
        return self.__bar_ext

    @property
    def bars(self):
        return self.__bars


class BarExtMapper(ObjectMapper):

    @docval({"name": "spec", "type": Spec, "doc": "the spec to get the attribute value for"},
            {"name": "container", "type": Bar, "doc": "the container to get the attribute value from"},
            {"name": "manager", "type": BuildManager, "doc": "the BuildManager used for managing this build"},
            returns='the value of the attribute')
    def get_attr_value(self, **kwargs):
        ''' Get the value of the attribute corresponding to this spec from the given container '''
        spec, container, manager = getargs('spec', 'container', 'manager', kwargs)

        breakpoint()
        # handle custom mapping of container Units.waveform_rate -> spec Units.waveform_mean.sampling_rate
        if isinstance(container.parent, BarHolder):
            if spec.name == 'attr5':
                return container.attr5
        return super().get_attr_value(**kwargs)


class TestObjectMapperExtAttrs(ObjectMapperMixin, TestCase):
    """
    If the spec defines data_type A using 'data_type_def' and defines another data_type B that includes A using
    'data_type_inc', then the included A spec is an extended (or refined) spec of A - call it A'. The spec of A' can
    change or add attributes to the spec of A. This test ensures that the new attributes are added properly.

    The Bar type and class is the type A, and the BarHolder type and class is the type B which can contain multiple
    A' objects. A' adds an attribute 'attr5' to the spec of A.
    """

    def setUp(self):
        super().setUp()
        self.setUpBarHolderSpec()
        self.spec_catalog.register_spec(self.bar_holder_spec, 'test.yaml')
        self.type_map.register_container_type(CORE_NAMESPACE, 'BarHolder', BarHolder)
        self.type_map.register_map(Bar, BarExtMapper)  # override default ObjectMapper set earlier
        self.type_map.register_map(BarHolder, ObjectMapper)

    def setUpBarSpec(self):
        data_dset = DatasetSpec(
            name='data',
            dtype='int',
            doc='an example dataset',
        )
        attr1_attr = AttributeSpec(
            name='attr1',
            dtype='text',
            doc='an example string attribute',
        )
        self.bar_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Bar',
            datasets=[data_dset],
            attributes=[attr1_attr],
        )

    def setUpBarHolderSpec(self):
        """
        BarHolder may contain the named bar_ext group which extends Bar with an additional subgroup of any number
        of extended Bar objects. Both extended Bar objects have an additional attribute.

        """
        attr5_attr = AttributeSpec(
            name='attr5',
            dtype='bool',
            doc='A boolean attribute',
        )
        bar_ext_no_name_spec = GroupSpec(
            doc='A Bar extended with attribute attr5',
            data_type_inc='Bar',
            quantity='*',
            attributes=[attr5_attr],
        )
        self.bar_holder_spec = GroupSpec(
            doc='A container of multiple extended Bar objects',
            data_type_def='BarHolder',
            groups=[bar_ext_no_name_spec],
        )

    def test_build_bar_holder(self):
        ext_bar2_inst = Bar(
            name='my_bar',
            data=list(range(10)),
            attr1='a string',
            attr2=10,
            attr5=False,
        )
        bar_holder_inst = BarHolder(
            name='my_bar_holder',
            bars=[ext_bar2_inst]
        )

        expected_inner = GroupBuilder(
            name='my_bar',
            datasets={'data': DatasetBuilder(
                name='data',
                data=list(range(10))
            )},
            attributes={'attr1': 'a string',
                        'attr2': 10,
                        'attr5': False,
                        'data_type': 'Bar',
                        'namespace': CORE_NAMESPACE,
                        'object_id': ext_bar2_inst.object_id}
        )
        expected = GroupBuilder(
            name='my_bar_holder',
            groups={'my_bar': expected_inner})

        bar_holder_mapper = ObjectMapper(self.bar_holder_spec)
        bar_group_spec = bar_holder_mapper.spec.get_data_type('Bar')
        breakpoint()
        bar_holder_mapper.map_spec('bars', bar_group_spec)  # map BarHolder.bars to the included extended bar types
        builder = bar_holder_mapper.build(bar_holder_inst, self.manager)
        breakpoint()
        self.assertDictEqual(builder, expected)


class TestObjectMapperExtAttrs2(ObjectMapperMixin, TestCase):
    """
    If the spec defines data_type A using 'data_type_def' and defines another data_type B that includes A using
    'data_type_inc', then the included A spec is an extended (or refined) spec of A - call it A'. The spec of A' can
    change or add attributes to the spec of A. This test ensures that the new attributes are added properly.

    The Bar type and class is the type A, and the BarHolder type and class is the type B which can contain multiple
    A' objects. A' adds an attribute 'attr5' to the spec of A.
    """

    def setUp(self):
        super().setUp()
        self.setUpBarExtSpec()
        self.spec_catalog.register_spec(self.bar_holder_spec, 'test.yaml')
        self.type_map.register_container_type(CORE_NAMESPACE, 'BarHolder', BarHolder)
        self.type_map.register_map(BarHolder, ObjectMapper)

        import logging

        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)

        ch = logging.FileHandler('test.log', mode='w')
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    def setUpBarSpec(self):
        data_dset = DatasetSpec(
            doc='an example dataset',
            dtype='int',
            name='data'
        )
        attr1_attr = AttributeSpec(
            doc='an example string attribute',
            dtype='text',
            name='attr1'
        )
        self.bar_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Bar',
            datasets=[data_dset],
            attributes=[attr1_attr]
        )

    def setUpBarExtAttrs(self):
        """
        BarHolder may contain the named bar_ext group which extends Bar with an additional subgroup of any number
        of extended Bar objects. Both extended Bar objects have an additional attribute.

        TODO do the same with dataset but with or without extending the dtype
        """
        self.bar_ext_no_name_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_inc='Bar',
            quantity='*',
            attributes=[AttributeSpec('attr5', 'an example boolean attribute', 'bool')]
        )
        # self.bar_ext_spec = GroupSpec(
        #     doc='A test group specification with a data type',
        #     data_type_inc='Bar',
        #     name='bar_ext',
        #     quantity='?',
        #     groups=[self.bar_ext_no_name_spec],
        #     attributes=[AttributeSpec('attr3', 'an example float attribute', 'float')]
        # )
        self.bar_holder_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='BarHolder',
            groups=[self.bar_ext_no_name_spec]
        )

    def test_build_bar_holder(self):
        ''' Test default mapping functionality when object attributes map to an attribute deeper
        than top-level Builder '''
        ext_bar2_inst = Bar(
            name='my_bar',
            data=list(range(10)),
            attr1='value_inner_inner',
            attr2=10,
            attr5=False
        )
        # ext_bar1_inst = Bar(
        #     name='my_bar',
        #     data=list(range(10)),
        #     attr1='value_inner',
        #     attr2=10,
        #     attr3=5.,
        #     bars=[ext_bar2_inst]
        # )
        # extended type cannot have new groups
        bar_holder_inst = BarHolder(
            name='my_bar_holder',
            # bar_ex=ext_bar1_inst
            bars=[ext_bar2_inst]
        )
        expected_inner_inner = GroupBuilder(
            name='my_bar',
            datasets={'data': DatasetBuilder(
                name='data',
                data=list(range(10))
            )},
            attributes={'attr1': 'value_inner_inner',
                        'attr2': 10,
                        'attr5': False,
                        'data_type': 'Bar',
                        'namespace': CORE_NAMESPACE,
                        'object_id': ext_bar2_inst.object_id}
        )
        # expected_inner = GroupBuilder(
        #     name='bar_ext',
        #     datasets={'data': DatasetBuilder(
        #         name='data',
        #         data=list(range(10))
        #     )},
        #     attributes={'attr1': 'value_inner',
        #                 'attr2': 10,
        #                 'attr3': 5.,
        #                 'data_type': 'BarHolder',
        #                 'namespace': CORE_NAMESPACE,
        #                 'object_id': ext_bar1_inst.object_id},
        #     groups={'bars': {'my_bar': expected_inner_inner}}
        # )
        expected = GroupBuilder(
            name='my_bar_holder',
            # groups={'bar_ext': expected_inner},
            groups={'my_bar': expected_inner_inner})
        bar_holder_mapper = ObjectMapper(self.bar_holder_spec)
        bar_group_spec = bar_holder_mapper.spec.get_data_type('Bar')
        bar_holder_mapper.map_spec('bars', bar_group_spec)
        builder = bar_holder_mapper.build(bar_holder_inst, self.manager)
        self.assertDictEqual(builder, expected)
        # TODO builder missing extended attributes


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
        self.type_map.register_map(Foo, ObjectMapper)
        self.type_map.register_map(Bar, ObjectMapper)
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
        bar1_expected = GroupBuilder('n/a',  # name doesn't matter
                                     datasets={'data': DatasetBuilder('data', list(range(10)))},
                                     groups={'foo': inner_foo_builder},
                                     attributes={'attr1': 'value1', 'attr2': 10})
        link_foo_builder = LinkBuilder(builder=inner_foo_builder)
        bar2_expected = GroupBuilder('n/a',
                                     datasets={'data': DatasetBuilder('data', list(range(10)))},
                                     links={'foo': link_foo_builder},
                                     attributes={'attr1': 'value1', 'attr2': 10})
        self.assertDictEqual(foo_builder, foo_expected)
        self.assertDictEqual(bar1_builder, bar1_expected)
        self.assertDictEqual(bar2_builder, bar2_expected)


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
        value_types = ['float', 'float32', 'long', 'int64', 'int', 'int32', 'int16', 'int8', 'uint64', 'uint',
                       'uint32', 'uint16', 'uint8', 'bool']
        self._test_convert_higher_precision_helper(spec_type, value_types)

        spec_type = 'int64'
        value_types = ['long', 'int64']
        self._test_convert_alias(spec_type, value_types)

        spec_type = 'int64'
        value_types = ['double', 'float64', 'float', 'float32', 'int', 'int32', 'int16', 'int8', 'uint64', 'uint',
                       'uint32', 'uint16', 'uint8', 'bool']
        self._test_convert_higher_precision_helper(spec_type, value_types)

        spec_type = 'uint64'
        value_types = ['uint64']
        self._test_convert_alias(spec_type, value_types)

        spec_type = 'uint64'
        value_types = ['double', 'float64', 'float', 'float32', 'long', 'int64', 'int', 'int32', 'int16', 'int8',
                       'uint', 'uint32', 'uint16', 'uint8', 'bool']
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

        value_types = ['int', 'int32', 'int16', 'int8', 'uint', 'uint32', 'uint16', 'uint8', 'bool']
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

        value_types = ['float', 'float32', 'int16', 'int8', 'uint', 'uint32', 'uint16', 'uint8', 'bool']
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

        value_types = ['float', 'float32', 'int', 'int32', 'int16', 'int8', 'uint16', 'uint8', 'bool']
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

        value_types = ['int16']
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

        value_types = ['int16', 'int8', 'uint8', 'bool']
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

        value_types = ['double', 'float64', 'float', 'float32', 'long', 'int64', 'int', 'int32', 'int16', 'uint64',
                       'uint', 'uint32', 'uint16']
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

    def test_no_spec(self):
        spec_type = None
        spec = DatasetSpec('an example dataset', spec_type, name='data')

        value = [1, 2, 3]
        ret = ObjectMapper.convert_dtype(spec, value)
        match = (value, int)
        self.assertTupleEqual(ret, match)
        self.assertIs(type(ret[0][0]), match[1])

        value = np.uint64(4)
        ret = ObjectMapper.convert_dtype(spec, value)
        match = (value, np.uint64)
        self.assertTupleEqual(ret, match)
        self.assertIs(type(ret[0]), match[1])

        value = 'hello'
        ret = ObjectMapper.convert_dtype(spec, value)
        match = (value, 'utf8')
        self.assertTupleEqual(ret, match)
        self.assertIs(type(ret[0]), str)

        value = bytes('hello', encoding='utf-8')
        ret = ObjectMapper.convert_dtype(spec, value)
        match = (value, 'ascii')
        self.assertTupleEqual(ret, match)
        self.assertIs(type(ret[0]), bytes)

        value = DataChunkIterator(data=[1, 2, 3])
        ret = ObjectMapper.convert_dtype(spec, value)
        match = (value, np.dtype(int).type)
        self.assertTupleEqual(ret, match)
        self.assertIs(ret[0].dtype.type, match[1])

        value = DataChunkIterator(data=[1., 2., 3.])
        ret = ObjectMapper.convert_dtype(spec, value)
        match = (value, np.dtype(float).type)
        self.assertTupleEqual(ret, match)
        self.assertIs(ret[0].dtype.type, match[1])

        value = H5DataIO(np.arange(30).reshape(5, 2, 3))
        ret = ObjectMapper.convert_dtype(spec, value)
        match = (value, np.dtype(int).type)
        self.assertTupleEqual(ret, match)
        self.assertIs(ret[0].dtype.type, match[1])

        value = H5DataIO(['foo' 'bar'])
        ret = ObjectMapper.convert_dtype(spec, value)
        match = (value, 'utf8')
        self.assertTupleEqual(ret, match)
        self.assertIs(type(ret[0].data[0]), str)

    def test_numeric_spec(self):
        spec_type = 'numeric'
        spec = DatasetSpec('an example dataset', spec_type, name='data')

        value = np.uint64(4)
        ret = ObjectMapper.convert_dtype(spec, value)
        match = (value, np.uint64)
        self.assertTupleEqual(ret, match)
        self.assertIs(type(ret[0]), match[1])

        value = DataChunkIterator(data=[1, 2, 3])
        ret = ObjectMapper.convert_dtype(spec, value)
        match = (value, np.dtype(int).type)
        self.assertTupleEqual(ret, match)
        self.assertIs(ret[0].dtype.type, match[1])

    def test_bool_spec(self):
        spec_type = 'bool'
        spec = DatasetSpec('an example dataset', spec_type, name='data')

        value = np.bool_(True)
        ret = ObjectMapper.convert_dtype(spec, value)
        match = (value, np.bool_)
        self.assertTupleEqual(ret, match)
        self.assertIs(type(ret[0]), match[1])

        value = True
        ret = ObjectMapper.convert_dtype(spec, value)
        match = (value, np.bool_)
        self.assertTupleEqual(ret, match)
        self.assertIs(type(ret[0]), match[1])

    def test_override_type_int_restrict_precision(self):
        spec = DatasetSpec('an example dataset', 'int8', name='data')
        res = ObjectMapper.convert_dtype(spec, 1, 'int64')
        self.assertTupleEqual(res, (np.int64(1), np.int64))

    def test_override_type_numeric_to_uint(self):
        spec = DatasetSpec('an example dataset', 'numeric', name='data')
        res = ObjectMapper.convert_dtype(spec, 1, 'uint8')
        self.assertTupleEqual(res, (np.uint32(1), np.uint32))

    def test_override_type_numeric_to_uint_list(self):
        spec = DatasetSpec('an example dataset', 'numeric', name='data')
        res = ObjectMapper.convert_dtype(spec, (1, 2, 3), 'uint8')
        np.testing.assert_array_equal(res[0], np.uint32((1, 2, 3)))
        self.assertEqual(res[1], np.uint32)

    def test_override_type_none_to_bool(self):
        spec = DatasetSpec('an example dataset', None, name='data')
        res = ObjectMapper.convert_dtype(spec, True, 'bool')
        self.assertTupleEqual(res, (True, np.bool_))
