import unittest2 as unittest
import re

from hdmf.spec import GroupSpec, AttributeSpec, DatasetSpec, SpecCatalog, SpecNamespace, NamespaceCatalog
from hdmf.build import GroupBuilder, DatasetBuilder, ObjectMapper, BuildManager, TypeMap, LinkBuilder
from hdmf import Container
from hdmf.utils import docval, getargs, get_docval

from abc import ABCMeta
from six import with_metaclass

from tests.unit.test_utils import CORE_NAMESPACE


class Bar(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this Bar'},
            {'name': 'data', 'type': list, 'doc': 'some data'},
            {'name': 'attr1', 'type': str, 'doc': 'an attribute'},
            {'name': 'attr2', 'type': int, 'doc': 'another attribute'},
            {'name': 'attr3', 'type': float, 'doc': 'a third attribute', 'default': 3.14},
            {'name': 'foo', 'type': 'Foo', 'doc': 'a group', 'default': None})
    def __init__(self, **kwargs):
        name, data, attr1, attr2, attr3, foo = getargs('name', 'data', 'attr1', 'attr2', 'attr3', 'foo', kwargs)
        super(Bar, self).__init__(name=name)
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


class Foo(Container):

    @property
    def data_type(self):
        return 'Foo'


class TestGetSubSpec(unittest.TestCase):

    def setUp(self):
        self.bar_spec = GroupSpec('A test group specification with a data type', data_type_def='Bar')
        spec_catalog = SpecCatalog()
        spec_catalog.register_spec(self.bar_spec, 'test.yaml')
        namespace = SpecNamespace('a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}], catalog=spec_catalog)
        namespace_catalog = NamespaceCatalog()
        namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)
        self.type_map = TypeMap(namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Bar', Bar)

    def test_get_subspec_data_type_noname(self):
        parent_spec = GroupSpec('Something to hold a Bar', 'bar_bucket', groups=[self.bar_spec])
        sub_builder = GroupBuilder('my_bar', attributes={'data_type': 'Bar', 'namespace': CORE_NAMESPACE,
                                                         'object_id': -1})
        builder = GroupBuilder('bar_bucket', groups={'my_bar': sub_builder})  # noqa: F841
        result = self.type_map.get_subspec(parent_spec, sub_builder)
        self.assertIs(result, self.bar_spec)

    def test_get_subspec_named(self):
        child_spec = GroupSpec('A test group specification with a data type', 'my_subgroup')
        parent_spec = GroupSpec('Something to hold a Bar', 'my_group', groups=[child_spec])
        sub_builder = GroupBuilder('my_subgroup', attributes={'data_type': 'Bar', 'namespace': CORE_NAMESPACE,
                                                              'object_id': -1})
        builder = GroupBuilder('my_group', groups={'my_bar': sub_builder})  # noqa: F841
        result = self.type_map.get_subspec(parent_spec, sub_builder)
        self.assertIs(result, child_spec)


class TestTypeMap(unittest.TestCase):

    def setUp(self):
        self.bar_spec = GroupSpec('A test group specification with a data type', data_type_def='Bar')
        self.foo_spec = GroupSpec('A test group specification with data type Foo', data_type_def='Foo')
        self.spec_catalog = SpecCatalog()
        self.spec_catalog.register_spec(self.bar_spec, 'test.yaml')
        self.spec_catalog.register_spec(self.foo_spec, 'test.yaml')
        self.namespace = SpecNamespace('a test namespace', CORE_NAMESPACE, [{'source': 'test.yaml'}],
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


class TestDynamicContainer(unittest.TestCase):

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
                                       [{'source': 'test.yaml'}], catalog=self.spec_catalog)
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
            if x['name'] != 'foo':
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

    def test_dynamic_container_composition_wrong_order(self):
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

        # Setup all the data we need
        msg = ("Cannot dynamically generate class for type 'Baz1'. Type 'Baz2' does not exist. "
               "Please define that type before defining 'Baz1'.")
        with self.assertRaisesRegex(ValueError, re.escape(msg)):
            self.manager.type_map.get_container_cls(CORE_NAMESPACE, 'Baz1')


class TestObjectMapper(with_metaclass(ABCMeta, unittest.TestCase)):

    def setUp(self):
        self.setUpBarSpec()
        self.spec_catalog = SpecCatalog()
        self.spec_catalog.register_spec(self.bar_spec, 'test.yaml')
        self.namespace = SpecNamespace('a test namespace', CORE_NAMESPACE,
                                       [{'source': 'test.yaml'}], catalog=self.spec_catalog)
        self.namespace_catalog = NamespaceCatalog()
        self.namespace_catalog.add_namespace(CORE_NAMESPACE, self.namespace)
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Bar', Bar)
        self.type_map.register_map(Bar, ObjectMapper)
        self.manager = BuildManager(self.type_map)
        self.mapper = ObjectMapper(self.bar_spec)

    def setUpBarSpec(self):
        raise unittest.SkipTest('setUpBarSpec not implemented')

    def test_default_mapping(self):
        attr_map = self.mapper.get_attr_names(self.bar_spec)
        keys = set(attr_map.keys())
        for key in keys:
            with self.subTest(key=key):
                self.assertIs(attr_map[key], self.mapper.get_attr_spec(key))
                self.assertIs(attr_map[key], self.mapper.get_carg_spec(key))


class TestObjectMapperNested(TestObjectMapper):

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
        container = self.mapper.construct(builder, self.manager)
        self.assertEqual(container, expected)

    def test_default_mapping_keys(self):
        attr_map = self.mapper.get_attr_names(self.bar_spec)
        keys = set(attr_map.keys())
        expected = {'attr1', 'data', 'attr2'}
        self.assertSetEqual(keys, expected)


class TestObjectMapperNoNesting(TestObjectMapper):

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


class TestObjectMapperContainer(TestObjectMapper):

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


class TestLinkedContainer(unittest.TestCase):

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
                                       [{'source': 'test.yaml'}], catalog=self.spec_catalog)
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


if __name__ == '__main__':
    unittest.main()
