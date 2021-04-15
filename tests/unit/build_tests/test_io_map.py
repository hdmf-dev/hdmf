import unittest
from abc import ABCMeta, abstractmethod

from hdmf import Container
from hdmf.backends.hdf5 import H5DataIO
from hdmf.build import (GroupBuilder, DatasetBuilder, ObjectMapper, BuildManager, TypeMap, LinkBuilder,
                        ReferenceBuilder, MissingRequiredBuildWarning, OrphanContainerBuildError,
                        ContainerConfigurationError)
from hdmf.spec import GroupSpec, AttributeSpec, DatasetSpec, SpecCatalog, SpecNamespace, NamespaceCatalog, RefSpec
from hdmf.testing import TestCase
from hdmf.utils import docval, getargs

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
