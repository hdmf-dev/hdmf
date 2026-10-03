import os
import shutil
import tempfile
from abc import ABCMeta, abstractmethod
from copy import copy

from hdmf.build import GroupBuilder, DatasetBuilder, ObjectMapper, BuildManager, TypeMap, ContainerConfigurationError
from hdmf.build.manager import TypeSource
from hdmf.spec import GroupSpec, AttributeSpec, DatasetSpec, SpecCatalog, SpecNamespace, NamespaceCatalog
from hdmf.spec.spec import ZERO_OR_MANY
from hdmf.testing import TestCase

from tests.unit.helpers.utils import Foo, FooBucket, CORE_NAMESPACE, create_load_namespace_yaml


class FooMapper(ObjectMapper):
    """Maps nested 'attr2' attribute on dataset 'my_data' to Foo.attr2 in constructor and attribute map
    """

    def __init__(self, spec):
        super().__init__(spec)
        my_data_spec = spec.get_dataset('my_data')
        self.map_spec('attr2', my_data_spec.get_attribute('attr2'))


class TestBase(TestCase):

    def setUp(self):
        self.foo_spec = GroupSpec(
            doc='A test group specification with a data type',
            data_type_def='Foo',
            datasets=[
                DatasetSpec(
                    doc='an example dataset',
                    dtype='int',
                    name='my_data',
                    attributes=[
                        AttributeSpec(
                            name='attr2',
                            doc='an example integer attribute',
                            dtype='int'
                        )
                    ]
                )
            ],
            attributes=[AttributeSpec('attr1', 'an example string attribute', 'text')]
        )

        self.spec_catalog = SpecCatalog()
        self.spec_catalog.register_spec(self.foo_spec, 'test.yaml')
        self.namespace = SpecNamespace(
            'a test namespace',
            CORE_NAMESPACE,
            [{'source': 'test.yaml'}],
            version='0.1.0',
            catalog=self.spec_catalog)
        self.namespace_catalog = NamespaceCatalog()
        self.namespace_catalog.add_namespace(CORE_NAMESPACE, self.namespace)
        self.type_map = TypeMap(self.namespace_catalog)
        self.type_map.register_container_type(CORE_NAMESPACE, 'Foo', Foo)
        self.type_map.register_map(Foo, FooMapper)
        self.manager = BuildManager(self.type_map)


class TestBuildManager(TestBase):

    def test_build(self):
        container_inst = Foo('my_foo', list(range(10)), 'value1', 10)
        expected = GroupBuilder(
            'my_foo',
            datasets={
                'my_data':
                DatasetBuilder(
                    'my_data',
                    list(range(10)),
                    attributes={'attr2': 10})},
            attributes={'attr1': 'value1', 'namespace': CORE_NAMESPACE, 'data_type': 'Foo',
                        'object_id': container_inst.object_id})
        builder1 = self.manager.build(container_inst)
        self.assertDictEqual(builder1, expected)

    def test_build_memoization(self):
        container_inst = Foo('my_foo', list(range(10)), 'value1', 10)
        expected = GroupBuilder(
            'my_foo',
            datasets={
                'my_data': DatasetBuilder(
                    'my_data',
                    list(range(10)),
                    attributes={'attr2': 10})},
            attributes={'attr1': 'value1', 'namespace': CORE_NAMESPACE, 'data_type': 'Foo',
                        'object_id': container_inst.object_id})
        builder1 = self.manager.build(container_inst)
        builder2 = self.manager.build(container_inst)
        self.assertDictEqual(builder1, expected)
        self.assertIs(builder1, builder2)

    def test_construct(self):
        builder = GroupBuilder(
            'my_foo',
            datasets={
                'my_data': DatasetBuilder(
                    'my_data',
                    list(range(10)),
                    attributes={'attr2': 10})},
            attributes={'attr1': 'value1', 'namespace': CORE_NAMESPACE, 'data_type': 'Foo',
                        'object_id': -1})
        container = self.manager.construct(builder)
        self.assertListEqual(container.my_data, list(range(10)))
        self.assertEqual(container.attr1, 'value1')
        self.assertEqual(container.attr2, 10)

    def test_construct_memoization(self):
        builder = GroupBuilder(
            'my_foo', datasets={'my_data': DatasetBuilder(
                'my_data',
                list(range(10)),
                attributes={'attr2': 10})},
            attributes={'attr1': 'value1', 'namespace': CORE_NAMESPACE, 'data_type': 'Foo',
                        'object_id': -1})
        container1 = self.manager.construct(builder)
        container2 = self.manager.construct(builder)
        self.assertIs(container1, container2)

    def test_clear_cache(self):
        container_inst = Foo('my_foo', list(range(10)), 'value1', 10)
        builder1 = self.manager.build(container_inst)
        self.manager.clear_cache()
        builder2 = self.manager.build(container_inst)
        self.assertIsNot(builder1, builder2)

        builder = GroupBuilder(
            'my_foo', datasets={'my_data': DatasetBuilder(
                'my_data',
                list(range(10)),
                attributes={'attr2': 10})},
            attributes={'attr1': 'value1', 'namespace': CORE_NAMESPACE, 'data_type': 'Foo',
                        'object_id': -1})
        container1 = self.manager.construct(builder)
        self.manager.clear_cache()
        container2 = self.manager.construct(builder)
        self.assertIsNot(container1, container2)


class NestedBaseMixin(metaclass=ABCMeta):

    def setUp(self):
        super().setUp()
        self.foo_bucket = FooBucket('test_foo_bucket', [
                            Foo('my_foo1', list(range(10)), 'value1', 10),
                            Foo('my_foo2', list(range(10, 20)), 'value2', 20)])
        self.foo_builders = {
            'my_foo1': GroupBuilder('my_foo1',
                                    datasets={'my_data': DatasetBuilder(
                                        'my_data',
                                        list(range(10)),
                                        attributes={'attr2': 10})},
                                    attributes={'attr1': 'value1', 'namespace': CORE_NAMESPACE, 'data_type': 'Foo',
                                                'object_id': self.foo_bucket.foos['my_foo1'].object_id}),
            'my_foo2': GroupBuilder('my_foo2', datasets={'my_data':
                                                         DatasetBuilder(
                                                             'my_data',
                                                             list(range(10, 20)),
                                                             attributes={'attr2': 20})},
                                    attributes={'attr1': 'value2', 'namespace': CORE_NAMESPACE, 'data_type': 'Foo',
                                                'object_id': self.foo_bucket.foos['my_foo2'].object_id})
        }
        self.setUpBucketBuilder()
        self.setUpBucketSpec()

        self.spec_catalog.register_spec(self.bucket_spec, 'test.yaml')
        self.type_map.register_container_type(CORE_NAMESPACE, 'FooBucket', FooBucket)
        self.type_map.register_map(FooBucket, self.setUpBucketMapper())
        self.manager = BuildManager(self.type_map)

    @abstractmethod
    def setUpBucketBuilder(self):
        raise NotImplementedError('Cannot run test unless setUpBucketBuilder is implemented')

    @abstractmethod
    def setUpBucketSpec(self):
        raise NotImplementedError('Cannot run test unless setUpBucketSpec is implemented')

    @abstractmethod
    def setUpBucketMapper(self):
        raise NotImplementedError('Cannot run test unless setUpBucketMapper is implemented')

    def test_build(self):
        ''' Test default mapping for an Container that has an Container as an attribute value '''
        builder = self.manager.build(self.foo_bucket)
        self.assertDictEqual(builder, self.bucket_builder)

    def test_construct(self):
        container = self.manager.construct(self.bucket_builder)
        self.assertEqual(container, self.foo_bucket)


class TestNestedContainersNoSubgroups(NestedBaseMixin, TestBase):
    '''
        Test BuildManager.build and BuildManager.construct when the
        Container contains other Containers, but does not keep them in
        additional subgroups
    '''

    def setUpBucketBuilder(self):
        self.bucket_builder = GroupBuilder(
            'test_foo_bucket',
            groups=self.foo_builders,
            attributes={'namespace': CORE_NAMESPACE, 'data_type': 'FooBucket', 'object_id': self.foo_bucket.object_id})

    def setUpBucketSpec(self):
        self.bucket_spec = GroupSpec('A test group specification for a data type containing data type',
                                     name="test_foo_bucket",
                                     data_type_def='FooBucket',
                                     groups=[GroupSpec(
                                         'the Foos in this bucket',
                                         data_type_inc='Foo',
                                         quantity=ZERO_OR_MANY)])

    def setUpBucketMapper(self):
        return ObjectMapper


class TestNestedContainersSubgroup(NestedBaseMixin, TestBase):
    '''
        Test BuildManager.build and BuildManager.construct when the
        Container contains other Containers that are stored in a subgroup
    '''

    def setUpBucketBuilder(self):
        tmp_builder = GroupBuilder('foo_holder', groups=self.foo_builders)
        self.bucket_builder = GroupBuilder(
            'test_foo_bucket',
            groups={'foos': tmp_builder},
            attributes={'namespace': CORE_NAMESPACE, 'data_type': 'FooBucket', 'object_id': self.foo_bucket.object_id})

    def setUpBucketSpec(self):
        tmp_spec = GroupSpec(
            'A subgroup for Foos',
            name='foo_holder',
            groups=[GroupSpec('the Foos in this bucket',
                              data_type_inc='Foo',
                              quantity=ZERO_OR_MANY)])
        self.bucket_spec = GroupSpec('A test group specification for a data type containing data type',
                                     name="test_foo_bucket",
                                     data_type_def='FooBucket',
                                     groups=[tmp_spec])

    def setUpBucketMapper(self):
        class BucketMapper(ObjectMapper):
            def __init__(self, spec):
                super().__init__(spec)
                self.unmap(spec.get_group('foo_holder'))
                self.map_spec('foos', spec.get_group('foo_holder').get_data_type('Foo'))

        return BucketMapper


class TestNestedContainersSubgroupSubgroup(NestedBaseMixin, TestBase):
    '''
        Test BuildManager.build and BuildManager.construct when the
        Container contains other Containers that are stored in a subgroup
        in a subgroup
    '''

    def setUpBucketBuilder(self):
        tmp_builder = GroupBuilder('foo_holder', groups=self.foo_builders)
        tmp_builder = GroupBuilder('foo_holder_holder', groups={'foo_holder': tmp_builder})
        self.bucket_builder = GroupBuilder(
            'test_foo_bucket',
            groups={'foo_holder': tmp_builder},
            attributes={'namespace': CORE_NAMESPACE, 'data_type': 'FooBucket', 'object_id': self.foo_bucket.object_id})

    def setUpBucketSpec(self):
        tmp_spec = GroupSpec('A subgroup for Foos',
                             name='foo_holder',
                             groups=[GroupSpec('the Foos in this bucket',
                                               data_type_inc='Foo',
                                               quantity=ZERO_OR_MANY)])
        tmp_spec = GroupSpec('A subgroup to hold the subgroup', name='foo_holder_holder', groups=[tmp_spec])
        self.bucket_spec = GroupSpec('A test group specification for a data type containing data type',
                                     name="test_foo_bucket",
                                     data_type_def='FooBucket',
                                     groups=[tmp_spec])

    def setUpBucketMapper(self):
        class BucketMapper(ObjectMapper):
            def __init__(self, spec):
                super().__init__(spec)
                self.unmap(spec.get_group('foo_holder_holder'))
                self.unmap(spec.get_group('foo_holder_holder').get_group('foo_holder'))
                self.map_spec('foos', spec.get_group('foo_holder_holder').get_group('foo_holder').get_data_type('Foo'))

        return BucketMapper

    def test_build(self):
        ''' Test default mapping for an Container that has an Container as an attribute value '''
        builder = self.manager.build(self.foo_bucket)
        self.assertDictEqual(builder, self.bucket_builder)

    def test_construct(self):
        container = self.manager.construct(self.bucket_builder)
        self.assertEqual(container, self.foo_bucket)


class TestNoAttribute(TestBase):

    def test_build(self):
        """Test that an error is raised when a spec is mapped to a non-existent container attribute."""
        class Unmapper(ObjectMapper):
            def __init__(self, spec):
                super().__init__(spec)
                self.map_spec("unknown", self.spec.get_dataset('my_data'))

        self.type_map.register_map(Foo, Unmapper)  # override

        container_inst = Foo('my_foo', list(range(10)), 'value1', 10)
        msg = ("Foo 'my_foo' does not have attribute 'unknown' for mapping to spec: %s"
               % self.foo_spec.get_dataset('my_data'))
        with self.assertRaisesWith(ContainerConfigurationError, msg):
            self.manager.build(container_inst)


class TestTypeMap(TestBase):

    def test_get_ns_dt_missing(self):
        bldr = GroupBuilder('my_foo', attributes={'attr1': 'value1'})
        dt = self.type_map.get_builder_dt(bldr)
        ns = self.type_map.get_builder_ns(bldr)
        self.assertIsNone(dt)
        self.assertIsNone(ns)

    def test_get_ns_dt(self):
        bldr = GroupBuilder('my_foo', attributes={'attr1': 'value1', 'namespace': 'CORE', 'data_type': 'Foo',
                                                  'object_id': -1})
        dt = self.type_map.get_builder_dt(bldr)
        ns = self.type_map.get_builder_ns(bldr)
        self.assertEqual(dt, 'Foo')
        self.assertEqual(ns, 'CORE')


class TestRetrieveContainerClass(TestBase):

    def test_get_dt_container_cls(self):
        ret = self.type_map.get_dt_container_cls(data_type="Foo")
        self.assertIs(ret, Foo)

    def test_get_dt_container_cls_no_namespace(self):
        with self.assertRaisesWith(ValueError, "Namespace could not be resolved for data type 'Unknown'."):
            self.type_map.get_dt_container_cls(data_type="Unknown")


class TestRetrieveContainerClassWithTypeSource(TestCase):
    """Test that get_dt_container_cls skips TypeSource entries when resolving namespace."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.type_map = TypeMap()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_get_dt_container_cls_skips_typesource(self):
        """Test that namespace lookup skips TypeSource entries and finds the real class."""
        # Create ns1 with type Bar
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        create_load_namespace_yaml('ns1', [bar_spec], self.test_dir, {}, self.type_map)

        # Create ns2 that includes Bar from ns1 - this registers a TypeSource for Bar in ns2
        create_load_namespace_yaml('ns2', [], self.test_dir, {'ns1': ['Bar']}, self.type_map)

        # ns2 should have a TypeSource for Bar (registered before ns1's class is generated)
        self.assertIsInstance(self.type_map.get_dt_container_cls('Bar', 'ns2', autogen=False), TypeSource)

        # Register actual class for Bar in ns1
        Bar = self.type_map.get_dt_container_cls('Bar', 'ns1')

        # Lookup Bar without namespace - should skip ns2's TypeSource and find ns1
        ret = self.type_map.get_dt_container_cls(data_type='Bar')
        self.assertIs(ret, Bar)

        # Class should be associated with ns1, not ns2
        ns, _ = self.type_map.get_container_cls_dt(Bar)
        self.assertEqual(ns, 'ns1')

        # ns2 should still have TypeSource (not resolved to Bar)
        self.assertIsInstance(self.type_map.get_dt_container_cls('Bar', 'ns2', autogen=False), TypeSource)


class TestTypeSourceDataclass(TestCase):
    """Test TypeSource dataclass behavior."""

    def test_frozen(self):
        """Test that TypeSource is frozen and attributes cannot be changed."""
        ts = TypeSource('ns1', 'Bar')
        with self.assertRaises(AttributeError):
            ts.namespace = 'ns2'
        with self.assertRaises(AttributeError):
            ts.data_type = 'Baz'

    def test_equality(self):
        """Test that two TypeSource instances with the same fields are equal."""
        ts1 = TypeSource('ns1', 'Bar')
        ts2 = TypeSource('ns1', 'Bar')
        self.assertEqual(ts1, ts2)

    def test_inequality(self):
        """Test that TypeSource instances with different fields are not equal."""
        ts1 = TypeSource('ns1', 'Bar')
        ts2 = TypeSource('ns1', 'Baz')
        ts3 = TypeSource('ns2', 'Bar')
        self.assertNotEqual(ts1, ts2)
        self.assertNotEqual(ts1, ts3)

    def test_hashable(self):
        """Test that TypeSource is hashable and can be used in sets and as dict keys."""
        ts1 = TypeSource('ns1', 'Bar')
        ts2 = TypeSource('ns1', 'Bar')
        ts3 = TypeSource('ns2', 'Bar')
        self.assertEqual(hash(ts1), hash(ts2))
        # TypeSources can be used as dict keys (important for __container_cls_to_ns_dt)
        d = {ts1: ('ns1', 'Bar')}
        self.assertEqual(d[ts2], ('ns1', 'Bar'))
        self.assertNotIn(ts3, d)


class TestLoadNamespacesSourceTypes(TestCase):
    """Test that load_namespaces returns source types and registers TypeSource entries."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.type_map = TypeMap()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_load_namespaces_returns_source_types(self):
        """Test that load_namespaces returns a dict with dependencies and stores source types."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        baz_spec = GroupSpec(doc='A test group spec', data_type_def='Baz')
        ret = create_load_namespace_yaml('ns1', [bar_spec, baz_spec], self.test_dir, {}, self.type_map)

        # ret should be {ns_name: ns_deps}
        self.assertIn('ns1', ret)
        ns_deps = ret['ns1']
        self.assertEqual(ns_deps, {})

        # source types should be accessible via get_source_types
        source_types = self.type_map.namespace_catalog.get_source_types('ns1')
        self.assertIn('Bar', source_types)
        self.assertIn('Baz', source_types)

    def test_load_namespaces_returns_dependent_types(self):
        """Test that load_namespaces returns dependency info for namespaces with includes."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        create_load_namespace_yaml('ns1', [bar_spec], self.test_dir, {}, self.type_map)

        baz_spec = GroupSpec(doc='A test group spec', data_type_def='Baz', data_type_inc='Bar')
        ret = create_load_namespace_yaml('ns2', [baz_spec], self.test_dir, {'ns1': ['Bar']}, self.type_map)

        self.assertIn('ns2', ret)
        ns_deps = ret['ns2']
        self.assertIn('ns1', ns_deps)
        self.assertIn('Bar', ns_deps['ns1'])

        # source types should be accessible via get_source_types
        source_types = self.type_map.namespace_catalog.get_source_types('ns2')
        self.assertIn('Baz', source_types)

    def test_load_namespaces_registers_source_typesource(self):
        """Test that source types are registered as TypeSource after load_namespaces."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        baz_spec = GroupSpec(doc='A test group spec', data_type_def='Baz')
        create_load_namespace_yaml('ns1', [bar_spec, baz_spec], self.test_dir, {}, self.type_map)

        # Both types should be registered as TypeSource (not yet generated)
        bar_cls = self.type_map.get_dt_container_cls('Bar', 'ns1', autogen=False)
        baz_cls = self.type_map.get_dt_container_cls('Baz', 'ns1', autogen=False)
        self.assertIsInstance(bar_cls, TypeSource)
        self.assertIsInstance(baz_cls, TypeSource)
        # The TypeSource should reference the source namespace
        self.assertEqual(bar_cls.namespace, 'ns1')
        self.assertEqual(bar_cls.data_type, 'Bar')

    def test_load_namespaces_registers_dep_typesource(self):
        """Test that dependent types are registered as TypeSource in the importing namespace."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        create_load_namespace_yaml('ns1', [bar_spec], self.test_dir, {}, self.type_map)

        create_load_namespace_yaml('ns2', [], self.test_dir, {'ns1': ['Bar']}, self.type_map)

        # Bar in ns2 should be a TypeSource pointing to ns1
        bar_cls = self.type_map.get_dt_container_cls('Bar', 'ns2', autogen=False)
        self.assertIsInstance(bar_cls, TypeSource)
        self.assertEqual(bar_cls.namespace, 'ns1')
        self.assertEqual(bar_cls.data_type, 'Bar')

    def test_load_namespaces_preserves_existing_class(self):
        """Test that load_namespaces does not overwrite an already-registered container class."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')

        # Manually set up a namespace and register a real class for Bar
        spec_catalog = SpecCatalog()
        spec_catalog.register_spec(bar_spec, 'test.yaml')
        namespace = SpecNamespace(doc='a test namespace', name='ns1', schema=[{'source': 'test.yaml'}],
                                  version='0.1.0', catalog=spec_catalog)
        ns_catalog = NamespaceCatalog()
        ns_catalog.add_namespace('ns1', namespace)
        type_map = TypeMap(ns_catalog)
        type_map.register_container_type('ns1', 'Bar', Foo)  # register Foo as the Bar container class

        # Now load_namespaces for ns2 that includes Bar from ns1
        create_load_namespace_yaml('ns2', [], self.test_dir, {'ns1': ['Bar']}, type_map)

        # Bar in ns1 should still be Foo (not a TypeSource)
        bar_cls = type_map.get_dt_container_cls('Bar', 'ns1', autogen=False)
        self.assertIs(bar_cls, Foo)

    def test_load_namespaces_preserves_resolved_source_type_on_reload(self):
        """Test that calling load_namespaces again preserves an already-resolved source type class.

        When load_namespaces is called a second time with the same path, the NamespaceCatalog returns
        cached results, so TypeMap re-processes the source types. An already-resolved class should not
        be overwritten with a new TypeSource.
        """
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        create_load_namespace_yaml('ns1', [bar_spec], self.test_dir, {}, self.type_map)

        # Resolve Bar to a real class
        Bar = self.type_map.get_dt_container_cls('Bar', 'ns1')
        self.assertFalse(isinstance(Bar, TypeSource))

        # Call load_namespaces again — ns_catalog returns cached result, TypeMap re-processes source types
        ns_path = os.path.join(self.test_dir, 'ns1.namespace.yaml')
        self.type_map.load_namespaces(ns_path)

        # Bar should still be the resolved class (not overwritten with a TypeSource)
        bar_cls = self.type_map.get_dt_container_cls('Bar', 'ns1', autogen=False)
        self.assertIs(bar_cls, Bar)


class TestGetDtContainerClsTypeSourceResolution(TestCase):
    """Test get_dt_container_cls resolution of TypeSource entries."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.type_map = TypeMap()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_resolve_self_referential_typesource(self):
        """Test that get_dt_container_cls generates a class for a self-referential TypeSource."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        create_load_namespace_yaml('ns1', [bar_spec], self.test_dir, {}, self.type_map)

        # Before resolution, Bar should be a TypeSource
        cls = self.type_map.get_dt_container_cls('Bar', 'ns1', autogen=False)
        self.assertIsInstance(cls, TypeSource)

        # Now resolve it
        Bar = self.type_map.get_dt_container_cls('Bar', 'ns1')
        self.assertFalse(isinstance(Bar, TypeSource))
        self.assertEqual(Bar.__name__, 'Bar')
        ns, dt = self.type_map.get_container_cls_dt(Bar)
        self.assertEqual(ns, 'ns1')
        self.assertEqual(dt, 'Bar')

        # After resolution, it should return the same class
        Bar2 = self.type_map.get_dt_container_cls('Bar', 'ns1', autogen=False)
        self.assertIs(Bar, Bar2)


    def test_resolve_cross_namespace_typesource(self):
        """Test that get_dt_container_cls resolves a TypeSource pointing to another namespace."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        create_load_namespace_yaml('ns1', [bar_spec], self.test_dir, {}, self.type_map)

        create_load_namespace_yaml('ns2', [], self.test_dir, {'ns1': ['Bar']}, self.type_map)

        # Resolve Bar from ns2 - should follow the TypeSource to ns1 and generate the class
        Bar = self.type_map.get_dt_container_cls('Bar', 'ns2')
        self.assertFalse(isinstance(Bar, TypeSource))
        self.assertEqual(Bar.__name__, 'Bar')

        # The same class should be returned from ns1
        Bar_ns1 = self.type_map.get_dt_container_cls('Bar', 'ns1')
        self.assertIs(Bar, Bar_ns1)

    def test_resolve_typesource_with_inheritance(self):
        """Test that resolving a TypeSource for a child type also resolves the parent."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        baz_spec = GroupSpec(doc='A test group spec', data_type_def='Baz', data_type_inc='Bar')
        create_load_namespace_yaml('ns1', [bar_spec, baz_spec], self.test_dir, {}, self.type_map)

        # Resolve Baz - should also resolve Bar as its parent
        Baz = self.type_map.get_dt_container_cls('Baz', 'ns1')

        # Bar should already be resolved by Baz resolution
        Bar = self.type_map.get_dt_container_cls('Bar', 'ns1', autogen=False)
        self.assertTrue(issubclass(Baz, Bar))
        self.assertFalse(isinstance(Bar, TypeSource))

    def test_resolve_cross_namespace_with_extension(self):
        """Test resolving a type that extends a type from another namespace."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        create_load_namespace_yaml('ns1', [bar_spec], self.test_dir, {}, self.type_map)

        baz_spec = GroupSpec(doc='A test group spec', data_type_def='Baz', data_type_inc='Bar')
        create_load_namespace_yaml('ns2', [baz_spec], self.test_dir, {'ns1': ['Bar']}, self.type_map)

        # Resolve Baz from ns2 - should generate Baz and resolve Bar from ns1
        Baz = self.type_map.get_dt_container_cls('Baz', 'ns2')

        # Bar should already be resolved by Baz resolution
        Bar = self.type_map.get_dt_container_cls('Bar', 'ns1', autogen=False)
        self.assertTrue(issubclass(Baz, Bar))
        self.assertEqual(Baz.__name__, 'Baz')
        self.assertEqual(Bar.__name__, 'Bar')

    def test_generate_class_raises_when_parent_not_extendermeta(self):
        """Test that generating a child class raises ValueError if the parent class is not ExtenderMeta."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        baz_spec = GroupSpec(doc='A test group spec', data_type_def='Baz', data_type_inc='Bar')
        create_load_namespace_yaml('ns1', [bar_spec, baz_spec], self.test_dir, {}, self.type_map)

        # Register a non-ExtenderMeta class for Bar
        class NotExtenderMetaClass:
            pass
        self.type_map.register_container_type('ns1', 'Bar', NotExtenderMetaClass)

        # Generating Baz (which inherits from Bar) should fail because Bar's class is not ExtenderMeta
        with self.assertRaises(ValueError) as cm:
            self.type_map.get_dt_container_cls('Baz', 'ns1')
        self.assertIn("not of type ExtenderMeta", str(cm.exception))

    def test_autogen_false_returns_none_when_unregistered(self):
        """Test that get_dt_container_cls with autogen=False returns None for unregistered types."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        spec_catalog = SpecCatalog()
        spec_catalog.register_spec(bar_spec, 'test.yaml')
        namespace = SpecNamespace(doc='a test namespace', name='ns1', schema=[{'source': 'test.yaml'}],
                                  version='0.1.0', catalog=spec_catalog)
        ns_catalog = NamespaceCatalog()
        ns_catalog.add_namespace('ns1', namespace)
        type_map = TypeMap(ns_catalog)
        # Do not register any container type for Bar - simulate spec registered without class
        cls = type_map.get_dt_container_cls('Bar', 'ns1', autogen=False)
        self.assertIsNone(cls)

    def test_get_container_classes_after_partial_resolution(self):
        """Test get_container_classes only returns resolved classes, not TypeSource placeholders."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        baz_spec = GroupSpec(doc='A test group spec', data_type_def='Baz')
        create_load_namespace_yaml('ns1', [bar_spec, baz_spec], self.test_dir, {}, self.type_map)

        # Initially, no resolved classes
        classes = self.type_map.get_container_classes('ns1')
        self.assertEqual(len(classes), 0)

        # Resolve only Bar
        self.type_map.get_dt_container_cls('Bar', 'ns1')

        # Now there should be one real class
        classes = self.type_map.get_container_classes('ns1')
        self.assertEqual(len(classes), 1)
        self.assertFalse(isinstance(classes[0], TypeSource))


class TestNamespaceLookupWithTypeSource(TestCase):
    """Test that namespace lookup in get_dt_container_cls handles TypeSource correctly."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.type_map = TypeMap()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_namespace_lookup_finds_self_referential_typesource(self):
        """Test that namespace lookup finds a TypeSource that points to its own namespace."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        create_load_namespace_yaml('ns1', [bar_spec], self.test_dir, {}, self.type_map)

        # Without specifying namespace, should find ns1 because the TypeSource
        # for Bar in ns1 is self-referential (points to ns1)
        Bar = self.type_map.get_dt_container_cls('Bar')
        self.assertEqual(Bar.__name__, 'Bar')

    def test_namespace_lookup_prefers_real_class_over_typesource(self):
        """Test that namespace lookup prefers a real class over a cross-namespace TypeSource."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        create_load_namespace_yaml('ns1', [bar_spec], self.test_dir, {}, self.type_map)

        # Include Bar in ns2 (creates a cross-namespace TypeSource in ns2)
        create_load_namespace_yaml('ns2', [], self.test_dir, {'ns1': ['Bar']}, self.type_map)

        # Generate the real class for Bar in ns1
        Bar = self.type_map.get_dt_container_cls('Bar', 'ns1')

        # Now look up Bar without namespace - should find ns1 (the real class), not ns2 (TypeSource)
        Bar2 = self.type_map.get_dt_container_cls(data_type='Bar')
        self.assertIs(Bar, Bar2)

    def test_namespace_lookup_error_for_unknown_type(self):
        """Test that looking up a type not in any namespace raises ValueError."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        create_load_namespace_yaml('ns1', [bar_spec], self.test_dir, {}, self.type_map)

        with self.assertRaises(ValueError) as cm:
            self.type_map.get_dt_container_cls(data_type='NonExistent')
        self.assertIn("Namespace could not be resolved", str(cm.exception))


class TestTypeMapMergeWithTypeSource(TestCase):
    """Test TypeMap merge and copy with TypeSource entries."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_merge_preserves_typesource(self):
        """Test that merging a TypeMap with TypeSource entries preserves them."""
        type_map1 = TypeMap()
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        create_load_namespace_yaml('ns1', [bar_spec], self.test_dir, {}, type_map1)

        # TypeSource should be present in type_map1
        cls = type_map1.get_dt_container_cls('Bar', 'ns1', autogen=False)
        self.assertIsInstance(cls, TypeSource)

        # Copy type_map1 - the TypeSource should be preserved
        type_map2 = copy(type_map1)
        cls2 = type_map2.get_dt_container_cls('Bar', 'ns1', autogen=False)
        self.assertIsInstance(cls2, TypeSource)

    def test_merge_resolved_class_overrides_typesource(self):
        """Test that merging a TypeMap where the class is resolved overrides TypeSource."""
        type_map1 = TypeMap()
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        create_load_namespace_yaml('ns1', [bar_spec], self.test_dir, {}, type_map1)

        # Resolve Bar in type_map1
        Bar = type_map1.get_dt_container_cls('Bar', 'ns1')

        # Create a fresh TypeMap and merge
        type_map2 = copy(type_map1)
        cls = type_map2.get_dt_container_cls('Bar', 'ns1', autogen=False)
        self.assertIs(cls, Bar)


class TestLoadNamespacesMultipleTypes(TestCase):
    """Test load_namespaces with multiple types and dependencies."""

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.type_map = TypeMap()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_load_namespaces_chain_dependencies(self):
        """Test loading a namespace chain: ns1 -> ns2 -> ns3."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        create_load_namespace_yaml('ns1', [bar_spec], self.test_dir, {}, self.type_map)

        baz_spec = GroupSpec(doc='A test group spec', data_type_def='Baz', data_type_inc='Bar')
        create_load_namespace_yaml('ns2', [baz_spec], self.test_dir, {'ns1': ['Bar']}, self.type_map)

        qux_spec = GroupSpec(doc='A test group spec', data_type_def='Qux', data_type_inc='Baz')
        create_load_namespace_yaml('ns3', [qux_spec], self.test_dir, {'ns2': ['Baz']}, self.type_map)

        # Resolve Qux - should resolve entire chain
        Qux = self.type_map.get_dt_container_cls('Qux', 'ns3')
        Baz = self.type_map.get_dt_container_cls('Baz', 'ns2', autogen=False)
        Bar = self.type_map.get_dt_container_cls('Bar', 'ns1', autogen=False)
        self.assertTrue(issubclass(Qux, Baz))
        self.assertTrue(issubclass(Baz, Bar))

    def test_load_namespaces_no_eager_class_generation(self):
        """Test that load_namespaces does not eagerly generate classes."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        baz_spec = GroupSpec(doc='A test group spec', data_type_def='Baz')
        create_load_namespace_yaml('ns1', [bar_spec, baz_spec], self.test_dir, {}, self.type_map)

        # After load_namespaces, no classes should be resolved yet
        classes = self.type_map.get_container_classes('ns1')
        self.assertEqual(len(classes), 0)

    def test_resolve_all_types_in_namespace(self):
        """Test resolving all types in a namespace one by one."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        baz_spec = GroupSpec(doc='A test group spec', data_type_def='Baz')
        qux_spec = DatasetSpec(doc='A test dataset spec', data_type_def='Qux')
        create_load_namespace_yaml('ns1', [bar_spec, baz_spec, qux_spec], self.test_dir, {}, self.type_map)

        self.type_map.get_dt_container_cls('Bar', 'ns1')
        self.type_map.get_dt_container_cls('Baz', 'ns1')
        self.type_map.get_dt_container_cls('Qux', 'ns1')

        classes = self.type_map.get_container_classes('ns1')
        self.assertEqual(len(classes), 3)
        self.assertFalse(any(isinstance(c, TypeSource) for c in classes))
        class_names = {c.__name__ for c in classes}
        self.assertSetEqual(class_names, {'Bar', 'Baz', 'Qux'})


class TestExtensionIncludeNamespaceDoesNotContaminateCore(TestCase):
    """Tests that loading an extension with include_namespace("core") does not contaminate core types.

    When an extension calls include_namespace("core"), all core types get re-registered under the
    extension namespace. These tests verify that the reverse map, class attributes, and builders
    all still point to the original "core" namespace.
    """

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.type_map = TypeMap()

        # Define "core" namespace with NWBFile, like pynwb core
        nwbfile_spec = GroupSpec(doc='The root NWB file type', data_type_def='NWBFile')
        create_load_namespace_yaml(
            namespace_name='core',
            specs=[nwbfile_spec],
            output_dir=self.test_dir,
            incl_types={},
            type_map=self.type_map,
        )
        self.NWBFile = self.type_map.get_dt_container_cls(data_type='NWBFile', namespace='core')

        # Load an extension that does include_namespace("core"), like ndx-events or ndx-hed
        events_spec = GroupSpec(doc='An events type', data_type_def='EventsTable', data_type_inc='NWBFile')
        create_load_namespace_yaml(
            namespace_name='ndx-events',
            specs=[events_spec],
            output_dir=self.test_dir,
            incl_types={'core': None},  # None means include_namespace("core"), i.e. all core types
            type_map=self.type_map,
        )

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_build_core_type_keeps_core_namespace(self):
        """Building a core type must produce a builder with namespace='core'."""
        builder = self.type_map.build(
            container=self.NWBFile(name='root'),
            manager=BuildManager(self.type_map),
            source='test.nwb',
        )
        self.assertEqual(builder.attributes['namespace'], 'core')

    def test_reverse_map_returns_core_namespace(self):
        """get_container_cls_dt must return 'core' for a core type."""
        ns, dt = self.type_map.get_container_cls_dt(self.NWBFile)
        self.assertEqual(ns, 'core')
        self.assertEqual(dt, 'NWBFile')

    def test_class_namespace_attribute_not_overwritten(self):
        """The class-level .namespace attribute must remain 'core'."""
        self.assertEqual(self.NWBFile.namespace, 'core')


class TestRegisterContainerTypeCrossNamespace(TestCase):
    """Tests that register_container_type correctly handles types shared across namespaces.

    These tests verify that:
    - Replacing a class in one namespace does not corrupt the reverse map for another namespace.
    - Replacing a TypeSource with a real class in the same namespace still works correctly.
    """

    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.type_map = TypeMap()

    def tearDown(self):
        shutil.rmtree(self.test_dir)

    def test_resolving_included_type_does_not_destroy_original_mapping(self):
        """Replacing a TypeSource in a non-primary namespace must not destroy the primary namespace's reverse map."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        create_load_namespace_yaml(
            namespace_name='ns1',
            specs=[bar_spec],
            output_dir=self.test_dir,
            incl_types={},
            type_map=self.type_map,
        )

        # ns2 includes Bar from ns1
        baz_spec = GroupSpec(doc='Another type', data_type_def='Baz', data_type_inc='Bar')
        create_load_namespace_yaml(
            namespace_name='ns2',
            specs=[baz_spec],
            output_dir=self.test_dir,
            incl_types={'ns1': ['Bar']},
            type_map=self.type_map,
        )

        # Resolve Bar in ns1 (primary namespace) - this replaces the TypeSource with a real class
        Bar = self.type_map.get_dt_container_cls('Bar', 'ns1')
        ns, dt = self.type_map.get_container_cls_dt(Bar)
        self.assertEqual(ns, 'ns1')
        self.assertEqual(dt, 'Bar')

        # Resolve Bar in ns2 - this replaces the TypeSource in ns2's forward map with Bar.
        # The reverse map for Bar must still point to ns1.
        Bar_via_ns2 = self.type_map.get_dt_container_cls('Bar', 'ns2')
        self.assertIs(Bar_via_ns2, Bar)
        ns, dt = self.type_map.get_container_cls_dt(Bar)
        self.assertEqual(ns, 'ns1')
        self.assertEqual(dt, 'Bar')

    def test_reregistration_keeps_the_previous_class_mapped(self):
        """A live class must keep its data type when a second class object registers for the same type.

        Re-importing a module that defines a class with @register_class builds a second class object for
        the same data type. Containers created before that are still instances of the first one, so it
        must keep its entry in the class-to-data-type map, otherwise the build walks up the MRO and
        writes them as a registered ancestor with the subtype's own fields dropped.
        """
        base_spec = GroupSpec(doc='A base type with no datasets', data_type_def='Base')
        sub_spec = GroupSpec(
            doc='A subtype that declares a dataset',
            data_type_def='Sub',
            data_type_inc='Base',
            datasets=[DatasetSpec(doc='the payload', name='payload', dtype='text')],
        )
        create_load_namespace_yaml('ns1', [base_spec, sub_spec], self.test_dir, {}, self.type_map)
        Base = self.type_map.get_dt_container_cls('Base', 'ns1')
        Sub = self.type_map.get_dt_container_cls('Sub', 'ns1')
        container = Sub(name='thing', payload='important text')

        class SubReimported(Base):
            pass

        self.type_map.register_container_type('ns1', 'Sub', SubReimported)

        ns, dt = self.type_map.get_container_cls_dt(Sub)
        self.assertEqual(ns, 'ns1')
        self.assertEqual(dt, 'Sub')

        builder = BuildManager(self.type_map).build(container, source='test.h5')
        self.assertEqual(builder.attributes['data_type'], 'Sub')
        self.assertIn('payload', builder.datasets)

    def test_typesource_replacement_updates_reverse_map(self):
        """Replacing a TypeSource with a real class in the same namespace must update the reverse map."""
        bar_spec = GroupSpec(doc='A test group spec', data_type_def='Bar')
        create_load_namespace_yaml(
            namespace_name='ns1',
            specs=[bar_spec],
            output_dir=self.test_dir,
            incl_types={},
            type_map=self.type_map,
        )

        # Before resolution, Bar should be a TypeSource
        bar_ts = self.type_map.get_dt_container_cls('Bar', 'ns1', autogen=False)
        self.assertIsInstance(bar_ts, TypeSource)

        # Resolve to a real class — this replaces the TypeSource in register_container_type
        Bar = self.type_map.get_dt_container_cls('Bar', 'ns1')
        self.assertFalse(isinstance(Bar, TypeSource))

        # The real class should be in the reverse map pointing to ns1
        ns, dt = self.type_map.get_container_cls_dt(Bar)
        self.assertEqual(ns, 'ns1')
        self.assertEqual(dt, 'Bar')


# TODO:
class TestWildCardNamedSpecs(TestCase):
    pass
