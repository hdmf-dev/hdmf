"""Tests for the deduplicate_objects functionality in BuildManager"""

from hdmf.build import GroupBuilder, DatasetBuilder, BuildManager, TypeMap, ObjectMapper
from hdmf.spec import GroupSpec, AttributeSpec, DatasetSpec, SpecCatalog, SpecNamespace, NamespaceCatalog
from hdmf.testing import TestCase
from hdmf.container import Data

from tests.unit.helpers.utils import Foo, CORE_NAMESPACE


class FooMapper(ObjectMapper):
    """Maps nested 'attr2' attribute on dataset 'my_data' to Foo.attr2 in constructor and attribute map"""

    def __init__(self, spec):
        super().__init__(spec)
        my_data_spec = spec.get_dataset('my_data')
        self.map_spec('attr2', my_data_spec.get_attribute('attr2'))


class TestBuildManagerDeduplication(TestCase):
    """Test BuildManager deduplication functionality"""

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

    def test_default_deduplicate_objects_true(self):
        """Test that deduplicate_objects defaults to True"""
        manager = BuildManager(self.type_map)
        self.assertTrue(manager.deduplicate_objects)

    def test_deduplicate_objects_explicit_true(self):
        """Test that deduplicate_objects can be explicitly set to True"""
        manager = BuildManager(self.type_map, deduplicate_objects=True)
        self.assertTrue(manager.deduplicate_objects)

    def test_deduplicate_objects_explicit_false(self):
        """Test that deduplicate_objects can be explicitly set to False"""
        manager = BuildManager(self.type_map, deduplicate_objects=False)
        self.assertFalse(manager.deduplicate_objects)

    def test_get_builder_with_deduplication_enabled(self):
        """Test that get_builder returns cached builder when deduplication is enabled"""
        manager = BuildManager(self.type_map, deduplicate_objects=True)
        
        # Create a simple Data container
        container = Data(name="test_data", data=[1, 2, 3])
        
        # Create and cache a builder
        builder = DatasetBuilder(name="test_data", data=[1, 2, 3])
        manager.prebuilt(container, builder)
        
        # get_builder should return the cached builder
        cached_builder = manager.get_builder(container)
        self.assertIs(cached_builder, builder)

    def test_get_builder_with_deduplication_disabled(self):
        """Test that get_builder returns None when deduplication is disabled"""
        manager = BuildManager(self.type_map, deduplicate_objects=False)
        
        # Create a simple Data container
        container = Data(name="test_data", data=[1, 2, 3])
        
        # Create and cache a builder
        builder = DatasetBuilder(name="test_data", data=[1, 2, 3])
        manager.prebuilt(container, builder)
        
        # get_builder should return None when deduplication is disabled
        cached_builder = manager.get_builder(container)
        self.assertIsNone(cached_builder)

    def test_build_memoization_with_deduplication_enabled(self):
        """Test that repeated builds return same builder when deduplication is enabled"""
        manager = BuildManager(self.type_map, deduplicate_objects=True)
        
        container_inst = Foo('my_foo', list(range(10)), 'value1', 10)
        
        # Build twice - should get same builder
        builder1 = manager.build(container_inst)
        builder2 = manager.build(container_inst)
        
        self.assertIs(builder1, builder2)

    def test_build_no_memoization_with_deduplication_disabled(self):
        """Test that repeated builds create new builders when deduplication is disabled"""
        manager = BuildManager(self.type_map, deduplicate_objects=False)
        
        container_inst = Foo('my_foo', list(range(10)), 'value1', 10)
        
        # Build twice - should get different builders
        builder1 = manager.build(container_inst)
        builder2 = manager.build(container_inst)
        
        self.assertIsNot(builder1, builder2)
        # But they should have the same content
        self.assertDictEqual(builder1, builder2)

    def test_clear_cache_behavior(self):
        """Test that clear_cache works regardless of deduplication setting"""
        # Test with deduplication enabled
        manager_true = BuildManager(self.type_map, deduplicate_objects=True)
        container = Data(name="test_data", data=[1, 2, 3])
        builder = DatasetBuilder(name="test_data", data=[1, 2, 3])
        manager_true.prebuilt(container, builder)
        
        self.assertIs(manager_true.get_builder(container), builder)
        manager_true.clear_cache()
        self.assertIsNone(manager_true.get_builder(container))
        
        # Test with deduplication disabled
        manager_false = BuildManager(self.type_map, deduplicate_objects=False)
        manager_false.prebuilt(container, builder)
        
        # Should return None even before clearing cache
        self.assertIsNone(manager_false.get_builder(container))
        manager_false.clear_cache()
        self.assertIsNone(manager_false.get_builder(container))
