from hdmf.spec import DatasetSpec, GroupSpec, SpecCatalog, SpecNamespace, NamespaceCatalog
from hdmf.testing import TestCase


class TestMultiNamespaceResolution(TestCase):
    """Tests for namespace-agnostic type resolution across all loaded namespaces.

    The fixture builds a shared dependency namespace ('test-core') and two independent
    extension namespaces ('ndx-a', 'ndx-b'), neither of which depends on the other. As the
    real loader does, each extension namespace's catalog also holds the dependency types it
    includes ('Base', 'MyVector'). ns-a defines a subtype 'AVector' of 'MyVector' and ns-b
    defines an independent subtype 'MySubVector' of 'MyVector'.
    """

    def setUp(self):
        def core_catalog():
            cat = SpecCatalog()
            cat.register_spec(GroupSpec(doc='a base container', data_type_def='Base'), 'core.yaml')
            cat.register_spec(DatasetSpec(doc='a base vector', data_type_def='MyVector', dtype='int'), 'core.yaml')
            return cat

        cat_core = core_catalog()

        cat_a = core_catalog()  # includes the dependency types, as the loader would
        cat_a.register_spec(GroupSpec(doc='type A', data_type_inc='Base', data_type_def='TypeA'), 'a.yaml')
        cat_a.register_spec(DatasetSpec(doc='an A vector', data_type_inc='MyVector', data_type_def='AVector'), 'a.yaml')
        # a type whose name collides with a different type in ndx-b
        cat_a.register_spec(GroupSpec(doc='an A widget', data_type_def='Widget'), 'a.yaml')

        cat_b = core_catalog()  # includes the dependency types, as the loader would
        cat_b.register_spec(GroupSpec(doc='type B', data_type_inc='Base', data_type_def='TypeB'), 'b.yaml')
        cat_b.register_spec(
            DatasetSpec(doc='a sub vector', data_type_inc='MyVector', data_type_def='MySubVector'), 'b.yaml'
        )
        # a different type that reuses the name 'Widget' (here it inherits Base)
        cat_b.register_spec(GroupSpec(doc='a B widget', data_type_inc='Base', data_type_def='Widget'), 'b.yaml')

        core_ns = SpecNamespace('a shared core', 'test-core', [{'source': 'core.yaml'}],
                                version='0.1.0', catalog=cat_core)
        a_ns = SpecNamespace('extension a', 'ndx-a', [{'namespace': 'test-core'}, {'source': 'a.yaml'}],
                             version='0.1.0', catalog=cat_a)
        b_ns = SpecNamespace('extension b', 'ndx-b', [{'namespace': 'test-core'}, {'source': 'b.yaml'}],
                             version='0.1.0', catalog=cat_b)

        self.catalog = NamespaceCatalog()
        self.catalog.add_namespace('test-core', core_ns)
        self.catalog.add_namespace('ndx-a', a_ns)
        self.catalog.add_namespace('ndx-b', b_ns)

    def test_get_spec_search_all_namespaces(self):
        """get_spec with namespace=None finds a type defined in any loaded namespace."""
        self.assertEqual(self.catalog.get_spec(data_type='TypeA').data_type_def, 'TypeA')
        self.assertEqual(self.catalog.get_spec(data_type='TypeB').data_type_def, 'TypeB')
        self.assertEqual(self.catalog.get_spec(data_type='MySubVector').data_type_def, 'MySubVector')
        # a shared dependency type resolves too
        self.assertEqual(self.catalog.get_spec(data_type='Base').data_type_def, 'Base')

    def test_get_spec_two_arg_unchanged(self):
        """The existing (namespace, data_type) behavior is unchanged."""
        self.assertEqual(self.catalog.get_spec('ndx-a', 'TypeA').data_type_def, 'TypeA')
        # a type not in the named namespace still raises, as before
        with self.assertRaises(ValueError):
            self.catalog.get_spec('ndx-a', 'TypeB')

    def test_get_spec_unknown_type_raises(self):
        with self.assertRaises(ValueError):
            self.catalog.get_spec(data_type='NotAType')

    def test_get_spec_requires_data_type(self):
        with self.assertRaises(ValueError):
            self.catalog.get_spec()

    def test_get_hierarchy_search_all_namespaces(self):
        self.assertTupleEqual(self.catalog.get_hierarchy(data_type='MySubVector'), ('MySubVector', 'MyVector'))
        self.assertTupleEqual(self.catalog.get_hierarchy(data_type='TypeA'), ('TypeA', 'Base'))

    def test_get_hierarchy_two_arg_unchanged(self):
        self.assertTupleEqual(self.catalog.get_hierarchy('ndx-b', 'MySubVector'), ('MySubVector', 'MyVector'))

    def test_get_hierarchy_unknown_type_returns_empty(self):
        self.assertTupleEqual(self.catalog.get_hierarchy(data_type='NotAType'), ())

    def test_colliding_name_disambiguated_by_namespace(self):
        """The same type name in two namespaces resolves to different specs when the namespace is given."""
        a_widget = self.catalog.get_spec('ndx-a', 'Widget')
        b_widget = self.catalog.get_spec('ndx-b', 'Widget')
        self.assertIsNot(a_widget, b_widget)
        self.assertIsNone(a_widget.data_type_inc)
        self.assertEqual(b_widget.data_type_inc, 'Base')

    def test_colliding_name_search_all_returns_first(self):
        """With namespace=None, a colliding name resolves to the first-loaded namespace (documented behavior)."""
        # 'ndx-a' is added before 'ndx-b', so its Widget (which does not inherit Base) is returned
        self.assertIsNone(self.catalog.get_spec(data_type='Widget').data_type_inc)

    def test_is_sub_data_type_search_all_namespaces(self):
        self.assertTrue(self.catalog.is_sub_data_type(data_type='MySubVector', parent_data_type='MyVector'))
        self.assertTrue(self.catalog.is_sub_data_type(data_type='TypeA', parent_data_type='Base'))
        self.assertFalse(self.catalog.is_sub_data_type(data_type='TypeA', parent_data_type='MyVector'))

    def test_is_sub_data_type_two_arg_unchanged(self):
        self.assertTrue(self.catalog.is_sub_data_type('ndx-b', 'MySubVector', 'MyVector'))

    def test_type_key_properties(self):
        self.assertEqual(self.catalog.type_key, 'data_type')
        self.assertSetEqual(self.catalog.type_keys, {'data_type'})
