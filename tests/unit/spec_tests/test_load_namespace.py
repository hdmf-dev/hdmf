import json
import os
import ruamel.yaml as yaml
from tempfile import gettempdir
import warnings

from hdmf.common import get_type_map
from hdmf.spec import AttributeSpec, DatasetSpec, GroupSpec, SpecNamespace, NamespaceCatalog, NamespaceBuilder
from hdmf.testing import TestCase, remove_test_file

from tests.unit.utils import CustomGroupSpec, CustomDatasetSpec, CustomSpecNamespace


class TestSpecLoad(TestCase):
    NS_NAME = 'test_ns'

    def setUp(self):
        self.attributes = [
            AttributeSpec('attribute1', 'my first attribute', 'text'),
            AttributeSpec('attribute2', 'my second attribute', 'text')
        ]
        self.dset1_attributes = [
            AttributeSpec('attribute3', 'my third attribute', 'text'),
            AttributeSpec('attribute4', 'my fourth attribute', 'text')
        ]
        self.dset2_attributes = [
            AttributeSpec('attribute5', 'my fifth attribute', 'text'),
            AttributeSpec('attribute6', 'my sixth attribute', 'text')
        ]
        self.datasets = [
            DatasetSpec('my first dataset',
                        'int',
                        name='dataset1',
                        attributes=self.dset1_attributes,
                        linkable=True),
            DatasetSpec('my second dataset',
                        'int',
                        name='dataset2',
                        dims=(None, None),
                        attributes=self.dset2_attributes,
                        linkable=True,
                        data_type_def='VoltageArray')
        ]
        self.spec = GroupSpec('A test group',
                              name='root_constructor_datatype',
                              datasets=self.datasets,
                              attributes=self.attributes,
                              linkable=False,
                              data_type_def='EphysData')
        dset1_attributes_ext = [
            AttributeSpec('dset1_extra_attribute', 'an extra attribute for the first dataset', 'text')
        ]
        self.ext_datasets = [
            DatasetSpec('my first dataset extension',
                        'int',
                        name='dataset1',
                        attributes=dset1_attributes_ext,
                        linkable=True),
        ]
        self.ext_attributes = [
            AttributeSpec('ext_extra_attribute', 'an extra attribute for the group', 'text'),
        ]
        self.ext_spec = GroupSpec('A test group extension',
                                  name='root_constructor_datatype',
                                  datasets=self.ext_datasets,
                                  attributes=self.ext_attributes,
                                  linkable=False,
                                  data_type_inc='EphysData',
                                  data_type_def='SpikeData')
        to_dump = {'groups': [self.spec, self.ext_spec]}
        self.specs_path = 'test_load_namespace.specs.yaml'
        self.namespace_path = 'test_load_namespace.namespace.yaml'
        with open(self.specs_path, 'w') as tmp:
            yaml_obj = yaml.YAML(typ='safe', pure=True)
            yaml_obj.default_flow_style = False
            yaml_obj.dump(json.loads(json.dumps(to_dump)), tmp)
        ns_dict = {
            'doc': 'a test namespace',
            'name': self.NS_NAME,
            'schema': [
                {'source': self.specs_path}
            ],
            'version': '0.1.0'
        }
        self.namespace = SpecNamespace.build_namespace(**ns_dict)
        to_dump = {'namespaces': [self.namespace]}
        with open(self.namespace_path, 'w') as tmp:
            yaml_obj = yaml.YAML(typ='safe', pure=True)
            yaml_obj.default_flow_style = False
            yaml_obj.dump(json.loads(json.dumps(to_dump)), tmp)
        self.ns_catalog = NamespaceCatalog()

    def tearDown(self):
        if os.path.exists(self.namespace_path):
            os.remove(self.namespace_path)
        if os.path.exists(self.specs_path):
            os.remove(self.specs_path)

    def test_inherited_attributes(self):
        self.ns_catalog.load_namespaces(self.namespace_path, resolve=True)
        ts_spec = self.ns_catalog.get_spec(self.NS_NAME, 'EphysData')
        es_spec = self.ns_catalog.get_spec(self.NS_NAME, 'SpikeData')
        ts_attrs = {s.name for s in ts_spec.attributes}
        es_attrs = {s.name for s in es_spec.attributes}
        for attr in ts_attrs:
            with self.subTest(attr=attr):
                self.assertIn(attr, es_attrs)
        # self.assertSetEqual(ts_attrs, es_attrs)
        ts_dsets = {s.name for s in ts_spec.datasets}
        es_dsets = {s.name for s in es_spec.datasets}
        for dset in ts_dsets:
            with self.subTest(dset=dset):
                self.assertIn(dset, es_dsets)
        # self.assertSetEqual(ts_dsets, es_dsets)

    def test_inherited_attributes_not_resolved(self):
        self.ns_catalog.load_namespaces(self.namespace_path, resolve=False)
        es_spec = self.ns_catalog.get_spec(self.NS_NAME, 'SpikeData')
        src_attrs = {s.name for s in self.ext_attributes}
        ext_attrs = {s.name for s in es_spec.attributes}
        self.assertSetEqual(src_attrs, ext_attrs)
        src_dsets = {s.name for s in self.ext_datasets}
        ext_dsets = {s.name for s in es_spec.datasets}
        self.assertSetEqual(src_dsets, ext_dsets)


class TestSpecLoadEdgeCase(TestCase):

    def setUp(self):
        self.specs_path = 'test_load_namespace.specs.yaml'
        self.namespace_path = 'test_load_namespace.namespace.yaml'

        # write basically empty specs file
        to_dump = {'groups': []}
        with open(self.specs_path, 'w') as tmp:
            yaml_obj = yaml.YAML(typ='safe', pure=True)
            yaml_obj.default_flow_style = False
            yaml_obj.dump(json.loads(json.dumps(to_dump)), tmp)

    def tearDown(self):
        remove_test_file(self.namespace_path)
        remove_test_file(self.specs_path)

    def test_build_namespace_missing_version(self):
        """Test that building/creating a SpecNamespace without a version works but raises a warning."""
        # create namespace without version key
        ns_dict = {
            'doc': 'a test namespace',
            'name': 'test_ns',
            'schema': [
                {'source': self.specs_path}
            ],
        }
        msg = ("Loaded namespace 'test_ns' is missing the required key 'version'. Version will be set to "
               "'%s'. Please notify the extension author." % SpecNamespace.UNVERSIONED)
        with self.assertWarnsWith(UserWarning, msg):
            namespace = SpecNamespace.build_namespace(**ns_dict)

        self.assertEqual(namespace.version, SpecNamespace.UNVERSIONED)

    def test_load_namespace_none_version(self):
        """Test that reading a namespace file without a version works but raises a warning."""
        # create namespace with version key (remove it later)
        ns_dict = {
            'doc': 'a test namespace',
            'name': 'test_ns',
            'schema': [
                {'source': self.specs_path}
            ],
            'version': '0.0.1'
        }
        namespace = SpecNamespace.build_namespace(**ns_dict)
        namespace['version'] = None  # work around lack of setter to remove version key

        # write the namespace to file without version key
        to_dump = {'namespaces': [namespace]}
        with open(self.namespace_path, 'w') as tmp:
            yaml_obj = yaml.YAML(typ='safe', pure=True)
            yaml_obj.default_flow_style = False
            yaml_obj.dump(json.loads(json.dumps(to_dump)), tmp)

        # load the namespace from file
        ns_catalog = NamespaceCatalog()
        msg = ("Loaded namespace 'test_ns' is missing the required key 'version'. Version will be set to "
               "'%s'. Please notify the extension author." % SpecNamespace.UNVERSIONED)
        with self.assertWarnsWith(UserWarning, msg):
            ns_catalog.load_namespaces(self.namespace_path)

        self.assertEqual(ns_catalog.get_namespace('test_ns').version, SpecNamespace.UNVERSIONED)

    def test_load_namespace_unversioned_version(self):
        """Test that reading a namespace file with version=unversioned string works but raises a warning."""
        # create namespace with version key (remove it later)
        ns_dict = {
            'doc': 'a test namespace',
            'name': 'test_ns',
            'schema': [
                {'source': self.specs_path}
            ],
            'version': '0.0.1'
        }
        namespace = SpecNamespace.build_namespace(**ns_dict)
        namespace['version'] = str(SpecNamespace.UNVERSIONED)  # work around lack of setter to remove version key

        # write the namespace to file without version key
        to_dump = {'namespaces': [namespace]}
        with open(self.namespace_path, 'w') as tmp:
            yaml_obj = yaml.YAML(typ='safe', pure=True)
            yaml_obj.default_flow_style = False
            yaml_obj.dump(json.loads(json.dumps(to_dump)), tmp)

        # load the namespace from file
        ns_catalog = NamespaceCatalog()
        msg = "Loaded namespace 'test_ns' is unversioned. Please notify the extension author."
        with self.assertWarnsWith(UserWarning, msg):
            ns_catalog.load_namespaces(self.namespace_path)

        self.assertEqual(ns_catalog.get_namespace('test_ns').version, SpecNamespace.UNVERSIONED)

    def test_missing_version_string(self):
        """Test that the constant variable representing a missing version has not changed."""
        self.assertIsNone(SpecNamespace.UNVERSIONED)

    def test_get_namespace_missing_version(self):
        """Test that SpecNamespace.version returns the constant for a missing version if version gets removed."""
        # create namespace with version key (remove it later)
        ns_dict = {
            'doc': 'a test namespace',
            'name': 'test_ns',
            'schema': [
                {'source': self.specs_path}
            ],
            'version': '0.0.1'
        }
        namespace = SpecNamespace.build_namespace(**ns_dict)
        namespace['version'] = None  # work around lack of setter to remove version key

        self.assertEqual(namespace.version, SpecNamespace.UNVERSIONED)


class TestCatchDupNS(TestCase):

    def setUp(self):
        self.tempdir = gettempdir()
        self.ext_source1 = 'extension1.yaml'
        self.ns_path1 = 'namespace1.yaml'
        self.ext_source2 = 'extension2.yaml'
        self.ns_path2 = 'namespace2.yaml'

    def tearDown(self):
        for f in (self.ext_source1, self.ns_path1, self.ext_source2, self.ns_path2):
            remove_test_file(os.path.join(self.tempdir, f))

    def test_catch_dup_name(self):
        ns_builder1 = NamespaceBuilder('Extension doc', "test_ext", version='0.1.0')
        ns_builder1.add_spec(self.ext_source1, GroupSpec('doc', data_type_def='MyType'))
        ns_builder1.export(self.ns_path1, outdir=self.tempdir)
        ns_builder2 = NamespaceBuilder('Extension doc', "test_ext", version='0.2.0')
        ns_builder2.add_spec(self.ext_source2, GroupSpec('doc', data_type_def='MyType'))
        ns_builder2.export(self.ns_path2, outdir=self.tempdir)

        ns_catalog = NamespaceCatalog()
        ns_catalog.load_namespaces(os.path.join(self.tempdir, self.ns_path1))

        msg = "Ignoring cached namespace 'test_ext' version 0.2.0 because version 0.1.0 is already loaded."
        with self.assertWarnsRegex(UserWarning, msg):
            ns_catalog.load_namespaces(os.path.join(self.tempdir, self.ns_path2))

    def test_catch_dup_name_same_version(self):
        ns_builder1 = NamespaceBuilder('Extension doc', "test_ext", version='0.1.0')
        ns_builder1.add_spec(self.ext_source1, GroupSpec('doc', data_type_def='MyType'))
        ns_builder1.export(self.ns_path1, outdir=self.tempdir)
        ns_builder2 = NamespaceBuilder('Extension doc', "test_ext", version='0.1.0')
        ns_builder2.add_spec(self.ext_source2, GroupSpec('doc', data_type_def='MyType'))
        ns_builder2.export(self.ns_path2, outdir=self.tempdir)

        ns_catalog = NamespaceCatalog()
        ns_catalog.load_namespaces(os.path.join(self.tempdir, self.ns_path1))

        # no warning should be raised (but don't just check for 0 warnings -- warnings can come from other sources)
        msg = "Ignoring cached namespace 'test_ext' version 0.1.0 because version 0.1.0 is already loaded."
        with warnings.catch_warnings(record=True) as ws:
            ns_catalog.load_namespaces(os.path.join(self.tempdir, self.ns_path2))
        for w in ws:
            self.assertTrue(str(w) != msg)


class TestCustomSpecClasses(TestCase):

    def setUp(self):  # noqa: C901
        self.ns_catalog = NamespaceCatalog(CustomGroupSpec, CustomDatasetSpec, CustomSpecNamespace)
        hdmf_typemap = get_type_map()
        self.ns_catalog.merge(hdmf_typemap.namespace_catalog)

    def test_constructor_getters(self):
        self.assertEqual(self.ns_catalog.dataset_spec_cls, CustomDatasetSpec)
        self.assertEqual(self.ns_catalog.group_spec_cls, CustomGroupSpec)
        self.assertEqual(self.ns_catalog.spec_namespace_cls, CustomSpecNamespace)

    def test_load_namespaces(self):
        namespace_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test.namespace.yaml')
        namespace_deps = self.ns_catalog.load_namespaces(namespace_path)

        # test that the dependencies are correct
        expected = set(['Data', 'Container', 'DynamicTable'])
        self.assertSetEqual(set(namespace_deps['test']['hdmf-common']), expected)

        # test that the types are loaded
        types = self.ns_catalog.get_types('test.base.yaml')
        expected = ('TestData', 'TestContainer', 'TestTable')
        self.assertTupleEqual(types, expected)

        # test that the namespace is correct and the types_key is updated for test ns
        test_namespace = self.ns_catalog.get_namespace('test')
        expected = {'doc': 'Test namespace',
                    'schema': [{'namespace': 'hdmf-common',
                                'my_data_types': ['Data', 'DynamicTable', 'Container']},
                               {'doc': 'This source module contains base data types.',
                                'source': 'test.base.yaml',
                                'title': 'Base data types'}],
                    'name': 'test',
                    'full_name': 'Test',
                    'version': '0.1.0',
                    'author': ['Test test'],
                    'contact': ['test@test.com']}
        self.assertDictEqual(test_namespace, expected)

        # test that the def_key is updated for test ns
        test_data_spec = self.ns_catalog.get_spec('test', 'TestData')
        self.assertTrue('my_data_type_def' in test_data_spec)
        self.assertTrue('my_data_type_inc' in test_data_spec)

        # test that the def_key is maintained for hdmf-common
        data_spec = self.ns_catalog.get_spec('hdmf-common', 'Data')
        self.assertTrue('data_type_def' in data_spec)

    def test_load_namespaces_ext(self):
        namespace_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test.namespace.yaml')
        self.ns_catalog.load_namespaces(namespace_path)

        ext_namespace_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test-ext.namespace.yaml')
        ext_namespace_deps = self.ns_catalog.load_namespaces(ext_namespace_path)

        # test that the dependencies are correct
        expected_deps = set(['TestData', 'TestContainer', 'TestTable', 'Container', 'Data', 'DynamicTable'])
        self.assertSetEqual(set(ext_namespace_deps['test-ext']['test']), expected_deps)

    def test_load_namespaces_bad_path(self):
        namespace_path = 'test.namespace.yaml'
        msg = "namespace file 'test.namespace.yaml' not found"
        with self.assertRaisesWith(IOError, msg):
            self.ns_catalog.load_namespaces(namespace_path)

    def test_load_namespaces_twice(self):
        namespace_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'test.namespace.yaml')
        namespace_deps1 = self.ns_catalog.load_namespaces(namespace_path)
        namespace_deps2 = self.ns_catalog.load_namespaces(namespace_path)
        self.assertDictEqual(namespace_deps1, namespace_deps2)
