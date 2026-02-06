import datetime
import os

from hdmf.spec.namespace import SpecNamespace, NamespaceCatalog
from hdmf.spec.spec import GroupSpec
from hdmf.spec.write import NamespaceBuilder, YAMLSpecWriter, export_spec
from hdmf.testing import TestCase


class TestSpec(TestCase):

    def setUp(self):
        # create a builder for the namespace
        self.ns_name = "mylab"
        self.date = datetime.datetime.now()

        self.ns_builder = NamespaceBuilder(doc="mydoc",
                                           name=self.ns_name,
                                           full_name="My Laboratory",
                                           version="0.0.1",
                                           author="foo",
                                           contact="foo@bar.com",
                                           namespace_cls=SpecNamespace,
                                           date=self.date)

        # create extensions
        ext1 = GroupSpec('A custom DataSeries interface',
                         attributes=[],
                         datasets=[],
                         groups=[],
                         data_type_inc=None,
                         data_type_def='MyDataSeries')

        ext2 = GroupSpec('An extension of a DataSeries interface',
                         attributes=[],
                         datasets=[],
                         groups=[],
                         data_type_inc='MyDataSeries',
                         data_type_def='MyExtendedMyDataSeries')

        ext2.add_dataset(doc='test',
                         dtype='float',
                         name='testdata')

        self.data_types = [ext1, ext2]

        # add the extension
        self.ext_source_path = 'mylab.extensions.yaml'
        self.namespace_path = 'mylab.namespace.yaml'

    def _test_extensions_file(self):
        with open(self.ext_source_path, 'r') as file:
            match_str = """groups:
- data_type_def: MyDataSeries
  doc: A custom DataSeries interface
- data_type_def: MyExtendedMyDataSeries
  data_type_inc: MyDataSeries
  doc: An extension of a DataSeries interface
  datasets:
  - name: testdata
    dtype: float
    doc: test
"""
            nsstr = file.read()
            self.assertEqual(nsstr, match_str)

    def _test_namespace_file(self):
        with open(self.namespace_path, 'r') as file:
            match_str = """namespaces:
- author: foo
  contact: foo@bar.com
  date: '%s'
  doc: mydoc
  full_name: My Laboratory
  name: mylab
  schema:
  - doc: Extensions for my lab
    source: mylab.extensions.yaml
    title: Extensions for my lab
  version: 0.0.1
""" % self.date.isoformat()
            nsstr = file.read()
            self.assertEqual(nsstr, match_str)


class TestNamespaceBuilder(TestSpec):
    NS_NAME = 'test_ns'

    def setUp(self):
        super().setUp()
        for data_type in self.data_types:
            self.ns_builder.add_spec(source=self.ext_source_path, spec=data_type)
        self.ns_builder.add_source(source=self.ext_source_path,
                                   doc='Extensions for my lab',
                                   title='My lab extensions')
        self.ns_builder.export(self.namespace_path)

        # Additional paths for export tests
        self.output_path = "test_export.namespace.yaml"
        self.source_path = "test_source.yaml"

        # Create a test spec for reuse
        self.test_spec = GroupSpec('A test group',
                                 data_type_def='TestGroup',
                                 datasets=[],
                                 attributes=[])

    def tearDown(self):
        if os.path.exists(self.ext_source_path):
            os.remove(self.ext_source_path)
        if os.path.exists(self.namespace_path):
            os.remove(self.namespace_path)

        # Additional cleanup for export tests
        if os.path.exists(self.output_path):
            os.remove(self.output_path)
        if os.path.exists(self.source_path):
            os.remove(self.source_path)

    def test_export_namespace(self):
        """Test basic namespace export functionality."""
        self._test_namespace_file()
        self._test_extensions_file()

    def test_export_with_included_types(self):
        """Test export with included types from source."""
        self.ns_builder.include_type('TestType1', source=self.source_path)
        self.ns_builder.include_type('TestType2', source=self.source_path)

        self.ns_builder.export(self.output_path)

        # Verify the exported namespace
        with open(self.output_path, 'r') as f:
            content = f.read()
            # Check that both types are included
            self.assertIn('TestType1', content)
            self.assertIn('TestType2', content)
            # Check they're included from the correct source
            self.assertIn(self.source_path, content)

    def test_export_with_included_namespaces(self):
        """Test export with included namespaces."""
        namespace = "test_namespace"
        self.ns_builder.include_namespace(namespace)
        self.ns_builder.include_type('TestType1', namespace=namespace)

        self.ns_builder.export(self.output_path)

        # Verify the exported namespace
        with open(self.output_path, 'r') as f:
            content = f.read()
            self.assertIn(namespace, content)
            self.assertIn('TestType1', content)

    def test_export_source_with_specs(self):
        """Test export with source containing specs."""
        self.ns_builder.add_spec(self.source_path, self.test_spec)
        self.ns_builder.export(self.output_path)

        # Verify the spec was written to source file
        self.assertTrue(os.path.exists(self.source_path))
        with open(self.source_path, 'r') as f:
            content = f.read()
            self.assertIn('TestGroup', content)
            self.assertIn('A test group', content)

    def test_export_source_conflict_error(self):
        """Test error when trying to both include from and write to same source."""
        # Add both an included type and a spec to the same source
        self.ns_builder.include_type('TestType', source=self.source_path)
        self.ns_builder.add_spec(self.source_path, self.test_spec)

        # Verify export raises error
        with self.assertRaises(ValueError):
            self.ns_builder.export(self.output_path)

    def test_export_source_with_doc_title(self):
        """Test export with source containing doc and title."""
        self.ns_builder.add_source(self.source_path,
                                 doc='Test documentation',
                                 title='Test Title')
        self.ns_builder.add_spec(self.source_path, self.test_spec)

        self.ns_builder.export(self.output_path)

        # Verify doc and title in namespace file
        with open(self.output_path, 'r') as f:
            content = f.read()
            self.assertIn('doc: Test documentation', content)
            self.assertIn('title: Test documentation', content)

    def test_read_namespace(self):
        ns_catalog = NamespaceCatalog()
        ns_catalog.load_namespaces(self.namespace_path, resolve=True)
        loaded_ns = ns_catalog.get_namespace(self.ns_name)
        self.assertEqual(loaded_ns.doc, "mydoc")
        self.assertEqual(loaded_ns.author, "foo")
        self.assertEqual(loaded_ns.contact, "foo@bar.com")
        self.assertEqual(loaded_ns.full_name, "My Laboratory")
        self.assertEqual(loaded_ns.name, "mylab")
        self.assertEqual(loaded_ns.date, self.date.isoformat())
        self.assertDictEqual(loaded_ns.schema[0], {'doc': 'Extensions for my lab',
                                                   'source': 'mylab.extensions.yaml',
                                                   'title': 'Extensions for my lab'})
        self.assertEqual(loaded_ns.version, "0.0.1")

    def test_get_source_files(self):
        ns_catalog = NamespaceCatalog()
        ns_catalog.load_namespaces(self.namespace_path, resolve=True)
        loaded_ns = ns_catalog.get_namespace(self.ns_name)
        self.assertListEqual(loaded_ns.get_source_files(), ['mylab.extensions.yaml'])

    def test_get_source_description(self):
        ns_catalog = NamespaceCatalog()
        ns_catalog.load_namespaces(self.namespace_path, resolve=True)
        loaded_ns = ns_catalog.get_namespace(self.ns_name)
        descr = loaded_ns.get_source_description('mylab.extensions.yaml')
        self.assertDictEqual(descr, {'doc': 'Extensions for my lab',
                                     'source': 'mylab.extensions.yaml',
                                     'title': 'Extensions for my lab'})

    def test_missing_version(self):
        """Test that creating a namespace builder without a version raises an error."""
        msg = "Namespace '%s' missing key 'version'. Please specify a version for the extension." % self.ns_name
        with self.assertRaisesWith(ValueError, msg):
            self.ns_builder = NamespaceBuilder(doc="mydoc",
                                               name=self.ns_name,
                                               full_name="My Laboratory",
                                               author="foo",
                                               contact="foo@bar.com",
                                               namespace_cls=SpecNamespace,
                                               date=self.date)

    def test_include_type(self):
        """Test including types from source files and namespaces."""
        # Test including type from source
        source_path = "test_source.yaml"
        self.ns_builder.include_type('TestType', source=source_path)
        self.assertIn(source_path, self.ns_builder._NamespaceBuilder__sources)
        self.assertIn('TestType', self.ns_builder._NamespaceBuilder__sources[source_path].get('data_types', []))

        # Test including type from namespace
        namespace = "test_namespace"
        self.ns_builder.include_type('TestType2', namespace=namespace)
        self.assertIn(namespace, self.ns_builder._NamespaceBuilder__namespaces)
        self.assertIn('TestType2', self.ns_builder._NamespaceBuilder__namespaces[namespace].get('data_types', []))

        # Test error when neither source nor namespace is provided
        msg = "must specify 'source' or 'namespace' when including type"
        with self.assertRaisesWith(ValueError, msg):
            self.ns_builder.include_type('TestType3')

        # Test including multiple types from same source
        self.ns_builder.include_type('TestType4', source=source_path)
        types_in_source = self.ns_builder._NamespaceBuilder__sources[source_path].get('data_types', [])
        self.assertIn('TestType', types_in_source)
        self.assertIn('TestType4', types_in_source)

        # Test including multiple types from same namespace
        self.ns_builder.include_type('TestType5', namespace=namespace)
        types_in_namespace = self.ns_builder._NamespaceBuilder__namespaces[namespace].get('data_types', [])
        self.assertIn('TestType2', types_in_namespace)
        self.assertIn('TestType5', types_in_namespace)


class TestYAMLSpecWrite(TestSpec):

    def setUp(self):
        super().setUp()
        for data_type in self.data_types:
            self.ns_builder.add_spec(source=self.ext_source_path, spec=data_type)
        self.ns_builder.add_source(source=self.ext_source_path,
                                   doc='Extensions for my lab',
                                   title='My lab extensions')

        # Create a temporary YAML file for reorder_yaml testing
        self.temp_yaml = 'temp_test.yaml'
        with open(self.temp_yaml, 'w') as f:
            f.write("""
doc: test doc
name: test name
dtype: int
attributes:
- name: attr1
  doc: attr1 doc
  dtype: float
groups:
- name: group1
  doc: group1 doc
  datasets:
  - name: dataset1
    doc: dataset1 doc
    dtype: int
""")

    def tearDown(self):
        if os.path.exists(self.ext_source_path):
            os.remove(self.ext_source_path)
        if os.path.exists(self.namespace_path):
            os.remove(self.namespace_path)
        if os.path.exists(self.temp_yaml):
            os.remove(self.temp_yaml)

    def test_init(self):
        temp = YAMLSpecWriter('.')
        self.assertEqual(temp._YAMLSpecWriter__outdir, '.')

    def test_write_namespace(self):
        temp = YAMLSpecWriter()
        self.ns_builder.export(self.namespace_path, writer=temp)
        self._test_namespace_file()
        self._test_extensions_file()

    def test_get_name(self):
        self.assertEqual(self.ns_name, self.ns_builder.name)

    def test_reorder_yaml(self):
        """Test that reorder_yaml correctly loads, reorders, and saves a YAML file."""
        writer = YAMLSpecWriter()

        # Reorder the YAML file
        writer.reorder_yaml(self.temp_yaml)

        # Read the reordered content
        with open(self.temp_yaml, 'r') as f:
            content = f.read()

        # Verify the order of keys in the reordered content
        # The name should come before dtype and doc
        name_pos = content.find('name: test name')
        dtype_pos = content.find('dtype: int')
        doc_pos = content.find('doc: test doc')
        self.assertLess(name_pos, dtype_pos)
        self.assertLess(dtype_pos, doc_pos)

        # Verify nested structures are also reordered
        attr_block = content[content.find('- name: attr1'):content.find('groups:')]
        self.assertLess(attr_block.find('name: attr1'), attr_block.find('dtype: float'))
        self.assertLess(attr_block.find('dtype: float'), attr_block.find('doc: attr1 doc'))

    def test_sort_keys(self):
        """Test that sort_keys correctly orders dictionary keys according to the predefined order."""
        # Test basic ordering with predefined keys
        input_dict = {
            'doc': 'documentation',
            'dtype': 'int',
            'name': 'test_name',
            'attributes': [1],
            'datasets': [2],
            'groups': [3]
        }
        result = YAMLSpecWriter.sort_keys(input_dict)

        # Check that the keys are in the correct order
        expected_order = ['name', 'dtype', 'doc', 'attributes', 'datasets', 'groups']
        self.assertEqual(list(result.keys()), expected_order)

        # Test neurodata_type_def positioning
        input_dict = {
            'doc': 'documentation',
            'name': 'test_name',
            'neurodata_type_def': 'MyType',
            'attributes': [1]
        }
        result = YAMLSpecWriter.sort_keys(input_dict)
        self.assertEqual(list(result.keys())[0], 'neurodata_type_def')

        # Test nested dictionary ordering
        input_dict = {
            'doc': 'documentation',
            'nested': {
                'groups': [1],
                'name': 'nested_name',
                'dtype': 'int',
                'attributes': [2]
            }
        }
        result = YAMLSpecWriter.sort_keys(input_dict)
        self.assertEqual(list(result['nested'].keys()), ['name', 'dtype', 'attributes', 'groups'])

        # Test list handling
        input_dict = {
            'attributes': [
                {'doc': 'attr1', 'name': 'attr1_name', 'dtype': 'int'},
                {'doc': 'attr2', 'name': 'attr2_name', 'dtype': 'float'}
            ]
        }
        result = YAMLSpecWriter.sort_keys(input_dict)
        for attr in result['attributes']:
            self.assertEqual(list(attr.keys()), ['name', 'dtype', 'doc'])

        # Test tuple handling
        input_tuple = (
            {'doc': 'item1', 'name': 'name1', 'dtype': 'int'},
            {'doc': 'item2', 'name': 'name2', 'dtype': 'float'}
        )
        result = YAMLSpecWriter.sort_keys(input_tuple)
        # Convert generator to list for testing
        result_list = list(result)
        for item in result_list:
            self.assertEqual(list(item.keys()), ['name', 'dtype', 'doc'])
        # Verify the original order is maintained
        self.assertEqual(result_list[0]['name'], 'name1')
        self.assertEqual(result_list[1]['name'], 'name2')

class TestExportSpec(TestSpec):

    def test_export(self):
        """Test that export_spec writes the correct files."""
        export_spec(self.ns_builder, self.data_types, '.')
        self._test_namespace_file()
        self._test_extensions_file()

    def tearDown(self):
        if os.path.exists(self.ext_source_path):
            os.remove(self.ext_source_path)
        if os.path.exists(self.namespace_path):
            os.remove(self.namespace_path)

    def _test_namespace_file(self):
        with open(self.namespace_path, 'r') as file:
            match_str = """namespaces:
- author: foo
  contact: foo@bar.com
  date: '%s'
  doc: mydoc
  full_name: My Laboratory
  name: mylab
  schema:
  - source: mylab.extensions.yaml
  version: 0.0.1
""" % self.date.isoformat()
            nsstr = file.read()
            self.assertEqual(nsstr, match_str)

    def test_missing_data_types(self):
        """Test that calling export_spec on a namespace builder without data types raises a warning."""
        with self.assertWarnsWith(UserWarning, 'No data types specified. Exiting.'):
            export_spec(self.ns_builder, [], '.')
