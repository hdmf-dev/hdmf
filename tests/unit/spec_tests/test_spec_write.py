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
            match_str = \
"""groups:
- data_type_def: MyDataSeries
  doc: A custom DataSeries interface
- data_type_def: MyExtendedMyDataSeries
  data_type_inc: MyDataSeries
  doc: An extension of a DataSeries interface
  datasets:
  - name: testdata
    dtype: float
    doc: test
"""  # noqa: E122
            nsstr = file.read()
            self.assertEqual(nsstr, match_str)

    def _test_namespace_file(self):
        with open(self.namespace_path, 'r') as file:
            match_str = \
"""namespaces:
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
""" % self.date.isoformat()  # noqa: E122
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

    def tearDown(self):
        if os.path.exists(self.ext_source_path):
            os.remove(self.ext_source_path)
        if os.path.exists(self.namespace_path):
            os.remove(self.namespace_path)

    def test_export_namespace(self):
        self._test_namespace_file()
        self._test_extensions_file()

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


class TestYAMLSpecWrite(TestSpec):

    def setUp(self):
        super().setUp()
        for data_type in self.data_types:
            self.ns_builder.add_spec(source=self.ext_source_path, spec=data_type)
        self.ns_builder.add_source(source=self.ext_source_path,
                                   doc='Extensions for my lab',
                                   title='My lab extensions')

    def tearDown(self):
        if os.path.exists(self.ext_source_path):
            os.remove(self.ext_source_path)
        if os.path.exists(self.namespace_path):
            os.remove(self.namespace_path)

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
            match_str = \
"""namespaces:
- author: foo
  contact: foo@bar.com
  date: '%s'
  doc: mydoc
  full_name: My Laboratory
  name: mylab
  schema:
  - source: mylab.extensions.yaml
  version: 0.0.1
""" % self.date.isoformat()  # noqa: E122
            nsstr = file.read()
            self.assertEqual(nsstr, match_str)

    def test_missing_data_types(self):
        """Test that calling export_spec on a namespace builder without data types raises a warning."""
        with self.assertWarnsWith(UserWarning, 'No data types specified. Exiting.'):
            export_spec(self.ns_builder, [], '.')
