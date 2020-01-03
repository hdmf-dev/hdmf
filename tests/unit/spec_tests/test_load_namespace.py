import ruamel.yaml as yaml
import json
import os

from hdmf.spec import AttributeSpec, DatasetSpec, GroupSpec, SpecNamespace, NamespaceCatalog, DimSpec
from hdmf.testing import TestCase


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
            yaml.safe_dump(json.loads(json.dumps(to_dump)), tmp, default_flow_style=False)
        ns_dict = {
            'doc': 'a test namespace',
            'name': self.NS_NAME,
            'schema': [
                {'source': self.specs_path}
            ]
        }
        self.namespace = SpecNamespace.build_namespace(**ns_dict)
        to_dump = {'namespaces': [self.namespace]}
        with open(self.namespace_path, 'w') as tmp:
            yaml.safe_dump(json.loads(json.dumps(to_dump)), tmp, default_flow_style=False)
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


class TestLoadDims(TestCase):

    NS_NAME = 'test_ns'

    def setUp(self):
        dset2_dims = (DimSpec(name='x', required=True), DimSpec(name='y', required=False))
        dset3_dims = (DimSpec(name='x', required=True), DimSpec(name='y', required=True))
        dset4_dims = ('x', 'y')

        self.datasets = [
            DatasetSpec('my first dataset',
                        'int',
                        name='dataset1',
                        linkable=True),
            DatasetSpec('my second dataset',
                        'int',
                        name='dataset2',
                        dims=dset2_dims,
                        linkable=True),
            DatasetSpec('my third dataset',
                        'int',
                        name='dataset3',
                        dims=dset3_dims,
                        linkable=True),
            DatasetSpec('my fourth dataset',
                        'int',
                        name='dataset4',
                        dims=dset4_dims,
                        linkable=True),
        ]
        self.spec = GroupSpec('A test group',
                              name='root_constructor_datatype',
                              datasets=self.datasets,
                              linkable=False,
                              data_type_def='EphysData')
        dset1_dims_ext = (DimSpec(name='x', required=True), )  # specify dims
        # require y, change names
        dset2_dims_ext = (DimSpec(name='x2', required=True), DimSpec(name='y2', required=True))
        dset4_dims_ext = (DimSpec(name='x2', required=True), DimSpec(name='y2', required=True))  # change names
        self.ext_datasets = [
            DatasetSpec('my first dataset extension',
                        'int',
                        name='dataset1',
                        dims=dset1_dims_ext,
                        linkable=True),
            DatasetSpec('my second dataset extension',
                        'int',
                        name='dataset2',
                        dims=dset2_dims_ext,
                        linkable=True),
            DatasetSpec('my fourth dataset extension',
                        'int',
                        name='dataset4',
                        dims=dset4_dims_ext,
                        linkable=True)
        ]
        self.ext_spec = GroupSpec('A test group extension',
                                  name='root_constructor_datatype',
                                  datasets=self.ext_datasets,
                                  linkable=False,
                                  data_type_inc='EphysData',
                                  data_type_def='SpikeData')
        dset2_dims_ext2 = (DimSpec(name='x', required=False), DimSpec(name='y', required=False))  # make x optional
        self.ext2_datasets = [
            DatasetSpec('my second dataset extension',
                        'int',
                        name='dataset2',
                        dims=dset2_dims_ext2,
                        linkable=True)
        ]
        self.ext2_spec = GroupSpec('A test group extension',
                                   name='root_constructor_datatype',
                                   datasets=self.ext2_datasets,
                                   linkable=False,
                                   data_type_inc='EphysData',
                                   data_type_def='InvalidData')
        to_dump = {'groups': [self.spec, self.ext_spec, self.ext2_spec]}
        self.specs_path = 'test_load_namespace.specs.yaml'
        self.namespace_path = 'test_load_namespace.namespace.yaml'
        with open(self.specs_path, 'w') as tmp:
            yaml.safe_dump(json.loads(json.dumps(to_dump)), tmp, default_flow_style=False)
        ns_dict = {
            'doc': 'a test namespace',
            'name': self.NS_NAME,
            'schema': [
                {'source': self.specs_path}
            ]
        }
        self.namespace = SpecNamespace.build_namespace(**ns_dict)
        to_dump = {'namespaces': [self.namespace]}
        with open(self.namespace_path, 'w') as tmp:
            yaml.safe_dump(json.loads(json.dumps(to_dump)), tmp, default_flow_style=False)
        self.ns_catalog = NamespaceCatalog()

    def tearDown(self):
        if os.path.exists(self.namespace_path):
            os.remove(self.namespace_path)
        if os.path.exists(self.specs_path):
            os.remove(self.specs_path)

    def test_inherited_dims(self):
        """
        Test a subclass inheriting the superclass' datasets with dims.
        """
        self.ns_catalog.load_namespaces(self.namespace_path, resolve=True)
        dset3_spec = self.ns_catalog.get_spec(self.NS_NAME, 'EphysData').datasets[2]
        dset3_ext_spec = self.ns_catalog.get_spec(self.NS_NAME, 'SpikeData').datasets[2]
        expected = (DimSpec(name='x', required=True), DimSpec(name='y', required=True))
        self.assertEqual(dset3_ext_spec.dims, dset3_spec.dims)
        self.assertEqual(dset3_ext_spec.dims, expected)

    def test_override_dims_simple(self):
        """
        Test a subclass specifying a dataset's dims to override the superclass definition of the dataset without dims.
        """
        self.ns_catalog.load_namespaces(self.namespace_path, resolve=True)
        dset1_spec = self.ns_catalog.get_spec(self.NS_NAME, 'EphysData').datasets[0]
        dset1_ext_spec = self.ns_catalog.get_spec(self.NS_NAME, 'SpikeData').datasets[0]

        self.assertIsNone(dset1_spec.dims)

        expected = (DimSpec(name='x', required=True), )
        self.assertEqual(dset1_ext_spec.dims, expected)

    def test_override_dims_stricter(self):
        """
        Test a subclass specifying a dataset's dims to override the superclass definition of the dataset with dims.

        The subclass dataset's dims are more restrictive than the superclass dataset's dims and have different names.
        """
        self.ns_catalog.load_namespaces(self.namespace_path, resolve=True)
        dset1_spec = self.ns_catalog.get_spec(self.NS_NAME, 'EphysData').datasets[1]
        dset1_ext_spec = self.ns_catalog.get_spec(self.NS_NAME, 'SpikeData').datasets[1]

        expected = (DimSpec(name='x', required=True), DimSpec(name='y', required=False))
        self.assertEqual(dset1_spec.dims, expected)

        expected_ext = (DimSpec(name='x2', required=True), DimSpec(name='y2', required=True))
        self.assertEqual(dset1_ext_spec.dims, expected_ext)
        # TODO should succeed

    def test_override_dims_looser(self):
        """
        Test a subclass cannot use override the superclass definition of a dataset with less restrictive dims.
        """
        self.ns_catalog.load_namespaces(self.namespace_path, resolve=True)
        # TODO load_namespaces should fail?

    def test_override_dims_new_over_legacy(self):
        """
        Test a subclass specifying a dataset's dims to override the superclass legacy def. of the dataset with dims.

        The subclass dataset's dims also has different names.
        """
        self.ns_catalog.load_namespaces(self.namespace_path, resolve=True)
        dset4_spec = self.ns_catalog.get_spec(self.NS_NAME, 'EphysData').datasets[3]
        dset4_ext_spec = self.ns_catalog.get_spec(self.NS_NAME, 'SpikeData').datasets[3]

        expected = (DimSpec(name='x', required=True), DimSpec(name='y', required=True))
        self.assertEqual(dset4_spec.dims, expected)

        expected_ext = (DimSpec(name='x2', required=True), DimSpec(name='y2', required=True))
        self.assertEqual(dset4_ext_spec.dims, expected_ext)
        # TODO should succeed
