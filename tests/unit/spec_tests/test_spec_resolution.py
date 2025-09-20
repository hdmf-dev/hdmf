"""
Tests for the spec resolution system.

This module tests the resolution functionality that allows specs to be resolved
after loading, including cross-namespace resolution capabilities.
"""

from pathlib import Path
import tempfile
import json
import ruamel.yaml as yaml
import shutil

from hdmf.spec import (
    AttributeSpec,
    DatasetSpec,
    DtypeSpec,
    GroupSpec,
    SpecNamespace,
    NamespaceCatalog,
)
from hdmf.spec.spec import BaseStorageSpec
from hdmf.testing import TestCase


class TestSpecResolution(TestCase):
    """Test the spec resolution system."""

    def setUp(self):
        """Set up test specs and namespaces."""
        # Create base specs
        self.base_attr = AttributeSpec(
            name="base_attr", dtype="text", doc="Base attribute"
        )
        self.base_dataset = DatasetSpec(
            data_type_def="BaseDataset",
            name="base_dataset",
            dtype="int",
            doc="Base dataset",
            attributes=[self.base_attr],
        )
        self.base_group = GroupSpec(
            data_type_def="BaseGroup",
            name="base_group",
            doc="Base group",
            datasets=[
                DatasetSpec(
                    data_type_inc="BaseDataset",
                    name="base_dataset",
                    doc="Base dataset reference",
                )
            ],
        )

        # Create extending specs
        self.ext_attr = AttributeSpec(
            name="ext_attr", dtype="text", doc="Extension attribute"
        )
        self.ext_dataset = DatasetSpec(
            data_type_inc="BaseDataset",
            data_type_def="ExtDataset",
            name="ext_dataset",
            dtype="int",
            doc="Extended dataset",
            attributes=[self.ext_attr],
        )
        self.ext_group = GroupSpec(
            data_type_inc="BaseGroup",
            data_type_def="ExtGroup",
            name="ext_group",
            doc="Extended group",
            datasets=[
                DatasetSpec(
                    data_type_inc="ExtDataset",
                    name="ext_dataset",
                    doc="Extended dataset reference",
                )
            ],
        )

    def test_resolve_inc_spec_basic(self):
        """Test basic resolve_inc_spec functionality."""
        # Initially, the extension should not be resolved
        # Note: resolved property is managed by the overarching NamespaceCatalog on subspecs being resolved
        self.assertFalse(self.ext_dataset.inc_spec_resolved)

        # Resolve the extension
        self.ext_dataset.resolve_inc_spec(self.base_dataset)

        # Check that resolution flags are set
        self.assertTrue(self.ext_dataset.inc_spec_resolved)

        # Check that attributes are inherited (ext_attr should be present, base_attr should be inherited)
        ext_attrs = {attr.name: attr for attr in self.ext_dataset.attributes}
        self.assertIn("base_attr", ext_attrs)
        self.assertIn("ext_attr", ext_attrs)

        # Check inheritance tracking
        self.assertTrue(self.ext_dataset.is_inherited_attribute("base_attr"))
        self.assertFalse(self.ext_dataset.is_inherited_attribute("ext_attr"))

    def test_resolve_inc_spec_group(self):
        """Test resolve_inc_spec for GroupSpec."""
        # Initially, the extension should not be resolved
        self.assertFalse(self.ext_group.resolved)
        self.assertFalse(self.ext_group.inc_spec_resolved)

        # Resolve the extension
        self.ext_group.resolve_inc_spec(self.base_group)

        # Check that resolution flags are set
        self.assertTrue(self.ext_group.inc_spec_resolved)
        # Note: GroupSpec resolution depends on subspecs being resolved

        # Check that datasets are inherited
        ext_datasets = {dset.name: dset for dset in self.ext_group.datasets}
        self.assertIn("base_dataset", ext_datasets)
        self.assertIn("ext_dataset", ext_datasets)

        # Check inheritance tracking
        self.assertTrue(self.ext_group.is_inherited_dataset("base_dataset"))
        self.assertFalse(self.ext_group.is_inherited_dataset("ext_dataset"))

    def test_resolve_inc_spec_attribute_override(self):
        """Test that attribute overrides work correctly."""
        # Create an extension that overrides an attribute with compatible type
        override_attr = AttributeSpec(
            name="base_attr",
            dtype="text",
            value="overridden",
            doc="Overridden attribute",
        )
        ext_dataset = DatasetSpec(
            data_type_inc="BaseDataset",
            data_type_def="ExtDataset",
            name="ext_dataset",
            dtype="int",
            doc="Extended dataset with override",
            attributes=[override_attr],
        )

        # Resolve the extension
        ext_dataset.resolve_inc_spec(self.base_dataset)

        # Check that the attribute was overridden
        ext_attr = ext_dataset.get_attribute("base_attr")
        self.assertEqual(ext_attr.dtype, "text")
        self.assertEqual(ext_attr.value, "overridden")
        self.assertEqual(ext_attr.doc, "Overridden attribute")

        # Check override tracking
        self.assertTrue(ext_dataset.is_overridden_attribute("base_attr"))

    def test_resolve_inc_spec_dtype_extension(self):
        """Test that dtype extensions work correctly."""
        # Create a base dataset with compound dtype
        base_dtype = [
            DtypeSpec(name="col1", dtype="int", doc="First column"),
            DtypeSpec(name="col2", dtype="float", doc="Second column"),
        ]
        base_dataset = DatasetSpec(
            data_type_def="BaseCompound", dtype=base_dtype, doc="Base compound dataset"
        )

        # Create an extension that adds a column
        ext_dtype = [DtypeSpec(name="col3", dtype="text", doc="Third column")]
        ext_dataset = DatasetSpec(
            data_type_inc="BaseCompound",
            data_type_def="ExtCompound",
            dtype=ext_dtype,
            doc="Extended compound dataset",
        )

        # Resolve the extension
        ext_dataset.resolve_inc_spec(base_dataset)

        # Check that all columns are present
        self.assertEqual(len(ext_dataset.dtype), 3)
        col_names = [col.name for col in ext_dataset.dtype]
        self.assertIn("col1", col_names)
        self.assertIn("col2", col_names)
        self.assertIn("col3", col_names)

    def test_resolve_inc_spec_dtype_override(self):
        """Test that dtype column overrides work correctly."""
        base_dtype = [
            DtypeSpec(name="col1", dtype="int32", doc="First column"),
            DtypeSpec(name="col2", dtype="float32", doc="Second column"),
        ]
        base_dataset = DatasetSpec(
            data_type_def="BaseCompound", dtype=base_dtype, doc="Base compound dataset"
        )

        # Create an extension that overrides col2 with higher precision
        ext_dtype = [
            DtypeSpec(
                name="col2", dtype="float64", doc="Second column with higher precision"
            )
        ]
        ext_dataset = DatasetSpec(
            data_type_inc="BaseCompound",
            data_type_def="ExtCompound",
            dtype=ext_dtype,
            doc="Extended compound dataset",
        )

        # Resolve the extension
        ext_dataset.resolve_inc_spec(base_dataset)

        # Check that col2 was overridden with higher precision
        col2 = next(col for col in ext_dataset.dtype if col.name == "col2")
        self.assertEqual(col2.dtype, "float64")
        self.assertEqual(col2.doc, "Second column with higher precision")

    def test_resolve_inc_spec_dtype_invalid_override(self):
        """Test that invalid dtype overrides raise errors."""
        base_dtype = [DtypeSpec(name="col1", dtype="float64", doc="First column")]
        base_dataset = DatasetSpec(
            data_type_def="BaseCompound", dtype=base_dtype, doc="Base compound dataset"
        )

        # Create an extension that tries to override with lower precision
        ext_dtype = [
            DtypeSpec(
                name="col1", dtype="float32", doc="First column with lower precision"
            )
        ]
        ext_dataset = DatasetSpec(
            data_type_inc="BaseCompound",
            data_type_def="ExtCompound",
            dtype=ext_dtype,
            doc="Extended compound dataset",
        )

        # This should raise an error
        with self.assertRaises(ValueError) as cm:
            ext_dataset.resolve_inc_spec(base_dataset)
        self.assertIn("Cannot extend float64 to float32", str(cm.exception))

    def test_resolve_inc_spec_shape_inheritance(self):
        """Test that shape is inherited correctly."""
        base_dataset = DatasetSpec(
            data_type_def="BaseWithShape",
            dtype="int",
            dims=("x", "y"),
            shape=(None, 3),
            doc="Base dataset with shape",
        )

        ext_dataset = DatasetSpec(
            data_type_inc="BaseWithShape",
            data_type_def="ExtWithShape",
            dtype="int",
            doc="Extended dataset",
        )

        # Resolve the extension
        ext_dataset.resolve_inc_spec(base_dataset)

        # Check that shape and dims are inherited
        self.assertEqual(ext_dataset.shape, (None, 3))
        self.assertEqual(ext_dataset.dims, ("x", "y"))

    def test_resolve_inc_spec_default_value_inheritance(self):
        """Test that default_value is inherited correctly."""
        base_dataset = DatasetSpec(
            data_type_def="BaseWithValue",
            dtype="int",
            default_value=42,
            doc="Base dataset with value",
        )

        ext_dataset = DatasetSpec(
            data_type_inc="BaseWithValue",
            data_type_def="ExtWithValue",
            dtype="int",
            doc="Extended dataset",
        )

        # Resolve the extension
        ext_dataset.resolve_inc_spec(base_dataset)

        # Check that default_value is inherited
        self.assertEqual(ext_dataset.default_value, 42)

    def test_resolve_inc_spec_value_inheritance(self):
        """Test that value is inherited correctly."""
        base_dataset = DatasetSpec(
            data_type_def="BaseWithValue",
            dtype="int",
            value=42,
            doc="Base dataset with value",
        )

        ext_dataset = DatasetSpec(
            data_type_inc="BaseWithValue",
            data_type_def="ExtWithValue",
            dtype="int",
            doc="Extended dataset",
        )

        # Resolve the extension
        ext_dataset.resolve_inc_spec(base_dataset)

        # Check that value is inherited
        self.assertEqual(ext_dataset.value, 42)

    def test_resolve_inc_spec_default_value_value_inheritance(self):
        """Test that default_value and value are inherited correctly."""
        base_dataset = DatasetSpec(
            data_type_def="BaseWithValue",
            dtype="int",
            default_value=42,
            doc="Base dataset with value",
        )

        ext_dataset = DatasetSpec(
            data_type_inc="BaseWithValue",
            data_type_def="ExtWithValue",
            value=100,
            dtype="int",
            doc="Extended dataset",
        )

        # Resolve the extension
        ext_dataset.resolve_inc_spec(base_dataset)

        # Check that default_value is nullified by the fixed value
        self.assertEqual(ext_dataset.value, 100)
        self.assertIsNone(ext_dataset.default_value)

    def test_resolved_property_setter(self):
        """Test that the resolved property can be set."""
        self.assertFalse(self.ext_dataset.resolved)

        # Set to True
        self.ext_dataset.resolved = True
        self.assertTrue(self.ext_dataset.resolved)

        # Set to False
        self.ext_dataset.resolved = False
        self.assertFalse(self.ext_dataset.resolved)

        # Invalid value should raise error
        with self.assertRaises(ValueError):
            self.ext_dataset.resolved = "not a boolean"


class TestNamespaceCatalogResolution(TestCase):
    """Test the NamespaceCatalog resolution functionality."""

    def setUp(self):
        """Set up test namespaces and specs."""
        self.tempdir = Path(tempfile.mkdtemp())
        self.ns_catalog = NamespaceCatalog()

    def tearDown(self):
        """Clean up temporary files."""
        shutil.rmtree(self.tempdir)

    def create_test_namespace(
        self, name: str, specs: list[BaseStorageSpec], dependencies: list[str] = None
    ) -> str:
        """Helper to create a test namespace with given specs."""
        # Create specs file
        specs_filename = f"{name}.specs.yaml"
        specs_path = self.tempdir / specs_filename

        specs_dict = {"groups": [], "datasets": []}
        for spec in specs:
            if isinstance(spec, GroupSpec):
                specs_dict["groups"].append(spec)
            elif isinstance(spec, DatasetSpec):
                specs_dict["datasets"].append(spec)

        with open(specs_path, "w") as f:
            yaml_obj = yaml.YAML(typ="safe", pure=True)
            yaml_obj.default_flow_style = False
            yaml_obj.dump(json.loads(json.dumps(specs_dict)), f)

        # Create namespace file
        ns_path = self.tempdir / f"{name}.namespace.yaml"

        schema = [{"source": specs_filename}]
        if dependencies:
            for dep in dependencies:
                schema.insert(0, {"namespace": dep})

        ns_dict = {
            "doc": f"Test namespace {name}",
            "name": name,
            "schema": schema,
            "version": "1.0.0",
        }

        namespace = SpecNamespace.build_namespace(**ns_dict)
        ns_file_dict = {"namespaces": [namespace]}

        with open(ns_path, "w") as f:
            yaml_obj = yaml.YAML(typ="safe", pure=True)
            yaml_obj.default_flow_style = False
            yaml_obj.dump(json.loads(json.dumps(ns_file_dict)), f)

        return str(ns_path)

    def test_get_namespace_for_type(self):
        """Test NamespaceCatalog.get_namespace_for_type method."""
        # Create a simple namespace
        base_spec = GroupSpec(data_type_def="BaseType", doc="Base group")
        ns_path = self.create_test_namespace("test", [base_spec])

        # Load namespace
        self.ns_catalog.load_namespaces(ns_path)

        # Test getting namespace for type
        namespace = self.ns_catalog.get_namespace_for_type("BaseType")
        self.assertIsNotNone(namespace)
        self.assertEqual(namespace.name, "test")

        # Test non-existent type
        namespace = self.ns_catalog.get_namespace_for_type("NonExistentType")
        self.assertIsNone(namespace)

    def test_get_spec_for_type(self):
        """Test NamespaceCatalog.get_spec_for_type method."""
        # Create a simple namespace
        base_spec = GroupSpec(data_type_def="BaseType", doc="Base group")
        ns_path = self.create_test_namespace("test", [base_spec])

        # Load namespace
        self.ns_catalog.load_namespaces(ns_path)

        # Test getting spec for type
        spec = self.ns_catalog.get_spec_for_type("BaseType")
        self.assertIsNotNone(spec)
        self.assertEqual(spec.data_type_def, "BaseType")

        # Test non-existent type
        msg = "Namespace for data_type 'NonExistentType' not found"
        with self.assertRaisesWith(ValueError, msg):
            self.ns_catalog.get_spec_for_type("NonExistentType")

    def test_resolve_all_specs_simple(self):
        """Test NamespaceCatalog.resolve_all_specs with simple inheritance."""
        # Create base and extension specs
        base_spec = GroupSpec(data_type_def="BaseType", doc="Base group")
        ext_spec = GroupSpec(
            data_type_inc="BaseType", data_type_def="ExtType", doc="Extended group"
        )

        ns_path = self.create_test_namespace("test", [base_spec, ext_spec])

        # Load namespace without resolution
        self.ns_catalog.load_namespaces(ns_path, resolve=False)

        # Check that specs are not resolved
        ext_loaded = self.ns_catalog.get_spec_for_type("ExtType")
        self.assertFalse(ext_loaded.resolved)
        self.assertFalse(ext_loaded.inc_spec_resolved)

        # Resolve all specs
        self.ns_catalog.resolve_all_specs()

        # Check that specs are now resolved
        ext_loaded = self.ns_catalog.get_spec_for_type("ExtType")
        self.assertTrue(ext_loaded.resolved)
        self.assertTrue(ext_loaded.inc_spec_resolved)

    def test_resolve_all_specs_cross_namespace(self):
        """Test resolve_all_specs with cross-namespace inheritance."""
        # Create base namespace
        base_spec = GroupSpec(data_type_def="BaseType", doc="Base group")
        base_ns_path = self.create_test_namespace("base", [base_spec])

        # Create extension namespace that depends on base
        ext_spec = GroupSpec(
            data_type_inc="BaseType", data_type_def="ExtType", doc="Extended group"
        )
        ext_ns_path = self.create_test_namespace(
            "ext", [ext_spec], dependencies=["base"]
        )

        # Load both namespaces
        self.ns_catalog.load_namespaces(base_ns_path, resolve=False)
        self.ns_catalog.load_namespaces(ext_ns_path, resolve=False)

        # Check that extension spec is not resolved
        ext_loaded = self.ns_catalog.get_spec_for_type("ExtType")
        self.assertFalse(ext_loaded.resolved)

        # Resolve all specs
        self.ns_catalog.resolve_all_specs()

        # Check that extension spec is now resolved
        ext_loaded = self.ns_catalog.get_spec_for_type("ExtType")
        self.assertTrue(ext_loaded.resolved)
        self.assertTrue(ext_loaded.inc_spec_resolved)

    def test_resolve_all_specs_complex_hierarchy(self):
        """Test resolve_all_specs with complex inheritance hierarchy."""
        # Create a chain of inheritance: Base -> Mid -> Ext
        base_spec = GroupSpec(data_type_def="BaseType", doc="Base group")
        mid_spec = GroupSpec(
            data_type_inc="BaseType", data_type_def="MidType", doc="Mid group"
        )
        ext_spec = GroupSpec(
            data_type_inc="MidType", data_type_def="ExtType", doc="Extended group"
        )

        ns_path = self.create_test_namespace("test", [base_spec, mid_spec, ext_spec])

        # Load namespace without resolution
        self.ns_catalog.load_namespaces(ns_path, resolve=False)

        # Resolve all specs
        self.ns_catalog.resolve_all_specs()

        # Check that all specs are resolved
        for type_name in ["BaseType", "MidType", "ExtType"]:
            spec = self.ns_catalog.get_spec_for_type(type_name)
            self.assertTrue(spec.resolved)
            if spec.data_type_inc:
                self.assertTrue(spec.inc_spec_resolved)

    def test_resolve_all_specs_circular_dependency_error(self):
        """Test that circular dependencies are detected and raise an error."""
        # Create circular dependency: A -> B -> A
        spec_a = GroupSpec(data_type_inc="TypeB", data_type_def="TypeA", doc="Group A")
        spec_b = GroupSpec(data_type_inc="TypeA", data_type_def="TypeB", doc="Group B")

        ns_path = self.create_test_namespace("test", [spec_a, spec_b])

        # Load namespace without resolution
        self.ns_catalog.load_namespaces(ns_path, resolve=False)

        # Attempting to resolve should raise an error
        with self.assertRaises(RuntimeError) as cm:
            self.ns_catalog.resolve_all_specs()
        self.assertIn("Could not resolve all specifications", str(cm.exception))
        self.assertIn("TypeA", str(cm.exception))
        self.assertIn("TypeB", str(cm.exception))

    def test_resolve_all_specs_with_subspecs(self):
        """Test resolve_all_specs with specs that have subspecs."""
        # Create base dataset
        base_dataset = DatasetSpec(
            data_type_def="BaseDataset", dtype="int", doc="Base dataset"
        )

        # Create group with dataset subspec
        group_with_dataset = GroupSpec(
            data_type_def="GroupWithDataset",
            doc="Group with dataset",
            datasets=[
                DatasetSpec(
                    data_type_inc="BaseDataset", name="sub_dataset", doc="Sub dataset"
                )
            ],
        )

        ns_path = self.create_test_namespace("test", [base_dataset, group_with_dataset])

        # Load namespace with resolution
        self.ns_catalog.load_namespaces(ns_path)

        # Check that group and its subspecs are resolved
        group_spec = self.ns_catalog.get_spec_for_type("GroupWithDataset")
        self.assertTrue(group_spec.resolved)

        # Check that the dataset subspec is also resolved
        dataset_subspec = group_spec.datasets[0]
        self.assertTrue(dataset_subspec.resolved)
        self.assertTrue(dataset_subspec.inc_spec_resolved)


class TestSpecResolutionEdgeCases(TestCase):
    """Test edge cases and error conditions in spec resolution."""

    def test_resolve_inc_spec_wrong_type(self):
        """Test error when trying to resolve DatasetSpec with GroupSpec."""
        base_group = GroupSpec(data_type_def="BaseGroup", doc="Base group")
        ext_dataset = DatasetSpec(
            data_type_inc="BaseGroup",
            data_type_def="ExtDataset",
            dtype="int",
            doc="Extended dataset",
        )

        # This should raise an error from docval type checking
        with self.assertRaises(TypeError):
            ext_dataset.resolve_inc_spec(base_group)

    def test_resolve_inc_spec_compound_to_simple_dtype_error(self):
        """Test error when trying to extend compound dtype to simple dtype."""
        # Base with compound dtype
        base_dtype = [DtypeSpec(name="col1", dtype="int", doc="Column 1")]
        base_dataset = DatasetSpec(
            data_type_def="BaseCompound", dtype=base_dtype, doc="Base dataset"
        )

        # Extension with simple dtype
        ext_dataset = DatasetSpec(
            data_type_inc="BaseCompound",
            data_type_def="ExtSimple",
            dtype="int",
            doc="Extended dataset",
        )

        # This should raise an error
        msg = "Cannot extend compound data type to simple data type"
        with self.assertRaisesWith(ValueError, msg):
            ext_dataset.resolve_inc_spec(base_dataset)

    def test_resolve_inc_spec_simple_to_compound_dtype_error(self):
        """Test error when trying to extend simple dtype to compound dtype."""
        # Base with simple dtype
        base_dataset = DatasetSpec(
            data_type_def="BaseSimple", dtype="int", doc="Base dataset"
        )

        # Extension with compound dtype
        ext_dtype = [DtypeSpec(name="col1", dtype="int", doc="Column 1")]
        ext_dataset = DatasetSpec(
            data_type_inc="BaseSimple",
            data_type_def="ExtCompound",
            dtype=ext_dtype,
            doc="Extended dataset",
        )

        # This should raise an error
        msg = "Cannot extend simple data type to compound data type"
        with self.assertRaisesWith(ValueError, msg):
            ext_dataset.resolve_inc_spec(base_dataset)

    def test_resolve_inc_spec_shape_extension_error(self):
        """Test error when trying to extend to incompatible shape."""
        # Base with 2D shape
        base_dataset = DatasetSpec(
            data_type_def="Base2D", dtype="int", shape=(None, 3), doc="Base dataset"
        )

        # Extension with 3D shape (more dimensions)
        ext_dataset = DatasetSpec(
            data_type_inc="Base2D",
            data_type_def="Ext3D",
            dtype="int",
            shape=(None, 3, 4),
            doc="Extended dataset",
        )

        # This should raise an error
        msg = "Cannot extend shape (None, 3) to (None, 3, 4)"
        with self.assertRaisesWith(ValueError, msg):
            ext_dataset.resolve_inc_spec(base_dataset)

    # def test_resolve_inc_spec_shape_list_extension(self):
    #     """Test trying to restrict a list of allowed shapes."""
    #     # Base with two allowed shapes
    #     base_dataset = DatasetSpec(
    #         data_type_def="Base2D",
    #         dtype="int",
    #         shape=((None, 3), (None, None, 3)),
    #         doc="Base dataset",
    #     )

    #     # Extension with one of the allowed shapes
    #     ext_dataset = DatasetSpec(
    #         data_type_inc="Base2D",
    #         data_type_def="Ext3D",
    #         dtype="int",
    #         shape=(None, None, 3),
    #         doc="Extended dataset",
    #     )

    #     ext_dataset.resolve_inc_spec(base_dataset)
    #     self.assertEqual(ext_dataset.shape, (None, None, 3))

    # def test_resolve_inc_spec_shape_list_extension_error(self):
    #     """Test error when trying to extend a list of allowed shapes."""
    #     # Base with two allowed shapes
    #     base_dataset = DatasetSpec(
    #         data_type_def="Base2D",
    #         dtype="int",
    #         shape=((None, 3), (None, None, 3)),
    #         doc="Base dataset",
    #     )

    #     # Extension with not one of the allowed shapes
    #     ext_dataset1 = DatasetSpec(
    #         data_type_inc="Base2D",
    #         data_type_def="Ext3D",
    #         dtype="int",
    #         shape=(None,),
    #         doc="Extended dataset",
    #     )

    #     msg = r"Cannot extend shape \(None, 3\), \(None, None, 3\) to \(None,\)"
    #     with self.assertRaisesWith(ValueError, msg):
    #         ext_dataset1.resolve_inc_spec(base_dataset)

    #     # Extension with not one of the allowed shapes
    #     ext_dataset2 = DatasetSpec(
    #         data_type_inc="Base2D",
    #         data_type_def="Ext3D",
    #         dtype="int",
    #         shape=(None, 2),
    #         doc="Extended dataset",
    #     )

    #     msg = r"Cannot extend shape \(None, 3\), \(None, None, 3\) to \(None, 2\)"
    #     with self.assertRaisesWith(ValueError, msg):
    #         ext_dataset2.resolve_inc_spec(base_dataset)

    #     # Extension with not one of the allowed shapes
    #     ext_dataset3 = DatasetSpec(
    #         data_type_inc="Base2D",
    #         data_type_def="Ext3D",
    #         dtype="int",
    #         shape=((None, 4), (None, None, 2)),
    #         doc="Extended dataset",
    #     )

    #     msg = r"Cannot extend shape \(None, 3\), \(None, None, 3\) to \(None, 4\), \(None, None, 2\)"
    #     with self.assertRaisesWith(ValueError, msg):
    #         ext_dataset3.resolve_inc_spec(base_dataset)


class TestSpecExtensionFunctionality(TestCase):
    """Test comprehensive spec extension functionality."""

    def test_group_type_extension(self):
        """Test basic GroupSpec extension functionality."""
        # Create base spec
        attributes = [
            AttributeSpec(name="attribute1", doc="my first attribute", dtype="text"),
            AttributeSpec(name="attribute2", doc="my second attribute", dtype="text"),
        ]
        dset1_attributes = [
            AttributeSpec(name="attribute3", doc="my third attribute", dtype="text"),
            AttributeSpec(name="attribute4", doc="my fourth attribute", dtype="text"),
        ]
        dset2_attributes = [
            AttributeSpec(name="attribute5", doc="my fifth attribute", dtype="text"),
            AttributeSpec(name="attribute6", doc="my sixth attribute", dtype="text"),
        ]
        datasets = [
            DatasetSpec(
                doc="my first dataset",
                dtype="int",
                name="dataset1",
                attributes=dset1_attributes,
                linkable=True,
            ),
            DatasetSpec(
                doc="my second dataset",
                dtype="int",
                name="dataset2",
                attributes=dset2_attributes,
                linkable=True,
                data_type_def="VoltageArray",
            ),
        ]

        spec = GroupSpec(
            doc="A test group",
            name="parent_type",
            datasets=datasets,
            attributes=attributes,
            linkable=False,
            data_type_def="EphysData",
        )

        # Create extension
        dset1_attributes_ext = [
            AttributeSpec(
                name="dset1_extra_attribute",
                doc="an extra attribute for the first dataset",
                dtype="text",
            )
        ]
        ext_datasets = [
            DatasetSpec(
                doc="my first dataset extension",
                dtype="int",
                name="dataset1",
                attributes=dset1_attributes_ext,
                linkable=True,
            ),
        ]
        ext_attributes = [
            AttributeSpec(
                name="ext_extra_attribute", doc="an extra attribute for the group", dtype="text"
            ),
        ]
        ext = GroupSpec(
            doc="A test group extension",
            name="child_type",
            datasets=ext_datasets,
            attributes=ext_attributes,
            linkable=False,
            data_type_inc="EphysData",
            data_type_def="SpikeData",
        )

        # Resolve extension
        ext.resolve_inc_spec(spec)

        # Test dataset extension
        ext_dset1 = ext.get_dataset("dataset1")
        ext_dset1_attrs = ext_dset1.attributes
        self.assertDictEqual(ext_dset1_attrs[0], dset1_attributes_ext[0])
        self.assertDictEqual(ext_dset1_attrs[1], dset1_attributes[0])
        self.assertDictEqual(ext_dset1_attrs[2], dset1_attributes[1])
        self.assertEqual(ext.data_type_def, "SpikeData")
        self.assertEqual(ext.data_type_inc, "EphysData")

        # Test inherited dataset
        ext_dset2 = ext.get_dataset("dataset2")
        self.assertEqual(str(ext_dset2), str(datasets[1]))

        # Test attribute extension
        res_attrs = ext.attributes
        self.assertDictEqual(res_attrs[0], ext_attributes[0])
        self.assertDictEqual(res_attrs[1], attributes[0])
        self.assertDictEqual(res_attrs[2], attributes[1])

        # Test inheritance tracking
        for d in datasets:
            self.assertTrue(ext.is_inherited_spec(d))
            self.assertFalse(spec.is_inherited_spec(d))

    def test_dataset_type_extension(self):
        """Test basic DatasetSpec extension functionality."""
        # Create base spec
        attributes = [
            AttributeSpec(name="attribute1", doc="my first attribute", dtype="text"),
            AttributeSpec(name="attribute2", doc="my second attribute", dtype="text"),
        ]
        base = DatasetSpec(
            doc="my first dataset",
            dtype="int",
            name="dataset1",
            attributes=attributes,
            linkable=False,
            data_type_def="EphysData",
        )

        # Create extension
        ext_attributes = [
            AttributeSpec(name="attribute3", doc="my first extending attribute", dtype="float")
        ]
        ext = DatasetSpec(
            doc="my first dataset extension",
            dtype="int",
            name="dataset1",
            attributes=ext_attributes,
            linkable=False,
            data_type_inc="EphysData",
            data_type_def="SpikeData",
        )

        # Resolve extension
        ext.resolve_inc_spec(base)

        # Test attribute inheritance and extension
        self.assertDictEqual(ext["attributes"][0], ext_attributes[0])
        self.assertDictEqual(ext["attributes"][1], attributes[0])
        self.assertDictEqual(ext["attributes"][2], attributes[1])

        # Test parent relationships
        ext_attrs = ext.attributes
        self.assertIs(ext, ext_attrs[0].parent)
        self.assertIs(ext, ext_attrs[1].parent)
        self.assertIs(ext, ext_attrs[2].parent)

    def test_dataset_extension_wrong_type_error(self):
        """Test error when resolving DatasetSpec with GroupSpec."""
        base = GroupSpec(doc="a fake group", data_type_def="EphysData")
        ext = DatasetSpec(
            doc="my first dataset extension",
            dtype="int",
            name="dataset1",
            data_type_inc="EphysData",
            data_type_def="SpikeData",
        )
        with self.assertRaises(TypeError):
            ext.resolve_inc_spec(base)

    def test_dataset_table_extension(self):
        """Test DatasetSpec table extension functionality."""
        # Create base table spec
        attributes = [
            AttributeSpec(name="attribute1", doc="my first attribute", dtype="text"),
            AttributeSpec(name="attribute2", doc="my second attribute", dtype="text"),
        ]
        dtype1 = DtypeSpec(name="column1", doc="the first column", dtype="int")
        dtype2 = DtypeSpec(name="column2", doc="the second column", dtype="float")
        base = DatasetSpec(
            doc="my first table",
            dtype=[dtype1, dtype2],
            attributes=attributes,
            data_type_def="SimpleTable",
        )

        # Create extension
        dtype3 = DtypeSpec(name="column3", doc="the third column", dtype="text")
        ext = DatasetSpec(
            doc="my first table extension",
            dtype=[dtype3],
            data_type_inc="SimpleTable",
            data_type_def="ExtendedTable",
        )

        # Resolve extension
        ext.resolve_inc_spec(base)

        # Test dtype extension
        self.assertEqual(ext["dtype"], [dtype1, dtype2, dtype3])
        self.assertEqual(ext["doc"], "my first table extension")

    def test_dataset_table_extension_higher_precision(self):
        """Test DatasetSpec table extension with higher precision."""
        # Create base table spec
        attributes = [
            AttributeSpec(name="attribute1", doc="my first attribute", dtype="text"),
            AttributeSpec(name="attribute2", doc="my second attribute", dtype="text"),
        ]
        dtype1 = DtypeSpec(name="column1", doc="the first column", dtype="int")
        dtype2 = DtypeSpec(name="column2", doc="the second column", dtype="float32")
        base = DatasetSpec(
            doc="my first table",
            dtype=[dtype1, dtype2],
            attributes=attributes,
            data_type_def="SimpleTable",
        )

        # Create extension with higher precision
        dtype3 = DtypeSpec(
            name="column2", doc="the second column, with greater precision", dtype="float64"
        )
        ext = DatasetSpec(
            doc="my first table extension",
            dtype=[dtype3],
            data_type_inc="SimpleTable",
            data_type_def="ExtendedTable",
        )

        # Resolve extension
        ext.resolve_inc_spec(base)

        # Test dtype override with higher precision
        self.assertEqual(ext["dtype"], [dtype1, dtype3])
        self.assertEqual(ext["doc"], "my first table extension")

    def test_dataset_table_extension_lower_precision_error(self):
        """Test error when extending to lower precision."""
        # Create base table spec
        attributes = [
            AttributeSpec(name="attribute1", doc="my first attribute", dtype="text"),
            AttributeSpec(name="attribute2", doc="my second attribute", dtype="text"),
        ]
        dtype1 = DtypeSpec(name="column1", doc="the first column", dtype="int")
        dtype2 = DtypeSpec(name="column2", doc="the second column", dtype="float64")
        base = DatasetSpec(
            doc="my first table",
            dtype=[dtype1, dtype2],
            attributes=attributes,
            data_type_def="SimpleTable",
        )

        # Create extension with lower precision (should fail)
        dtype3 = DtypeSpec(
            name="column2", doc="the second column, with lower precision", dtype="float32"
        )
        ext = DatasetSpec(
            doc="my first table extension",
            dtype=[dtype3],
            data_type_inc="SimpleTable",
            data_type_def="ExtendedTable",
        )

        # Should raise error
        with self.assertRaisesWith(ValueError, "Cannot extend float64 to float32"):
            ext.resolve_inc_spec(base)

    def test_dataset_table_extension_incompatible_format_error(self):
        """Test error when extending to incompatible format."""
        # Create base table spec
        attributes = [
            AttributeSpec(name="attribute1", doc="my first attribute", dtype="text"),
            AttributeSpec(name="attribute2", doc="my second attribute", dtype="text"),
        ]
        dtype1 = DtypeSpec(name="column1", doc="the first column", dtype="int")
        dtype2 = DtypeSpec(name="column2", doc="the second column", dtype="float64")
        base = DatasetSpec(
            doc="my first table",
            dtype=[dtype1, dtype2],
            attributes=attributes,
            data_type_def="SimpleTable",
        )

        # Create extension with incompatible format (should fail)
        dtype3 = DtypeSpec(
            name="column2", doc="the second column, with different format", dtype="int32"
        )
        ext = DatasetSpec(
            doc="my first table extension",
            dtype=[dtype3],
            data_type_inc="SimpleTable",
            data_type_def="ExtendedTable",
        )

        # Should raise error
        with self.assertRaisesWith(ValueError, "Cannot extend float64 to int32"):
            ext.resolve_inc_spec(base)

    def test_group_attribute_resolution(self):
        """Test GroupSpec attribute resolution."""
        # Create base spec
        def_group_spec = GroupSpec(
            doc="A test group",
            name="root",
            data_type_def="MyGroup",
            attributes=[
                AttributeSpec(name="attribute1", doc="my first attribute", dtype="text"),
                AttributeSpec(name="attribute2", doc="my second attribute", dtype="text"),
            ],
        )

        # Create extension spec
        inc_group_spec = GroupSpec(
            doc="A test group",
            name="root",
            data_type_inc="MyGroup",
            attributes=[
                AttributeSpec(
                    name="attribute2", doc="my second attribute", dtype="text", value="fixed"
                ),
                AttributeSpec(
                    name="attribute3", doc="my third attribute", dtype="text", value="fixed"
                ),
            ],
        )

        # Resolve extension
        inc_group_spec.resolve_inc_spec(def_group_spec)

        # Test resolved attributes
        self.assertTupleEqual(
            inc_group_spec.attributes,
            (
                AttributeSpec(
                    name="attribute2", doc="my second attribute", dtype="text", value="fixed"
                ),
                AttributeSpec(
                    name="attribute3", doc="my third attribute", dtype="text", value="fixed"
                ),
                AttributeSpec(name="attribute1", doc="my first attribute", dtype="text"),
            ),
        )

        # Test attribute access
        self.assertEqual(
            inc_group_spec.get_attribute("attribute1"),
            AttributeSpec(name="attribute1", doc="my first attribute", dtype="text"),
        )
        self.assertEqual(
            inc_group_spec.get_attribute("attribute2"),
            AttributeSpec(name="attribute2", doc="my second attribute", dtype="text", value="fixed"),
        )
        self.assertEqual(
            inc_group_spec.get_attribute("attribute3"),
            AttributeSpec(name="attribute3", doc="my third attribute", dtype="text", value="fixed"),
        )

        # Test inheritance tracking
        attr_spec_map = {attr.name: attr for attr in inc_group_spec.attributes}
        self.assertTrue(inc_group_spec.is_inherited_spec(attr_spec_map["attribute1"]))
        self.assertTrue(inc_group_spec.is_inherited_spec(attr_spec_map["attribute2"]))
        self.assertFalse(inc_group_spec.is_inherited_spec(attr_spec_map["attribute3"]))

        # Test override tracking
        self.assertFalse(inc_group_spec.is_overridden_spec(attr_spec_map["attribute1"]))
        self.assertTrue(inc_group_spec.is_overridden_spec(attr_spec_map["attribute2"]))
        self.assertFalse(inc_group_spec.is_overridden_spec(attr_spec_map["attribute3"]))

        # Test convenience methods
        self.assertTrue(inc_group_spec.is_inherited_attribute("attribute1"))
        self.assertTrue(inc_group_spec.is_inherited_attribute("attribute2"))
        self.assertFalse(inc_group_spec.is_inherited_attribute("attribute3"))

        self.assertFalse(inc_group_spec.is_overridden_attribute("attribute1"))
        self.assertTrue(inc_group_spec.is_overridden_attribute("attribute2"))
        self.assertFalse(inc_group_spec.is_overridden_attribute("attribute3"))
