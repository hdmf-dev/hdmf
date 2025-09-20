"""
Tests for the spec resolution system.

This module tests the resolution functionality that allows specs to be resolved after loading, including
cross-namespace resolution capabilities.
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
    RefSpec,
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
        self.base_attr = AttributeSpec(name="base_attr", dtype="text", doc="Base attribute")
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

    def test_resolved_property_setter(self):
        """Test that the resolved property can be set."""
        ext_attr = AttributeSpec(name="ext_attr", dtype="text", doc="Extension attribute")
        ext_dataset = DatasetSpec(
            data_type_inc="BaseDataset",
            data_type_def="ExtDataset",
            name="ext_dataset",
            dtype="int",
            doc="Extended dataset",
            attributes=[ext_attr],
        )

        self.assertFalse(ext_dataset.resolved)

        # Set resolved to True
        ext_dataset.resolved = True
        self.assertTrue(ext_dataset.resolved)

        # Set resolved to False
        ext_dataset.resolved = False
        self.assertFalse(ext_dataset.resolved)

        # Invalid value should raise error
        with self.assertRaises(ValueError):
            ext_dataset.resolved = "not a boolean"

    def test_resolve_inc_spec_dataset_add_attribute(self):
        """Test extending a dataset and adding an attribute."""
        # Create an extension dataset that adds an attribute
        # This should be the same as an extension group that adds a dataset because this is managed by
        # BaseStorageSpec.resolve_inc_spec
        ext_attr = AttributeSpec(name="ext_attr", dtype="text", doc="Extension attribute")
        ext_dataset = DatasetSpec(
            data_type_inc="BaseDataset",
            data_type_def="ExtDataset",
            name="ext_dataset",
            dtype="int",
            doc="Extended dataset",
            attributes=[ext_attr],
        )

        # Initially, the extension should not be resolved
        # Note: resolved property is managed by the overarching NamespaceCatalog on subspecs being resolved
        self.assertFalse(ext_dataset.inc_spec_resolved)

        # Resolve the extension
        ext_dataset.resolve_inc_spec(self.base_dataset)

        # Check that resolution flags are set
        self.assertTrue(ext_dataset.inc_spec_resolved)

        # Check that attributes are inherited (ext_attr should be present, base_attr should be inherited)
        ext_attrs = {attr.name: attr for attr in ext_dataset.attributes}
        self.assertIn("base_attr", ext_attrs)
        self.assertIn("ext_attr", ext_attrs)

        # Check inheritance tracking
        self.assertTrue(ext_dataset.is_inherited_attribute("base_attr"))
        self.assertFalse(ext_dataset.is_inherited_attribute("ext_attr"))

    def test_resolve_inc_spec_group_add_dataset(self):
        """Test extending a group and adding a dataset."""
        # Create an extension group that adds a dataset
        # NOTE: technically ExtDataset does not need to be defined for this test, but included for completeness
        _ = DatasetSpec(
            data_type_inc="BaseDataset",
            data_type_def="ExtDataset",
            name="ext_dataset",
            dtype="int",
            doc="Extended dataset",
        )
        ext_group = GroupSpec(
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

        # Initially, the extension should not be resolved
        self.assertFalse(ext_group.resolved)
        self.assertFalse(ext_group.inc_spec_resolved)

        # Resolve the extension
        ext_group.resolve_inc_spec(self.base_group)

        # Check that resolution flags are set
        self.assertTrue(ext_group.inc_spec_resolved)
        # Note: GroupSpec resolution depends on subspecs being resolved

        # Check that datasets are inherited
        ext_datasets = [dset.name for dset in ext_group.datasets]
        ext_datasets_expected = ["ext_dataset", "base_dataset"]
        self.assertEqual(ext_datasets, ext_datasets_expected)

        # Check inheritance tracking
        self.assertTrue(ext_group.is_inherited_dataset("base_dataset"))
        self.assertFalse(ext_group.is_inherited_dataset("ext_dataset"))

    def test_resolve_inc_spec_attribute_simple_override(self):
        """Test that attribute overrides work correctly."""
        # Create an extension that overrides an attribute with compatible type
        # NOTE: tests of _resolve_inc_spec_dtype, _resolve_inc_spec_shape, _resolve_inc_spec_dims, etc.
        # are done for dataset specs below, so they are not repeated here for attributes
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

    def test_resolve_inc_spec_cpd_dtype_extension_new_col(self):
        """Test that adding a column to a compound dtype in an extension works correctly."""
        # Create a base dataset with compound dtype
        base_dtype = [
            DtypeSpec(name="col1", dtype="int", doc="First column"),
            DtypeSpec(name="col2", dtype="float", doc="Second column"),
        ]
        base_dataset = DatasetSpec(data_type_def="BaseCompound", dtype=base_dtype, doc="Base compound dataset")

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

    def test_resolve_inc_spec_cpd_dtype_override_higher_precision(self):
        """Test that overriding to higher precision dtypes in compound dtypes works correctly."""
        base_dtype = [
            DtypeSpec(name="col1", dtype="int32", doc="First column"),
            DtypeSpec(name="col2", dtype="float32", doc="Second column"),
        ]
        base_dataset = DatasetSpec(data_type_def="BaseCompound", dtype=base_dtype, doc="Base compound dataset")

        # Create an extension that overrides col2 with higher precision
        ext_dtype = [DtypeSpec(name="col2", dtype="float64", doc="Second column with higher precision")]
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

    def test_resolve_inc_spec_cpd_dtype_override_lower_precision_error(self):
        """Test that overriding to lower precision dtypes in compound dtypes raises an error."""
        base_dtype = [DtypeSpec(name="col1", dtype="float64", doc="First column")]
        base_dataset = DatasetSpec(data_type_def="BaseCompound", dtype=base_dtype, doc="Base compound dataset")

        # Create an extension that tries to override col1 with lower precision
        ext_dtype = [DtypeSpec(name="col1", dtype="float32", doc="First column with lower precision")]
        ext_dataset = DatasetSpec(
            data_type_inc="BaseCompound",
            data_type_def="ExtCompound",
            dtype=ext_dtype,
            doc="Extended compound dataset",
        )

        # This should raise an error
        msg = "Cannot extend float64 to float32"
        with self.assertRaisesWith(ValueError, msg):
            ext_dataset.resolve_inc_spec(base_dataset)

    def test_resolve_inc_spec_cpd_dtype_override_incompatible_dtype(self):
        """Test that overriding to incompatible dtypes in compound dtypes raises an error."""
        base_dtype = [DtypeSpec(name="col1", dtype="float64", doc="First column")]
        base_dataset = DatasetSpec(data_type_def="BaseCompound", dtype=base_dtype, doc="Base compound dataset")

        # Create an extension that tries to override col1 with incompatible dtype
        ext_dtype = [DtypeSpec(name="col1", dtype="text", doc="First column with incompatible dtype")]
        ext_dataset = DatasetSpec(
            data_type_inc="BaseCompound",
            data_type_def="ExtCompound",
            dtype=ext_dtype,
            doc="Extended compound dataset",
        )

        # This should raise an error
        msg = "Cannot extend float64 to text"
        with self.assertRaisesWith(ValueError, msg):
            ext_dataset.resolve_inc_spec(base_dataset)

    def test_resolve_inc_spec_compound_to_simple_dtype_error(self):
        """Test error when trying to extend compound dtype to simple dtype."""
        # Base with compound dtype
        base_dtype = [DtypeSpec(name="col1", dtype="int", doc="Column 1")]
        base_dataset = DatasetSpec(data_type_def="BaseCompound", dtype=base_dtype, doc="Base dataset")

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
        base_dataset = DatasetSpec(data_type_def="BaseSimple", dtype="int", doc="Base dataset")

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

    def test_resolve_inc_spec_ref_dtype_same(self):
        """Test that ref dtypes are resolved correctly."""
        base_dataset = DatasetSpec(
            data_type_def="BaseWithRef",
            dtype=RefSpec(target_type="OtherType", reftype="object"),
            doc="Base dataset with ref dtype",
        )

        ext_dataset = DatasetSpec(
            data_type_inc="BaseWithRef",
            data_type_def="ExtWithRef",
            dtype=RefSpec(target_type="OtherType", reftype="object"),
            doc="Extended dataset with same ref dtype",
        )

        # Resolve the extension
        ext_dataset.resolve_inc_spec(base_dataset)

        # Check that dtype is inherited
        self.assertEqual(ext_dataset.dtype, RefSpec(target_type="OtherType", reftype="object"))

    # TODO: also test when OtherType extends AType
    # def test_resolve_inc_spec_ref_dtype_error(self):
    #     """Test that resolving ref dtypes mismatches raises an error."""
    #     base_dataset = DatasetSpec(
    #         data_type_def="BaseWithRef",
    #         dtype=RefSpec(target_type="AType", reftype="object"),
    #         doc="Base dataset with ref dtype",
    #     )

    #     ext_dataset = DatasetSpec(
    #         data_type_inc="BaseWithRef",
    #         data_type_def="ExtWithRef",
    #         dtype=RefSpec(target_type="OtherType", reftype="object"),
    #         doc="Extended dataset with same ref dtype",
    #     )

    #     # Resolve the extension
    #     msg = ("Cannot extend {'target_type': 'AType', 'reftype': 'object'} to "
    #            "{'target_type': 'OtherType', 'reftype': 'object'}")
    #     with self.assertRaisesWith(ValueError, msg):
    #         ext_dataset.resolve_inc_spec(base_dataset)

    def test_resolve_inc_spec_ref_dtype_to_simple(self):
        """Test that resolving ref dtypes mismatches raises an error."""
        base_dataset = DatasetSpec(
            data_type_def="BaseWithRef",
            dtype=RefSpec(target_type="AType", reftype="object"),
            doc="Base dataset with ref dtype",
        )

        ext_dataset = DatasetSpec(
            data_type_inc="BaseWithRef",
            data_type_def="ExtWithRef",
            dtype="int",
            doc="Extended dataset with int dtype",
        )

        # Resolve the extension
        msg = "Cannot extend {'target_type': 'AType', 'reftype': 'object'} to int"
        with self.assertRaisesWith(ValueError, msg):
            ext_dataset.resolve_inc_spec(base_dataset)

    def test_resolve_inc_spec_simple_to_ref_dtype(self):
        """Test that resolving ref dtypes mismatches raises an error."""
        base_dataset = DatasetSpec(
            data_type_def="BaseWithRef",
            dtype="int",
            doc="Base dataset",
        )

        ext_dataset = DatasetSpec(
            data_type_inc="BaseWithRef",
            data_type_def="ExtWithRef",
            dtype=RefSpec(target_type="AType", reftype="object"),
            doc="Extended dataset with a ref dtype",
        )

        # Resolve the extension
        msg = "Cannot extend int to {'target_type': 'AType', 'reftype': 'object'}"
        with self.assertRaisesWith(ValueError, msg):
            ext_dataset.resolve_inc_spec(base_dataset)


    def test_resolve_inc_spec_override_higher_precision(self):
        """Test that overriding to higher precision dtypes works correctly."""
        base_dataset = DatasetSpec(data_type_def="BaseCompound", dtype="int32", doc="Base dataset")

        # Create an extension that overrides BaseCompound with higher precision
        ext_dataset = DatasetSpec(
            data_type_inc="BaseCompound",
            data_type_def="ExtCompound",
            dtype="int64",
            doc="Extended dataset",
        )

        # Resolve the extension
        ext_dataset.resolve_inc_spec(base_dataset)

        # Check that the dtype was overridden with higher precision
        self.assertEqual(ext_dataset.dtype, "int64")

    def test_resolve_inc_spec_override_lower_precision_error(self):
        """Test that overriding to lower precision dtypes raises an error."""
        base_dataset = DatasetSpec(data_type_def="BaseCompound", dtype="int64", doc="Base dataset")

        # Create an extension that overrides BaseCompound with lower precision
        ext_dataset = DatasetSpec(
            data_type_inc="BaseCompound",
            data_type_def="ExtCompound",
            dtype="int32",
            doc="Extended dataset",
        )

        # This should raise an error
        msg = "Cannot extend int64 to int32"
        with self.assertRaisesWith(ValueError, msg):
            ext_dataset.resolve_inc_spec(base_dataset)

    def test_resolve_inc_spec_override_incompatible_dtype(self):
        """Test that overriding to an incompatible dtype raises an error."""
        base_dataset = DatasetSpec(data_type_def="BaseCompound", dtype="int64", doc="Base dataset")

        # Create an extension that overrides BaseCompound with incompatible dtype
        ext_dataset = DatasetSpec(
            data_type_inc="BaseCompound",
            data_type_def="ExtCompound",
            dtype="text",
            doc="Extended dataset",
        )

        # This should raise an error
        msg = "Cannot extend int64 to text"
        with self.assertRaisesWith(ValueError, msg):
            ext_dataset.resolve_inc_spec(base_dataset)



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

    def test_resolve_inc_spec_shape_extension_error(self):
        """Test error when trying to extend to incompatible shape."""
        # Base with 2D shape
        base_dataset = DatasetSpec(data_type_def="Base2D", dtype="int", shape=(None, 3), doc="Base dataset")

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

    def test_resolve_inc_spec_wrong_spec_type(self):
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

        base_dataset = DatasetSpec(data_type_def="BaseDataset", doc="Base dataset", dtype="int")
        ext_group = GroupSpec(
            data_type_inc="BaseDataset",
            data_type_def="ExtGroup",
            doc="Extended dataset",
        )

        # This should raise an error from docval type checking
        with self.assertRaises(TypeError):
            ext_group.resolve_inc_spec(base_dataset)


class TestNamespaceCatalogResolution(TestCase):
    """Test the NamespaceCatalog resolution functionality."""

    def setUp(self):
        """Set up test namespaces and specs."""
        self.tempdir = Path(tempfile.mkdtemp())
        self.ns_catalog = NamespaceCatalog()

    def tearDown(self):
        """Clean up temporary files."""
        shutil.rmtree(self.tempdir)

    def create_test_namespace(self, name: str, specs: list[BaseStorageSpec], dependencies: list[str] = None) -> str:
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
        ext_spec = GroupSpec(data_type_inc="BaseType", data_type_def="ExtType", doc="Extended group")

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
        ext_spec = GroupSpec(data_type_inc="BaseType", data_type_def="ExtType", doc="Extended group")
        ext_ns_path = self.create_test_namespace("ext", [ext_spec], dependencies=["base"])

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
        mid_spec = GroupSpec(data_type_inc="BaseType", data_type_def="MidType", doc="Mid group")
        ext_spec = GroupSpec(data_type_inc="MidType", data_type_def="ExtType", doc="Extended group")

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
        base_dataset = DatasetSpec(data_type_def="BaseDataset", dtype="int", doc="Base dataset")

        # Create group with dataset subspec
        group_with_dataset = GroupSpec(
            data_type_def="GroupWithDataset",
            doc="Group with dataset",
            datasets=[DatasetSpec(data_type_inc="BaseDataset", name="sub_dataset", doc="Sub dataset")],
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

    def test_resolve_all_specs_invalid_inc_spec_error(self):
        """Test error when a spec extends a non-existent type."""
        # Create a spec that extends a non-existent type
        invalid_spec = GroupSpec(data_type_inc="NonExistentType", data_type_def="InvalidType", doc="Invalid group")

        ns_path = self.create_test_namespace("test", [invalid_spec])

        # Load namespace without resolution
        self.ns_catalog.load_namespaces(ns_path, resolve=False)

        # Attempting to resolve should raise an error
        msg = "Namespace for data_type 'NonExistentType' not found"
        with self.assertRaisesWith(ValueError, msg):
            self.ns_catalog.resolve_all_specs()

    def test_resolve_all_specs_complex(self):
        # DatasetSpec D1 has 1D, 2D, or 3D shape, any dtype, no attributes
        # GroupSpec A1 contains a DatasetSpec D1 dataset named "col"
        # GroupSpec A2 extends A1
        # A2 specifies that the dataset "col" should have 1D or 2D shape, dtype int32, and an extra attribute attr1
        # Check that after resolution, A2/col has attributes [attr1]
        # GroupSpec A3 extends A2
        # A3 specifies that the dataset "col" should have 1D shape, dtype int64, and an extra attribute attr2
        # Check that after resolution, A3/col has attributes [attr2, attr1]
        # DatasetSpec D2 extends D1 that specifies shape 2D or 3D shape, dtype float64, and an extra attribute attr3
        # GroupSpec A4 extends A1
        # A4 specifies that the dataset "col" should be of type D2 and have 2D shape and an extra attribute attr4
        # Check that after resolution, A4/col has attributes [attr4, attr3] and dtype float64
        d1 = DatasetSpec(
            data_type_def="D1",
            name="col",
            dtype=None,
            shape=((None,), (None, None), (None, None, None)),
            doc="Dataset D1",
        )
        a1 = GroupSpec(
            data_type_def="A1",
            name="A1",
            datasets=[DatasetSpec(name="col", data_type_inc="D1", doc="D1 col in A1")],
            doc="Group A1",
        )
        a2 = GroupSpec(
            data_type_def="A2",
            data_type_inc="A1",
            name="A2",
            datasets=[
                DatasetSpec(
                    name="col",
                    data_type_inc="D1",  # TODO test whether this is necessary
                    shape=((None,), (None, None)),
                    dtype="int32",
                    attributes=[AttributeSpec(name="attr1", dtype="int", doc="Attribute 1")],
                    doc="Extended D1 col in A2 with restrictions",
                )
            ],
            doc="Group A2",
        )
        a3 = GroupSpec(
            data_type_def="A3",
            data_type_inc="A2",
            name="A3",
            datasets=[
                DatasetSpec(
                    name="col",
                    data_type_inc="D1",  # TODO test whether this is necessary
                    shape=(None,),
                    dtype="int64",
                    attributes=[AttributeSpec(name="attr2", dtype="text", doc="Attribute 2")],
                    doc="Extended D1 col in A3 with further restrictions",
                )
            ],
            doc="Group A3",
        )
        d2 = DatasetSpec(
            data_type_def="D2",
            data_type_inc="D1",
            name="D2",
            shape=((None, None), (None, None, None)),
            dtype="float64",
            attributes=[AttributeSpec(name="attr3", dtype="float", doc="Attribute 3")],
            doc="Dataset D2 extending D1 with restrictions",
        )
        a4 = GroupSpec(
            data_type_def="A4",
            data_type_inc="A1",
            name="A4",
            datasets=[
                DatasetSpec(
                    name="col",
                    data_type_inc="D2",
                    shape=(None, None),
                    attributes=[AttributeSpec(name="attr4", dtype="float", doc="Attribute 4")],
                    doc="D2 col in A4 with restrictions",
                )
            ],
            doc="Group A4",
        )
        ns_path = self.create_test_namespace("test", [d1, d2, a1, a2, a3, a4])
        self.ns_catalog.load_namespaces(ns_path)
        self.ns_catalog.resolve_all_specs()  # check no errors

        a2_loaded = self.ns_catalog.get_spec_for_type("A2")
        self.assertEqual(
            a2_loaded.datasets[0].attributes,
            (AttributeSpec(name="attr1", dtype="int", doc="Attribute 1"),)
        )

        a3_loaded = self.ns_catalog.get_spec_for_type("A3")
        self.assertEqual(
            a3_loaded.datasets[0].attributes,
            (
                AttributeSpec(name="attr2", dtype="text", doc="Attribute 2"),
                AttributeSpec(name="attr1", dtype="int", doc="Attribute 1"),
            )
        )

        a4_loaded = self.ns_catalog.get_spec_for_type("A4")
        self.assertTrue(a4_loaded.datasets[0].resolved)
        self.assertEqual(
            a4_loaded.datasets[0].attributes,
            (
                AttributeSpec(name="attr4", dtype="float", doc="Attribute 4"),
                # AttributeSpec(name="attr3", dtype="float", doc="Attribute 3"),  # TODO this should exist
            )
        )
        # self.assertEqual(a4_loaded.datasets[0].dtype, "float64")  # TODO this should work

    def test_resolve_all_specs_complex_error(self):
        # DatasetSpec D1 has 1D, 2D, or 3D shape, any dtype, no attributes
        # GroupSpec A1 contains a DatasetSpec D1 dataset named "col"
        # GroupSpec A2 extends A1
        # A2 specifies that the dataset "col" should have 1D or 2D shape, dtype int32, and an extra attribute attr1
        # GroupSpec A5 extends A2
        # A5 specifies that the dataset "col" should be of type D2. This will rarely happen. A2/col should be
        # brought in first when resolving A5's inc spec. Then the refinement of "col" in A5 to say that it should be
        # of type D2 should cause an error when it is found that D2's dtype is incompatible with A2/col's dtype.
        d1 = DatasetSpec(
            data_type_def="D1",
            name="col",
            dtype=None,
            shape=((None,), (None, None), (None, None, None)),
            doc="Dataset D1",
        )
        a1 = GroupSpec(
            data_type_def="A1",
            name="A1",
            datasets=[DatasetSpec(name="col", data_type_inc="D1", doc="D1 col in A1")],
            doc="Group A1",
        )
        a2 = GroupSpec(
            data_type_def="A2",
            data_type_inc="A1",
            name="A2",
            datasets=[
                DatasetSpec(
                    name="col",
                    data_type_inc="D1",  # TODO test whether this is necessary
                    shape=((None,), (None, None)),
                    dtype="int32",
                    attributes=[AttributeSpec(name="attr1", dtype="int", doc="Attribute 1")],
                    doc="Extended D1 col in A2 with restrictions",
                )
            ],
            doc="Group A2",
        )
        d2 = DatasetSpec(
            data_type_def="D2",
            data_type_inc="D1",
            name="D2",
            shape=((None, None), (None, None, None)),
            dtype="float64",
            attributes=[AttributeSpec(name="attr3", dtype="float", doc="Attribute 3")],
            doc="Dataset D2 extending D1 with restrictions",
        )
        a5 = GroupSpec(
            data_type_def="A5",
            data_type_inc="A2",
            name="A5",
            datasets=[
                DatasetSpec(
                    name="col",
                    data_type_inc="D2",
                    doc="D2 col in A5",
                )
            ],
            doc="Group A5",
        )
        ns_path = self.create_test_namespace("test", [d1, d2, a1, a2, a5])
        self.ns_catalog.load_namespaces(ns_path)
        # msg = ("Could not resolve all specifications. The following specifications could not be resolved: "
        #        "A5, col in A5")
        # TODO this should raise an error
        # with self.assertRaisesWith(RuntimeError, msg):
        #     self.ns_catalog.resolve_all_specs()
