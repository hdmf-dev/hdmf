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
    LinkSpec,
    RefSpec,
    SpecNamespace,
    NamespaceCatalog,
)
from hdmf.spec.spec import BaseStorageSpec
from hdmf.testing import TestCase


class TestSpecResolution(TestCase):
    """Test the spec resolution system."""

    # NOTE: tests of _resolve_inc_spec_dtype, _resolve_inc_spec_shape, _resolve_inc_spec_dims, etc.
    # are done for dataset specs below, so they are not repeated here for attributes
    # NOTE: tests of resolving attributes are done for group specs below and not repeated for datasets
    # because resolution of attributes is managed by BaseStorageSpec.resolve_inc_spec

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

    def test_resolve_inc_spec_dtype_inheritance(self):
        """Test that dtype is inherited correctly."""
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
            doc="Extended dataset",
        )

        # Resolve the extension
        ext_dataset.resolve_inc_spec(base_dataset)

        # Check that dtype is inherited
        self.assertEqual(ext_dataset.dtype, "int")

        # without data_type_def
        ext_dataset2 = DatasetSpec(
            data_type_inc="BaseWithShape",
            doc="Extended dataset",
        )

        # Resolve the extension
        ext_dataset2.resolve_inc_spec(base_dataset)

        # Check that dtype is inherited
        self.assertEqual(ext_dataset2.dtype, "int")

    def test_resolve_inc_spec_attribute_simple_override(self):
        """Test that attribute overrides work correctly."""
        base_group = GroupSpec(
            doc="A test group",
            data_type_def="MyGroup",
            attributes=[
                AttributeSpec(name="attribute1", doc="my first attribute", dtype="text"),
                AttributeSpec(name="attribute2", doc="my second attribute", dtype="text"),
            ],
        )
        ext_group = GroupSpec(
            doc="A test group",
            name="root",
            data_type_inc="MyGroup",
            attributes=[
                AttributeSpec(name="attribute2", doc="my second attribute", dtype="text", value="fixed"),
                AttributeSpec(name="attribute3", doc="my third attribute", dtype="text", value="fixed"),
            ],
        )
        ext_group.resolve_inc_spec(base_group)

        self.assertTupleEqual(
            ext_group.attributes,
            (
                AttributeSpec(name="attribute2", doc="my second attribute", dtype="text", value="fixed"),
                AttributeSpec(name="attribute3", doc="my third attribute", dtype="text", value="fixed"),
                AttributeSpec(name="attribute1", doc="my first attribute", dtype="text"),
            ),
        )

        self.assertEqual(
            ext_group.get_attribute("attribute1"),
            AttributeSpec(name="attribute1", doc="my first attribute", dtype="text"),
        )
        self.assertEqual(
            ext_group.get_attribute("attribute2"),
            AttributeSpec(name="attribute2", doc="my second attribute", dtype="text", value="fixed"),
        )
        self.assertEqual(
            ext_group.get_attribute("attribute3"),
            AttributeSpec(name="attribute3", doc="my third attribute", dtype="text", value="fixed"),
        )

        # Check is_inherited_spec
        self.assertFalse(base_group.is_inherited_spec(base_group.attributes[0]))
        self.assertFalse(base_group.is_inherited_spec(base_group.attributes[1]))

        attr_spec_map = {attr.name: attr for attr in ext_group.attributes}
        self.assertTrue(ext_group.is_inherited_spec(attr_spec_map["attribute1"]))
        self.assertTrue(ext_group.is_inherited_spec(attr_spec_map["attribute2"]))
        self.assertFalse(ext_group.is_inherited_spec(attr_spec_map["attribute3"]))

        # Check is_overridden_spec
        self.assertFalse(base_group.is_overridden_spec(base_group.attributes[0]))
        self.assertFalse(base_group.is_overridden_spec(base_group.attributes[1]))

        attr_spec_map = {attr.name: attr for attr in ext_group.attributes}
        self.assertFalse(ext_group.is_overridden_spec(attr_spec_map["attribute1"]))
        self.assertTrue(ext_group.is_overridden_spec(attr_spec_map["attribute2"]))
        self.assertFalse(ext_group.is_overridden_spec(attr_spec_map["attribute3"]))

        # Check is_inherited_attribute
        self.assertFalse(base_group.is_inherited_attribute("attribute1"))
        self.assertFalse(base_group.is_inherited_attribute("attribute2"))
        self.assertTrue(ext_group.is_inherited_attribute("attribute1"))
        self.assertTrue(ext_group.is_inherited_attribute("attribute2"))
        self.assertFalse(ext_group.is_inherited_attribute("attribute3"))
        with self.assertRaisesWith(ValueError, "Attribute 'attribute4' not found"):
            ext_group.is_inherited_attribute("attribute4")

        # Check is_overridden_attribute
        self.assertFalse(base_group.is_overridden_attribute("attribute1"))
        self.assertFalse(base_group.is_overridden_attribute("attribute2"))
        self.assertFalse(ext_group.is_overridden_attribute("attribute1"))
        self.assertTrue(ext_group.is_overridden_attribute("attribute2"))
        self.assertFalse(ext_group.is_overridden_attribute("attribute3"))
        with self.assertRaisesWith(ValueError, "Attribute 'attribute4' not found"):
            ext_group.is_overridden_attribute("attribute4")

    def test_resolve_inc_spec_is_overridden_spec_nested(self):
        """Test that is_overridden_spec correctly identifies overridden specs in nested structures."""
        # Create base spec with a dataset containing an attribute
        base_dataset = DatasetSpec(
            doc="Base dataset",
            dtype="int",
            name="test_dataset",
            attributes=[AttributeSpec(name="attr1", doc="Base attr", dtype="text")],
        )
        base_group = GroupSpec(
            doc="Base group", name="test_group", attributes=[AttributeSpec(name="attr1", doc="Base attr", dtype="text")]
        )
        base_spec = GroupSpec(
            doc="A base group", data_type_def="BaseType", datasets=[base_dataset], groups=[base_group]
        )
        # Create extending spec that overrides both dataset and group with new attribute values
        override_dataset = DatasetSpec(
            doc="Override dataset",
            dtype="int",
            name="test_dataset",
            attributes=[AttributeSpec(name="attr1", doc="Override attr", dtype="text")],
        )
        override_group = GroupSpec(
            doc="Override group",
            name="test_group",
            attributes=[AttributeSpec(name="attr1", doc="Override attr", dtype="text")],
        )
        ext_spec = GroupSpec(
            "An extending group",
            data_type_inc="BaseType",
            data_type_def="ExtType",
            datasets=[override_dataset],
            groups=[override_group],
        )

        # Resolve the extension
        ext_spec.resolve_inc_spec(base_spec)

        # Test attribute in overridden dataset is marked as overridden
        dataset_attr = ext_spec.get_dataset("test_dataset").get_attribute("attr1")
        self.assertTrue(ext_spec.is_overridden_spec(dataset_attr))

        # Test attribute in overridden group is marked as overridden
        group_attr = ext_spec.get_group("test_group").get_attribute("attr1")
        self.assertTrue(ext_spec.is_overridden_spec(group_attr))

        # Test attributes in base spec are not marked as overridden
        base_dataset_attr = base_spec.get_dataset("test_dataset").get_attribute("attr1")
        base_group_attr = base_spec.get_group("test_group").get_attribute("attr1")
        self.assertFalse(base_spec.is_overridden_spec(base_dataset_attr))
        self.assertFalse(base_spec.is_overridden_spec(base_group_attr))

    def test_resolve_inc_spec_group_spec_is_overridden_group(self):
        """Test that is_overridden_group correctly identifies overridden groups."""
        # Create base spec with a group
        base_group = GroupSpec(doc="Base group", name="test_group")
        base_spec = GroupSpec(doc="A base group", data_type_def="BaseType", groups=[base_group])

        # Create extending spec that overrides the group
        override_group = GroupSpec(doc="Override group", name="test_group")
        ext_spec = GroupSpec(
            doc="An extending group", data_type_inc="BaseType", data_type_def="ExtType", groups=[override_group]
        )

        # Resolve the extension
        ext_spec.resolve_inc_spec(base_spec)

        # Test base spec has no overridden groups
        self.assertFalse(base_spec.is_overridden_group("test_group"))

        # Test extending spec correctly identifies overridden group
        self.assertTrue(ext_spec.is_overridden_group("test_group"))

        # Test non-existent group raises error
        with self.assertRaisesWith(ValueError, "Group 'nonexistent_group' not found in spec"):
            ext_spec.is_overridden_group("nonexistent_group")

        # Test new group in extending spec is not overridden
        new_group = GroupSpec(doc="New group", name="new_group")
        ext_spec.set_group(new_group)
        self.assertFalse(ext_spec.is_overridden_group("new_group"))

    def test_resolve_inc_spec_group_spec_inheritance(self):
        """Test resolution of inherited groups in GroupSpec.resolve_inc_spec."""
        # Create base group with named and unnamed groups
        unnamed_group = GroupSpec(doc="An unnamed group", data_type_def="UnnamedType")
        named_group = GroupSpec(doc="A named group", name="named_group")
        base_groups = [unnamed_group, named_group]

        base_spec = GroupSpec(doc="A test group", data_type_def="BaseType", groups=base_groups)

        # Create extending group that overrides the named group and adds a new one
        override_group = GroupSpec(doc="Override named group", name="named_group")
        new_group = GroupSpec(doc="A new group", name="new_group")
        ext_groups = [override_group, new_group]

        ext_spec = GroupSpec(
            doc="An extending group", data_type_inc="BaseType", data_type_def="ExtType", groups=ext_groups
        )

        # Resolve the extension
        ext_spec.resolve_inc_spec(base_spec)

        # Test unnamed group is added to data_types
        self.assertEqual(ext_spec.get_data_type("UnnamedType"), unnamed_group)

        # Test named group is overridden
        resolved_group = ext_spec.get_group("named_group")
        self.assertEqual(resolved_group.doc, "Override named group")
        self.assertTrue(ext_spec.is_overridden_spec(resolved_group))

        # Test new group is added
        new_resolved = ext_spec.get_group("new_group")
        self.assertEqual(new_resolved.doc, "A new group")
        self.assertFalse(ext_spec.is_overridden_spec(new_resolved))

    def test_resolve_inc_spec_group_spec_inheritance_multiple(self):
        """Test resolution of multiple levels of group inheritance."""
        # Base spec with a named group
        base_group = GroupSpec(doc="Base group", name="test_group")
        base_spec = GroupSpec(doc="A base group", data_type_def="BaseType", groups=[base_group])

        # First extension overrides the group
        mid_group = GroupSpec(doc="Mid group", name="test_group")
        mid_spec = GroupSpec(
            doc="A middle group", data_type_inc="BaseType", data_type_def="MidType", groups=[mid_group]
        )

        # Second extension inherits without override
        ext_spec = GroupSpec(doc="An extending group", data_type_inc="MidType", data_type_def="ExtType")

        # Resolve the extensions
        mid_spec.resolve_inc_spec(base_spec)
        ext_spec.resolve_inc_spec(mid_spec)

        # Test group inheritance through multiple levels
        resolved_group = ext_spec.get_group("test_group")
        self.assertEqual(resolved_group.doc, "Mid group")
        self.assertTrue(ext_spec.is_inherited_spec(resolved_group))

    def test_resolve_inc_spec_group_spec_links_no_overwrite(self):
        link0 = LinkSpec(doc="Link 0", target_type="TargetType0")  # test unnamed
        link1 = LinkSpec(doc="Link 1", target_type="TargetType1", name="MyType1")  # test named
        link2 = LinkSpec(doc="Link 2", target_type="TargetType2", quantity="*")  # test named, multiple
        links = [link0, link1, link2]
        parent_spec = GroupSpec(
            data_type_def="ParentType",
            doc="A test group",
            links=links,
        )
        child_spec = GroupSpec(
            data_type_def="ChildType",
            data_type_inc="ParentType",
            doc="A test group",
        )
        child_spec.resolve_inc_spec(parent_spec)

        for link in links:
            with self.subTest(link_target_type=link.target_type):
                self.assertTrue(child_spec.is_inherited_spec(link))
                self.assertFalse(child_spec.is_overridden_spec(link))

    def test_resolve_inc_spec_group_spec_links_overwrite(self):
        link0 = LinkSpec(doc="Link 0", target_type="TargetType0", name="MyType0")
        link1 = LinkSpec(doc="Link 1", target_type="TargetType1", name="MyType1")
        # NOTE overwriting unnamed LinkSpec is not allowed
        # TODO test overwriting LinkSpec or DatasetSpec with mismatched quantity
        links = [link0, link1]
        parent_spec = GroupSpec(
            data_type_def="ParentType",
            doc="A test group",
            links=links,
        )

        link0_overwrite = LinkSpec(doc="New link 0", target_type="TargetType0", name="MyType0")
        link1_overwrite = LinkSpec(doc="New link 1", target_type="TargetType1Child", name="MyType1")
        overwritten_links = [link0_overwrite, link1_overwrite]
        child_spec = GroupSpec(
            data_type_def="ChildType",
            data_type_inc="ParentType",
            doc="A test group",
            links=overwritten_links,
        )
        child_spec.resolve_inc_spec(parent_spec)

        for link in overwritten_links:
            with self.subTest(link_target_type=link.target_type):
                self.assertTrue(child_spec.is_inherited_spec(link))
                self.assertTrue(child_spec.is_overridden_spec(link))

    def test_resolve_inc_spec_is_inherited_two_different_datasets(self):
        """Test is_inherited_spec with different attribute names in base and extension."""
        # https://github.com/hdmf-dev/hdmf/issues/1121
        base_group = GroupSpec(
            doc="A test group",
            data_type_def="MyGroup",
            datasets=[
                DatasetSpec(
                    name="dset1",
                    doc="dset1",
                    dtype="int",
                    attributes=[AttributeSpec("attr1", "MyGroup.dset1.attr1", "text")],
                ),
            ],
        )
        ext_group = GroupSpec(
            doc="A test subgroup",
            data_type_def="SubGroup",
            data_type_inc="MyGroup",
            datasets=[
                DatasetSpec(
                    name="dset2",
                    doc="dset2",
                    dtype="int",
                    attributes=[AttributeSpec("attr1", "SubGroup.dset2.attr1", "text")],
                ),
            ],
        )
        ext_group.resolve_inc_spec(base_group)

        self.assertFalse(base_group.is_inherited_spec(base_group.datasets[0].attributes[0]))

        dset_spec_map = {dset.name: dset for dset in ext_group.datasets}
        self.assertFalse(ext_group.is_inherited_spec(dset_spec_map["dset2"].attributes[0]))
        self.assertTrue(ext_group.is_inherited_spec(dset_spec_map["dset1"].attributes[0]))

    def test_resolve_inc_spec_is_inherited_same_name(self):
        """Test is_inherited_spec with same attribute name in base and extension."""
        # https://github.com/hdmf-dev/hdmf/issues/1121
        base_group = GroupSpec(
            doc="A test group",
            data_type_def="MyGroup",
            attributes=[AttributeSpec("attr1", "MyGroup.attr1", "text")],  # <-- added from above test
            datasets=[
                DatasetSpec(
                    name="dset1",
                    doc="dset1",
                    dtype="int",
                    attributes=[AttributeSpec("attr1", "MyGroup.dset1.attr1", "text")],
                ),
            ],
        )
        ext_group = GroupSpec(
            doc="A test subgroup",
            data_type_def="SubGroup",
            data_type_inc="MyGroup",
            attributes=[AttributeSpec("attr1", "SubGroup.attr1", "text")],  # <-- added from above test
            datasets=[
                DatasetSpec(
                    name="dset2",
                    doc="dset2",
                    dtype="int",
                    attributes=[AttributeSpec("attr1", "SubGroup.dset2.attr1", "text")],
                ),
            ],
        )
        ext_group.resolve_inc_spec(base_group)

        self.assertFalse(base_group.is_inherited_spec(base_group.datasets[0].attributes[0]))

        dset_spec_map = {dset.name: dset for dset in ext_group.datasets}
        self.assertFalse(ext_group.is_inherited_spec(dset_spec_map["dset2"].attributes[0]))
        self.assertTrue(ext_group.is_inherited_spec(dset_spec_map["dset1"].attributes[0]))
        self.assertTrue(ext_group.is_inherited_spec(ext_group.attributes[0]))

        ext_group2 = GroupSpec(
            doc="A test subsubgroup",
            data_type_def="SubSubGroup",
            data_type_inc="SubGroup",
        )
        ext_group2.resolve_inc_spec(ext_group)

        dset_spec_map = {dset.name: dset for dset in ext_group2.datasets}
        self.assertTrue(ext_group2.is_inherited_spec(dset_spec_map["dset1"].attributes[0]))
        self.assertTrue(ext_group2.is_inherited_spec(dset_spec_map["dset2"].attributes[0]))
        self.assertTrue(ext_group2.is_inherited_spec(ext_group2.attributes[0]))

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

    def test_resolve_inc_spec_cpd_dtype_override_incompatible_dtype_error(self):
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

    def test_resolve_inc_spec_ref_dtype_to_simple_error(self):
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

    def test_resolve_inc_spec_simple_to_ref_dtype_error(self):
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

    def test_resolve_inc_spec_override_incompatible_dtype_error(self):
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

    def test_resolve_inc_spec_override_numeric_to_numeric_dtype(self):
        """Test that overriding a numeric dtype to numeric dtype works correctly."""
        # numeric is a special case that needs to be handled specially
        base_dataset = DatasetSpec(data_type_def="BaseCompound", dtype="numeric", doc="Base dataset")

        # Create an extension that overrides BaseCompound with a compatible dtype
        ext_dataset = DatasetSpec(
            data_type_inc="BaseCompound",
            dtype="numeric",
            doc="Extended dataset",
        )

        ext_dataset.resolve_inc_spec(base_dataset)
        self.assertEqual(ext_dataset.dtype, "numeric")

    def test_resolve_inc_spec_override_numeric_to_float_dtype(self):
        """Test that overriding a numeric dtype to float dtype works correctly."""
        base_dataset = DatasetSpec(data_type_def="BaseCompound", dtype="numeric", doc="Base dataset")

        # Create an extension that overrides BaseCompound with a compatible dtype
        ext_dataset = DatasetSpec(
            data_type_inc="BaseCompound",
            dtype="float32",
            doc="Extended dataset",
        )

        ext_dataset.resolve_inc_spec(base_dataset)
        self.assertEqual(ext_dataset.dtype, "float32")

    def test_resolve_inc_spec_shape_dims_inheritance(self):
        """Test that shape and dims are inherited correctly."""
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
            doc="Extended dataset",
        )

        # Resolve the extension
        ext_dataset.resolve_inc_spec(base_dataset)

        # Check that shape and dims are inherited
        self.assertEqual(ext_dataset.shape, (None, 3))
        self.assertEqual(ext_dataset.dims, ("x", "y"))

        # test without data_type_def
        ext_dataset2 = DatasetSpec(
            data_type_inc="BaseWithShape",
            doc="Extended dataset",
        )

        # Resolve the extension
        ext_dataset2.resolve_inc_spec(base_dataset)

        # Check that shape and dims are inherited
        self.assertEqual(ext_dataset2.shape, (None, 3))
        self.assertEqual(ext_dataset2.dims, ("x", "y"))

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

    # TODO: re-enable when this is implemented
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

    # TODO: re-enable when this is implemented
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
            datasets=[DatasetSpec(name="col", data_type_inc="D1", doc="D1 col in A1")],
            doc="Group A1",
        )
        a2 = GroupSpec(
            data_type_def="A2",
            data_type_inc="A1",
            datasets=[
                DatasetSpec(
                    name="col",
                    data_type_inc="D1",
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
            shape=((None, None), (None, None, None)),
            dtype="float64",
            attributes=[AttributeSpec(name="attr3", dtype="float", doc="Attribute 3")],
            doc="Dataset D2 extending D1 with restrictions",
        )
        a4 = GroupSpec(
            data_type_def="A4",
            data_type_inc="A1",
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
            a2_loaded.datasets[0].attributes, (AttributeSpec(name="attr1", dtype="int", doc="Attribute 1"),)
        )

        a3_loaded = self.ns_catalog.get_spec_for_type("A3")
        self.assertEqual(
            a3_loaded.datasets[0].attributes,
            (
                AttributeSpec(name="attr2", dtype="text", doc="Attribute 2"),
                AttributeSpec(name="attr1", dtype="int", doc="Attribute 1"),
            ),
        )

        a4_loaded = self.ns_catalog.get_spec_for_type("A4")
        self.assertTrue(a4_loaded.datasets[0].resolved)
        self.assertEqual(
            a4_loaded.datasets[0].attributes,
            (
                AttributeSpec(name="attr4", dtype="float", doc="Attribute 4"),
                # AttributeSpec(name="attr3", dtype="float", doc="Attribute 3"),  # TODO this should exist
            ),
        )
        # self.assertEqual(a4_loaded.datasets[0].dtype, "float64")  # TODO this should work

    # def test_resolve_all_specs_subspec_data_type_mismatch_error1(self):
    #     # DatasetSpec D1 has 1D, 2D, or 3D shape, any dtype, no attributes
    #     # GroupSpec A1 contains a DatasetSpec D1 dataset named "col"
    #     # GroupSpec A2 extends A1
    #     # A2 specifies that the dataset "col" does not have a data type - this should cause an error
    #     # because A1/col is of type D1
    #     d1 = DatasetSpec(
    #         data_type_def="D1",
    #         name="col",
    #         dtype=None,
    #         shape=((None,), (None, None), (None, None, None)),
    #         doc="Dataset D1",
    #     )
    #     a1 = GroupSpec(
    #         data_type_def="A1",
    #         datasets=[DatasetSpec(name="col", data_type_inc="D1", doc="D1 col in A1")],
    #         doc="Group A1",
    #     )
    #     a2 = GroupSpec(
    #         data_type_def="A2",
    #         data_type_inc="A1",
    #         datasets=[
    #             DatasetSpec(
    #                 # no data_type_inc here should cause an error
    #                 name="col",
    #                 shape=((None,), (None, None), (None, None, None)),
    #                 dtype="int32",
    #                 doc="Column in A2 that conflicts with A1/col data type",
    #             )
    #         ],
    #         doc="Group A2",
    #     )
    #     ns_path = self.create_test_namespace("test", [d1, a1, a2])
    #     self.ns_catalog.load_namespaces(ns_path)

    #     msg = "TODO"
    #     with self.assertRaisesWith(ValueError, msg):
    #         self.ns_catalog.resolve_all_specs()

    # def test_resolve_all_specs_subspec_data_type_mismatch_error2(self):
    #     # DatasetSpec D1 has 1D, 2D, or 3D shape, any dtype, no attributes
    #     # GroupSpec A1 contains a DatasetSpec D1 dataset named "col"
    #     # GroupSpec A2 extends A1
    #     # A2 specifies that the dataset "col" has data type D2 that does not inherit from D1 - this should cause an
    #     # error because A1/col is of type D1
    #     d1 = DatasetSpec(
    #         data_type_def="D1",
    #         name="col",
    #         dtype=None,
    #         shape=(None,),
    #         doc="Dataset D1",
    #     )
    #     d2 = DatasetSpec(
    #         data_type_def="D2",
    #         name="col",
    #         dtype=None,
    #         shape=(None,),
    #         doc="Dataset D2",
    #     )
    #     a1 = GroupSpec(
    #         data_type_def="A1",
    #         datasets=[DatasetSpec(name="col", data_type_inc="D1", doc="D1 col in A1")],
    #         doc="Group A1",
    #     )
    #     a2 = GroupSpec(
    #         data_type_def="A2",
    #         data_type_inc="A1",
    #         datasets=[
    #             DatasetSpec(
    #                 # conflicting data_type_inc here should cause an error
    #                 name="col",
    #                 data_type_inc="D2",
    #                 doc="Column in A2 that conflicts with A1/col data type",
    #             )
    #         ],
    #         doc="Group A2",
    #     )
    #     ns_path = self.create_test_namespace("test", [d1, a1, a2])
    #     self.ns_catalog.load_namespaces(ns_path)

    #     msg = "TODO"
    #     with self.assertRaisesWith(ValueError, msg):
    #         self.ns_catalog.resolve_all_specs()

    # def test_resolve_all_specs_complex_error(self):
    #     # DatasetSpec D1 has 1D, 2D, or 3D shape, any dtype, no attributes
    #     # GroupSpec A1 contains a DatasetSpec D1 dataset named "col"
    #     # GroupSpec A2 extends A1
    #     # A2 specifies that the dataset "col" should have 1D or 2D shape, dtype int32, and an extra attribute attr1
    #     # GroupSpec A5 extends A2
    #     # A5 specifies that the dataset "col" should be of type D2. This will rarely happen. A2/col should be
    #     # brought in first when resolving A5's inc spec. Then the refinement of "col" in A5 to say that it should be
    #     # of type D2 should cause an error when it is found that D2's dtype is incompatible with A2/col's dtype.
    #     d1 = DatasetSpec(
    #         data_type_def="D1",
    #         name="col",
    #         dtype=None,
    #         shape=((None,), (None, None), (None, None, None)),
    #         doc="Dataset D1",
    #     )
    #     a1 = GroupSpec(
    #         data_type_def="A1",
    #         name="A1",
    #         datasets=[DatasetSpec(name="col", data_type_inc="D1", doc="D1 col in A1")],
    #         doc="Group A1",
    #     )
    #     a2 = GroupSpec(
    #         data_type_def="A2",
    #         data_type_inc="A1",
    #         name="A2",
    #         datasets=[
    #             DatasetSpec(
    #                 name="col",
    #                 data_type_inc="D1",  # TODO test whether this is necessary
    #                 shape=((None,), (None, None)),
    #                 dtype="int32",
    #                 attributes=[AttributeSpec(name="attr1", dtype="int", doc="Attribute 1")],
    #                 doc="Extended D1 col in A2 with restrictions and new attribute attr1",
    #             )
    #         ],
    #         doc="Group A2",
    #     )
    #     d2 = DatasetSpec(
    #         data_type_def="D2",
    #         data_type_inc="D1",
    #         shape=((None, None), (None, None, None)),
    #         dtype="float64",
    #         attributes=[AttributeSpec(name="attr3", dtype="float", doc="Attribute 3")],
    #         doc="Dataset D2 extending D1 with restrictions and new attribute attr3",
    #     )
    #     a5 = GroupSpec(
    #         data_type_def="A5",
    #         data_type_inc="A2",
    #         name="A5",
    #         datasets=[
    #             # A5 defines "col" to be of type D2 (dtype float64, shape (1D, 2D)), which is incompatible with A2/col
    #             # (dtype int32, shape (2D, 3D)), and that should cause an error during resolution
    #             DatasetSpec(
    #                 name="col",
    #                 data_type_inc="D2",
    #                 doc="D2 col in A5",
    #             )
    #         ],
    #         doc="Group A5",
    #     )
    #     ns_path = self.create_test_namespace("test", [d1, d2, a1, a2, a5])
    #     self.ns_catalog.load_namespaces(ns_path)

    #     msg = ("Could not resolve all specifications. The following specifications could not be resolved: "
    #            "A5, col in A5")
    #     with self.assertRaisesWith(RuntimeError, msg):
    #         self.ns_catalog.resolve_all_specs()

    def test_resolve_inc_spec_ref_dtype_subtype(self):
        """Test that resolving a ref dtype subtype raises no error."""
        g1 = GroupSpec(data_type_def="G1", doc="A group type")
        g2 = GroupSpec(data_type_def="G2", data_type_inc="G1", doc="A group subtype")

        d1 = DatasetSpec(
            data_type_def="D1",
            dtype=RefSpec(target_type="G1", reftype="object"),
            doc="Base dataset with ref dtype",
        )

        d2 = DatasetSpec(
            data_type_inc="D1",
            data_type_def="D2",
            dtype=RefSpec(target_type="G2", reftype="object"),
            doc="Extended dataset with ref dtype that is a subtype of D1's ref dtype",
        )

        ns_path = self.create_test_namespace("test", [g1, g2, d1, d2])
        self.ns_catalog.load_namespaces(ns_path)

        self.ns_catalog.resolve_all_specs()

        self.assertEqual(d2.dtype, RefSpec(target_type="G2", reftype="object"))

    # def test_resolve_inc_spec_ref_dtype_mismatch_error(self):
    #     """Test that resolving ref dtypes mismatches raises an error."""
    #     # Not sure if this should be tested through NamespaceCatalog.resolve_all_specs or on
    #     # DatasetSpec.resolve_inc_spec directly
    #     g1 = GroupSpec(data_type_def="G1", doc="A group type")
    #     h1 = GroupSpec(data_type_def="H1", doc="An unrelated group type")

    #     d1 = DatasetSpec(
    #         data_type_def="D1",
    #         dtype=RefSpec(target_type="G1", reftype="object"),
    #         doc="Base dataset with ref dtype",
    #     )

    #     d2 = DatasetSpec(
    #         data_type_inc="D1",
    #         data_type_def="D2",
    #         dtype=RefSpec(target_type="H1", reftype="object"),
    #         doc="Extended dataset with ref dtype that is not a subtype of D1's ref dtype",
    #     )

    #     ns_path = self.create_test_namespace("test", [g1, h1, d1, d2])
    #     self.ns_catalog.load_namespaces(ns_path)

    #     msg = "TODO"
    #     with self.assertRaisesWith(ValueError, msg):
    #         self.ns_catalog.resolve_all_specs()

    #     self.assertEqual(d2.dtype, RefSpec(target_type="H1", reftype="object"))
