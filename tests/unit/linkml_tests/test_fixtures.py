"""Tests for the hand-authored LinkML translation of the minimal hdmf-common test namespace.

The fixtures in ``fixtures/`` are the LinkML translation of ``base.yaml`` and ``sparse.yaml``
from the hdmf-common schema, following the conventions in ``docs/source/linkml_mapping.rst``.
They are the reference the LinkML reader and writer are tested against, so these tests check
that they are valid LinkML and that they carry every field of the HDMF ``Spec`` objects that
HDMF loads natively from the equivalent HDMFSL schema.
"""

import os

import pytest

from hdmf.common import get_type_map
from hdmf.spec import AttributeSpec, DatasetSpec, DtypeHelper, GroupSpec
from hdmf.testing import TestCase

try:
    from linkml_runtime.utils.schemaview import SchemaView

    REQUIREMENTS_INSTALLED = True
except ImportError:
    REQUIREMENTS_INSTALLED = False

FIXTURE_DIR = os.path.join(os.path.dirname(os.path.realpath(__file__)), "fixtures")

# the HDMFSL schema files the test namespace covers, and the data_type_defs in each
TEST_NAMESPACE_TYPES = {
    "base": ["Data", "Container", "SimpleMultiContainer"],
    "sparse": ["CSRMatrix"],
}

# HDMFSL dtypes deferred with references and compound dtypes, so not defined in
# hdmf-linkml-types
DEFERRED_DTYPES = {"object"}

# LinkML built-in types reused as-is for the HDMFSL dtypes of the same name
BUILTIN_DTYPES = {"float", "double", "date", "datetime"}

SPEC_TYPE_FOR_CLASS = {GroupSpec: "group", DatasetSpec: "dataset"}


def fixture_path(name):
    return os.path.join(FIXTURE_DIR, name + ".yaml")


def annotation(element, tag):
    """Return the value of an annotation on a LinkML element, or None if it is absent."""
    ann = element.annotations.get(tag)
    return None if ann is None else ann.value


def native_specs():
    """Return the Spec objects HDMF loads natively from the hdmf-common schema, by type name."""
    catalog = get_type_map().namespace_catalog
    names = [name for names in TEST_NAMESPACE_TYPES.values() for name in names]
    return {name: catalog.get_spec("hdmf-common", name) for name in names}


@pytest.mark.skipif(not REQUIREMENTS_INSTALLED, reason="optional LinkML module is not installed")
class TestFixturesAreValidLinkML(TestCase):
    """The fixtures are valid LinkML and resolve through the namespace-level schema."""

    def test_each_schema_loads(self):
        """Every fixture file loads under SchemaView on its own."""
        for name in ["hdmf-linkml-types", "base", "sparse", "namespace"]:
            with self.subTest(schema=name):
                view = SchemaView(fixture_path(name))
                self.assertIsNotNone(view.schema.id)
                self.assertIsNotNone(view.schema.name)

    def test_namespace_import_closure(self):
        """The namespace schema imports the per-file schemas and resolves their classes."""
        view = SchemaView(fixture_path("namespace"))
        self.assertEqual(view.schema.name, "hdmf-common-test")
        self.assertEqual(view.schema.version, "1.10.0")
        for name in ["hdmf-linkml-types", "base", "sparse"]:
            self.assertIn(name, view.schema.imports)
        expected = {name for names in TEST_NAMESPACE_TYPES.values() for name in names}
        self.assertTrue(expected.issubset(set(view.all_classes(imports=True))))

    def test_every_range_resolves(self):
        """Every slot range names a type or class that the import closure defines.

        SchemaView does not report a range that names nothing, so it is checked here.
        """
        view = SchemaView(fixture_path("namespace"))
        defined = set(view.all_types(imports=True)) | set(view.all_classes(imports=True))
        for class_name, class_def in view.all_classes(imports=True).items():
            for slot_name, slot in class_def.attributes.items():
                with self.subTest(cls=class_name, slot=slot_name):
                    self.assertIn(slot.range, defined)

    def test_namespace_authors_annotation(self):
        """The namespace author and contact lists are preserved as structured annotation values."""
        view = SchemaView(fixture_path("namespace"))
        authors = annotation(view.schema, "authors")
        self.assertEqual(
            authors,
            [
                {"name": "Andrew Tritt", "email": "ajtritt@lbl.gov"},
                {"name": "Oliver Ruebel", "email": "oruebel@lbl.gov"},
                {"name": "Ryan Ly", "email": "rly@lbl.gov"},
                {"name": "Ben Dichter", "email": "bdichter@lbl.gov"},
            ],
        )


@pytest.mark.skipif(not REQUIREMENTS_INSTALLED, reason="optional LinkML module is not installed")
class TestDtypeCoverage(TestCase):
    """hdmf-linkml-types covers the HDMFSL dtypes, keeping each synonym a distinct named type."""

    def test_covers_every_hdmfsl_dtype(self):
        """Every valid HDMFSL dtype string, including synonyms, resolves to a LinkML type."""
        view = SchemaView(fixture_path("hdmf-linkml-types"))
        available = set(view.all_types(imports=True))
        expected = set(DtypeHelper.valid_primary_dtypes) - DEFERRED_DTYPES
        self.assertEqual(expected - available, set())

    def test_defines_only_the_dtypes_linkml_lacks(self):
        """The dtypes LinkML already provides are reused rather than shadowed."""
        view = SchemaView(fixture_path("hdmf-linkml-types"))
        defined_here = set(view.schema.types)
        expected = set(DtypeHelper.valid_primary_dtypes) - DEFERRED_DTYPES - BUILTIN_DTYPES
        self.assertEqual(defined_here, expected)

    def test_synonyms_are_distinct_types(self):
        """Synonyms are separate named types, so the HDMFSL spelling survives the round trip."""
        view = SchemaView(fixture_path("hdmf-linkml-types"))
        for synonym, primary in [("uint", "uint32"), ("short", "int16"), ("long", "int64"),
                                 ("int", "int32"), ("utf8", "text"), ("bytes", "ascii")]:
            with self.subTest(dtype=synonym):
                self.assertEqual(view.get_type(synonym).typeof, primary)

    def test_any_type_is_defined(self):
        """The no-dtype case has a range to point at."""
        view = SchemaView(fixture_path("hdmf-linkml-types"))
        self.assertEqual(view.get_class("AnyType").class_uri, "linkml:Any")


@pytest.mark.skipif(not REQUIREMENTS_INSTALLED, reason="optional LinkML module is not installed")
class TestFixturesCoverTheHDMFSLSchema(TestCase):
    """Every field of the natively loaded Spec objects is represented in the fixtures.

    These checks walk the Spec objects HDMF loads from the hdmf-common schema rather than a
    hand-copied expectation, so a change to base.yaml or sparse.yaml that the fixtures do not
    follow shows up as a failure here.
    """

    @classmethod
    def setUpClass(cls):
        cls.view = SchemaView(fixture_path("namespace"))
        cls.specs = native_specs()

    @staticmethod
    def sub_datasets(spec):
        """Return the datasets entries of a spec. Only a GroupSpec can contain datasets."""
        return spec.datasets if isinstance(spec, GroupSpec) else []

    def get_slot(self, class_name, slot_name):
        slot = self.view.get_class(class_name).attributes.get(slot_name)
        self.assertIsNotNone(slot, msg="%s has no slot %s" % (class_name, slot_name))
        return slot

    def assert_array_matches(self, slot, spec, context):
        """The slot's array expression carries the spec's dims and shape."""
        if spec.dims is None:
            self.assertIsNone(slot.array, msg=context)
            return
        self.assertIsNotNone(slot.array, msg="%s has no array expression" % context)
        dimensions = slot.array.dimensions
        self.assertEqual([d.alias for d in dimensions], list(spec.dims), msg=context)
        self.assertEqual([d.exact_cardinality for d in dimensions], list(spec.shape), msg=context)

    def assert_named_slot_matches(self, class_name, spec, spec_type):
        """A named attribute or dataset maps to a slot whose range is its dtype."""
        context = "%s.%s" % (class_name, spec.name)
        slot = self.get_slot(class_name, spec.name)
        self.assertEqual(slot.description, spec.doc, msg=context)
        self.assertEqual(annotation(slot, "spec_type"), spec_type, msg=context)
        self.assertEqual(slot.range, spec.dtype if spec.dtype is not None else "AnyType", msg=context)
        self.assert_array_matches(slot, spec, context)

    def assert_include_slot_matches(self, class_name, spec, spec_type):
        """An unnamed data_type_inc maps to a slot whose range is the included type."""
        context = "%s include of %s" % (class_name, spec.data_type_inc)
        matches = [s for s in self.view.get_class(class_name).attributes.values()
                   if s.range == spec.data_type_inc]
        self.assertEqual(len(matches), 1, msg="%s: expected exactly one slot" % context)
        slot = matches[0]
        self.assertEqual(slot.description, spec.doc, msg=context)
        self.assertEqual(annotation(slot, "spec_type"), spec_type, msg=context)
        # quantity '*' is zero or more, so the slot is multivalued and not required
        self.assertEqual(spec.quantity, "*", msg=context)
        self.assertTrue(slot.multivalued, msg=context)
        self.assertFalse(slot.required, msg=context)
        self.assertTrue(slot.inlined_as_list, msg=context)

    def test_every_type_def_has_a_class(self):
        """Each data_type_def is a class carrying its doc, is_a, and spec_type."""
        for name, spec in self.specs.items():
            with self.subTest(type=name):
                class_def = self.view.get_class(name)
                self.assertIsNotNone(class_def)
                self.assertEqual(class_def.description, spec.doc)
                self.assertEqual(class_def.is_a, spec.data_type_inc)
                self.assertEqual(annotation(class_def, "spec_type"), SPEC_TYPE_FOR_CLASS[type(spec)])

    def test_root_classes_have_a_name_identifier(self):
        """The classes with no is_a declare the name identifier slot the subclasses inherit."""
        for name in ["Data", "Container"]:
            with self.subTest(type=name):
                slot = self.get_slot(name, "name")
                self.assertTrue(slot.identifier)
                self.assertEqual(slot.range, "string")
                self.assertIsNone(annotation(slot, "spec_type"))
        for name in ["SimpleMultiContainer", "CSRMatrix"]:
            with self.subTest(type=name):
                self.assertNotIn("name", self.view.get_class(name).attributes)

    def test_every_attribute_has_a_slot(self):
        """Each attributes entry is a slot tagged spec_type: attribute."""
        for name, spec in self.specs.items():
            for attribute in spec.attributes:
                with self.subTest(type=name, attribute=attribute.name):
                    self.assertIsInstance(attribute, AttributeSpec)
                    self.assert_named_slot_matches(name, attribute, "attribute")
                    slot = self.get_slot(name, attribute.name)
                    self.assertEqual(bool(slot.required), attribute.required)

    def test_every_dataset_has_a_slot(self):
        """Each datasets entry is a slot tagged spec_type: dataset."""
        for name, spec in self.specs.items():
            for dataset in self.sub_datasets(spec):
                with self.subTest(type=name, dataset=dataset.name or dataset.data_type_inc):
                    if dataset.name is not None:
                        self.assert_named_slot_matches(name, dataset, "dataset")
                    else:
                        self.assert_include_slot_matches(name, dataset, "dataset")

    def test_every_group_has_a_slot(self):
        """Each groups entry is a slot tagged spec_type: group."""
        for name, spec in self.specs.items():
            if not isinstance(spec, GroupSpec):
                continue
            for group in spec.groups:
                with self.subTest(type=name, group=group.name or group.data_type_inc):
                    self.assertIsNone(group.name, msg="named includes are out of scope")
                    self.assert_include_slot_matches(name, group, "group")

    def test_no_extra_slots(self):
        """The fixtures add nothing beyond the name identifier and the spec's own constructs.

        A slot the HDMFSL schema does not account for would become a Spec construct that the
        native load does not produce, so the reader's Spec would not compare equal.
        """
        for name, spec in self.specs.items():
            named = {c.name for c in list(spec.attributes) + list(self.sub_datasets(spec))
                     if c.name is not None}
            includes = {c.data_type_inc for c in
                        list(self.sub_datasets(spec)) + list(spec.groups if isinstance(spec, GroupSpec) else [])
                        if c.name is None}
            for slot_name, slot in self.view.get_class(name).attributes.items():
                with self.subTest(type=name, slot=slot_name):
                    if slot.identifier:
                        # the object's hierarchy name, not a declared attribute
                        self.assertIsNone(spec.data_type_inc)
                    elif slot.range in self.view.all_classes() and slot.range != "AnyType":
                        self.assertIn(slot.range, includes)
                    else:
                        self.assertIn(slot_name, named)
