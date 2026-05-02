"""Tests for matched_spec wiring in ObjectMapper build (write) path.

When the build path creates a sub-builder for a position in the parent's spec
tree (a typed dataset, a typed group, or an untyped named dataset/group), it
records that subspec on the sub-builder via builder.matched_spec. Build-time
consumers read this slot to honor inc-site overrides without re-deriving the
position pairing.
"""

from hdmf import Container, Data
from hdmf.build import BuildManager, TypeMap
from hdmf.spec import AttributeSpec, DatasetSpec, GroupSpec, NamespaceCatalog, SpecCatalog, SpecNamespace
from hdmf.testing import TestCase
from hdmf.utils import docval, getargs

from tests.unit.helpers.utils import CORE_NAMESPACE


class Child(Data):

    @docval(
        {"name": "name", "type": str, "doc": "name"}, {"name": "data", "type": ("array_data", "data"), "doc": "data"}
    )
    def __init__(self, **kwargs):
        name, data = getargs("name", "data", kwargs)
        super().__init__(name=name, data=data)

    @property
    def data_type(self):
        return "Child"


class ChildGroup(Container):

    @docval({"name": "name", "type": str, "doc": "name"})
    def __init__(self, **kwargs):
        name = getargs("name", kwargs)
        super().__init__(name=name)

    @property
    def data_type(self):
        return "ChildGroup"


class Parent(Container):

    @docval(
        {"name": "name", "type": str, "doc": "name"},
        {"name": "attr1", "type": str, "doc": "an attribute", "default": "v"},
        {"name": "named_child", "type": Child, "doc": "a typed dataset child", "default": None},
        {"name": "group_child", "type": ChildGroup, "doc": "a typed group child", "default": None},
        {"name": "inline_data", "type": ("array_data", "data"), "doc": "untyped dataset", "default": None},
    )
    def __init__(self, **kwargs):
        name, attr1, named_child, group_child, inline_data = getargs(
            "name", "attr1", "named_child", "group_child", "inline_data", kwargs
        )
        super().__init__(name=name)
        self.__attr1 = attr1
        self.__named_child = named_child
        self.__group_child = group_child
        self.__inline_data = inline_data
        for c in (named_child, group_child):
            if c is not None and c.parent is None:
                c.parent = self

    @property
    def data_type(self):
        return "Parent"

    @property
    def attr1(self):
        return self.__attr1

    @property
    def named_child(self):
        return self.__named_child

    @property
    def group_child(self):
        return self.__group_child

    @property
    def inline_data(self):
        return self.__inline_data


_TYPE_TO_CLASS = {"Parent": Parent, "Child": Child, "ChildGroup": ChildGroup}


def _build_type_map(parent_spec, child_specs=()):
    catalog = SpecCatalog()
    catalog.register_spec(parent_spec, "test.yaml")
    for s in child_specs:
        catalog.register_spec(s, "test.yaml")
    namespace = SpecNamespace(
        "a test namespace", CORE_NAMESPACE, [{"source": "test.yaml"}], version="0.1.0", catalog=catalog
    )
    namespace_catalog = NamespaceCatalog()
    namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)
    namespace_catalog.resolve_all_specs()
    type_map = TypeMap(namespace_catalog)
    for s in [parent_spec] + list(child_specs):
        dt = getattr(s, "data_type_def", None)
        if dt is not None and dt in _TYPE_TO_CLASS:
            type_map.register_container_type(CORE_NAMESPACE, dt, _TYPE_TO_CLASS[dt])
    return type_map


class TestMatchedSpecOnWrite(TestCase):

    def _make_parent_spec(self, datasets=(), groups=(), links=()):
        return GroupSpec(
            doc="parent",
            data_type_def="Parent",
            attributes=[AttributeSpec("attr1", "a", "text")],
            datasets=list(datasets),
            groups=list(groups),
            links=list(links),
        )

    def _build(self, parent_spec, child_specs, container):
        type_map = _build_type_map(parent_spec, child_specs)
        manager = BuildManager(type_map)
        return manager.build(container), manager

    def test_typed_dataset_child(self):
        """A typed DatasetSpec child gets matched_spec set to its parent's subspec."""
        child_def = DatasetSpec(doc="child", data_type_def="Child", dtype="int", shape=(None,))
        named_subspec = DatasetSpec(doc="named child", data_type_inc="Child", name="named_child")
        parent_spec = self._make_parent_spec(datasets=[named_subspec])

        c = Child("named_child", [1, 2, 3])
        p = Parent("p", named_child=c)

        builder, _ = self._build(parent_spec, [child_def], p)
        child_builder = builder.datasets["named_child"]
        self.assertIs(child_builder.matched_spec, named_subspec)

    def test_typed_group_child(self):
        """A typed GroupSpec child gets matched_spec set to its parent's subspec."""
        child_def = GroupSpec(doc="child group", data_type_def="ChildGroup")
        group_subspec = GroupSpec(doc="gc", data_type_inc="ChildGroup", name="group_child")
        parent_spec = self._make_parent_spec(groups=[group_subspec])

        gc = ChildGroup("group_child")
        p = Parent("p", group_child=gc)

        builder, _ = self._build(parent_spec, [child_def], p)
        gc_builder = builder.groups["group_child"]
        self.assertIs(gc_builder.matched_spec, group_subspec)

    def test_untyped_named_dataset(self):
        """An untyped, named dataset gets matched_spec set to its position subspec."""
        inline_subspec = DatasetSpec(doc="inline data", name="inline_data", dtype="int", shape=(None,))
        parent_spec = self._make_parent_spec(datasets=[inline_subspec])

        p = Parent("p", inline_data=[1, 2, 3])

        builder, _ = self._build(parent_spec, [], p)
        ds_builder = builder.datasets["inline_data"]
        self.assertIs(ds_builder.matched_spec, inline_subspec)

    def test_untyped_named_group(self):
        """An untyped, named *required* group gets matched_spec set to its position subspec.

        The build path skips empty optional named groups, so a required group with no
        sub-elements is the minimal way to exercise this branch.
        """
        # Default quantity=1 makes this required; required=False would let the build path skip it.
        nested_subspec = GroupSpec(doc="nested", name="nested")
        parent_spec = self._make_parent_spec(groups=[nested_subspec])

        p = Parent("p", attr1="v")

        builder, _ = self._build(parent_spec, [], p)
        nested_builder = builder.groups["nested"]
        self.assertIs(nested_builder.matched_spec, nested_subspec)

    def test_dynamic_table_column_dtype_override(self):
        """Inc-site dtype override (DynamicTable column case) on the build side.

        The parent's subspec carries the override (dtype=isodatetime), and the column
        builder produced by the build path records that subspec on matched_spec.
        """
        col_def = DatasetSpec(doc="generic column", data_type_def="Child")
        col_subspec = DatasetSpec(doc="date column", data_type_inc="Child", name="named_child", dtype="isodatetime")
        parent_spec = self._make_parent_spec(datasets=[col_subspec])

        c = Child("named_child", ["2020-01-01", "2021-06-15"])
        p = Parent("p", named_child=c)

        builder, _ = self._build(parent_spec, [col_def], p)
        child_builder = builder.datasets["named_child"]
        self.assertIs(child_builder.matched_spec, col_subspec)
        self.assertEqual(child_builder.matched_spec.dtype, "isodatetime")
