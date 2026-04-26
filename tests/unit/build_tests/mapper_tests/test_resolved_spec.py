"""Tests for resolved_spec wiring in ObjectMapper.__get_subspec_values matcher.

When __get_subspec_values pairs a sub-builder to a subspec from the parent's
spec, it records that subspec on the sub-builder via builder.resolved_spec.
Read-time consumers (e.g., dtype-driven datetime parsing) read this slot to
honor inc-site overrides without re-deriving the builder-to-subspec match.
"""

from hdmf import Container, Data
from hdmf.build import (BuildManager, DatasetBuilder, GroupBuilder, LinkBuilder, ObjectMapper, TypeMap)
from hdmf.spec import (AttributeSpec, DatasetSpec, GroupSpec, LinkSpec, NamespaceCatalog, SpecCatalog, SpecNamespace)
from hdmf.testing import TestCase
from hdmf.utils import docval, getargs

from tests.unit.helpers.utils import CORE_NAMESPACE


class Child(Data):

    @property
    def data_type(self):
        return 'Child'


class ChildGroup(Container):

    @property
    def data_type(self):
        return 'ChildGroup'


class Parent(Container):

    @docval({'name': 'name', 'type': str, 'doc': 'name'},
            {'name': 'attr1', 'type': str, 'doc': 'an attribute', 'default': 'v'},
            {'name': 'named_child', 'type': Child, 'doc': 'a named typed child', 'default': None},
            {'name': 'group_child', 'type': ChildGroup, 'doc': 'a typed group child', 'default': None},
            {'name': 'children', 'type': list, 'doc': 'unnamed typed children', 'default': None})
    def __init__(self, **kwargs):
        name, attr1, named_child, group_child, children = getargs(
            'name', 'attr1', 'named_child', 'group_child', 'children', kwargs)
        super().__init__(name=name)
        self.__attr1 = attr1
        self.__named_child = named_child
        self.__group_child = group_child
        self.__children = children
        for c in (named_child, group_child):
            if c is not None and c.parent is None:
                c.parent = self
        if children is not None:
            for c in children:
                if c.parent is None:
                    c.parent = self

    @property
    def data_type(self):
        return 'Parent'

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
    def children(self):
        return self.__children


_TYPE_TO_CLASS = {'Parent': Parent, 'Child': Child, 'ChildGroup': ChildGroup}


def _build_type_map(parent_spec, child_specs):
    catalog = SpecCatalog()
    catalog.register_spec(parent_spec, 'test.yaml')
    for s in child_specs:
        catalog.register_spec(s, 'test.yaml')
    namespace = SpecNamespace('a test namespace', CORE_NAMESPACE,
                              [{'source': 'test.yaml'}], version='0.1.0', catalog=catalog)
    namespace_catalog = NamespaceCatalog()
    namespace_catalog.add_namespace(CORE_NAMESPACE, namespace)
    type_map = TypeMap(namespace_catalog)
    for s in [parent_spec] + list(child_specs):
        dt = getattr(s, 'data_type_def', None)
        if dt is not None and dt in _TYPE_TO_CLASS:
            type_map.register_container_type(CORE_NAMESPACE, dt, _TYPE_TO_CLASS[dt])
    return type_map, namespace


class TestResolvedSpecOnRead(TestCase):
    """The read-path matcher sets builder.resolved_spec for each subspec kind."""

    def _make_parent_spec(self, datasets=(), groups=(), links=()):
        return GroupSpec(
            doc='parent', data_type_def='Parent',
            attributes=[AttributeSpec('attr1', 'a', 'text')],
            datasets=list(datasets), groups=list(groups), links=list(links),
        )

    def _construct_with(self, parent_spec, child_specs, parent_builder):
        type_map, _ = _build_type_map(parent_spec, child_specs)
        manager = BuildManager(type_map)
        mapper = ObjectMapper(parent_spec)
        mapper.construct(parent_builder, manager)
        return mapper

    def test_named_typed_dataset_child(self):
        """Named DatasetSpec with data_type_inc -> child builder gets the position subspec."""
        child_def = DatasetSpec(doc='child', data_type_def='Child', dtype='int')
        # position: same name in parent; could carry overrides like dtype/shape later
        named_subspec = DatasetSpec(doc='named child', data_type_inc='Child', name='named')
        parent_spec = self._make_parent_spec(datasets=[named_subspec])

        child_db = DatasetBuilder('named', list(range(3)),
                                  attributes={'data_type': 'Child', 'namespace': CORE_NAMESPACE,
                                              'object_id': 'cid'})
        parent_gb = GroupBuilder('p', datasets={'named': child_db},
                                 attributes={'attr1': 'v', 'data_type': 'Parent',
                                             'namespace': CORE_NAMESPACE, 'object_id': 'pid'})

        self._construct_with(parent_spec, [child_def], parent_gb)
        self.assertIs(child_db.resolved_spec, named_subspec)

    def test_typed_group_child(self):
        """Named GroupSpec with data_type_inc -> child group builder gets the position subspec."""
        child_def = GroupSpec(doc='child group', data_type_def='ChildGroup')
        group_subspec = GroupSpec(doc='gc', data_type_inc='ChildGroup', name='gc')
        parent_spec = self._make_parent_spec(groups=[group_subspec])

        child_gb = GroupBuilder('gc',
                                attributes={'data_type': 'ChildGroup', 'namespace': CORE_NAMESPACE,
                                            'object_id': 'gcid'})
        parent_gb = GroupBuilder('p', groups={'gc': child_gb},
                                 attributes={'attr1': 'v', 'data_type': 'Parent',
                                             'namespace': CORE_NAMESPACE, 'object_id': 'pid'})

        self._construct_with(parent_spec, [child_def], parent_gb)
        self.assertIs(child_gb.resolved_spec, group_subspec)

    def test_unnamed_typed_dataset_children(self):
        """Unnamed (data_type-only) DatasetSpec -> every matched builder gets the same subspec."""
        child_def = DatasetSpec(doc='child', data_type_def='Child', dtype='int')
        unnamed_subspec = DatasetSpec(doc='any children', data_type_inc='Child', quantity='*')
        parent_spec = self._make_parent_spec(datasets=[unnamed_subspec])

        c1 = DatasetBuilder('a', [1, 2],
                            attributes={'data_type': 'Child', 'namespace': CORE_NAMESPACE, 'object_id': 'a'})
        c2 = DatasetBuilder('b', [3, 4],
                            attributes={'data_type': 'Child', 'namespace': CORE_NAMESPACE, 'object_id': 'b'})
        parent_gb = GroupBuilder('p', datasets={'a': c1, 'b': c2},
                                 attributes={'attr1': 'v', 'data_type': 'Parent',
                                             'namespace': CORE_NAMESPACE, 'object_id': 'pid'})

        self._construct_with(parent_spec, [child_def], parent_gb)
        self.assertIs(c1.resolved_spec, unnamed_subspec)
        self.assertIs(c2.resolved_spec, unnamed_subspec)

    def test_named_structural_subspec_recursive(self):
        """Named subspec with no data_type triggers recursion; the structural builder gets resolved_spec."""
        # parent has a named, untyped sub-group containing an attribute
        nested_subspec = GroupSpec(doc='nested', name='nested',
                                   attributes=[AttributeSpec('inner_attr', 'x', 'text')])
        parent_spec = self._make_parent_spec(groups=[nested_subspec])

        nested_gb = GroupBuilder('nested', attributes={'inner_attr': 'val'})
        parent_gb = GroupBuilder('p', groups={'nested': nested_gb},
                                 attributes={'attr1': 'v', 'data_type': 'Parent',
                                             'namespace': CORE_NAMESPACE, 'object_id': 'pid'})

        self._construct_with(parent_spec, [], parent_gb)
        self.assertIs(nested_gb.resolved_spec, nested_subspec)

    def test_link_target_resolved_spec_not_set_by_link_match(self):
        """A LinkBuilder target is not assigned resolved_spec by the link-matching path.

        The link matcher constructs the target builder but does not record a resolved_spec
        on it, because the target's position-resolved spec is the type def-site (or whatever
        position originally placed it in the tree), not the parent's LinkSpec.
        """
        child_def = GroupSpec(doc='child group', data_type_def='ChildGroup')
        link_subspec = LinkSpec(doc='a link', name='lnk', target_type='ChildGroup')
        parent_spec = self._make_parent_spec(links=[link_subspec])

        target_gb = GroupBuilder('target',
                                 attributes={'data_type': 'ChildGroup', 'namespace': CORE_NAMESPACE,
                                             'object_id': 'tid'})
        link_b = LinkBuilder(target_gb, 'lnk')
        parent_gb = GroupBuilder('p', links={'lnk': link_b},
                                 attributes={'attr1': 'v', 'data_type': 'Parent',
                                             'namespace': CORE_NAMESPACE, 'object_id': 'pid'})

        self._construct_with(parent_spec, [child_def], parent_gb)
        self.assertIsNone(target_gb.resolved_spec)

    def test_write_once_preserved_across_matchers(self):
        """If a builder somehow already has resolved_spec set, the matcher does not overwrite it."""
        child_def = DatasetSpec(doc='child', data_type_def='Child', dtype='int')
        named_subspec = DatasetSpec(doc='named', data_type_inc='Child', name='named')
        parent_spec = self._make_parent_spec(datasets=[named_subspec])

        sentinel = object()
        child_db = DatasetBuilder('named', [1, 2],
                                  attributes={'data_type': 'Child', 'namespace': CORE_NAMESPACE,
                                              'object_id': 'cid'})
        child_db.resolved_spec = sentinel  # pre-set
        parent_gb = GroupBuilder('p', datasets={'named': child_db},
                                 attributes={'attr1': 'v', 'data_type': 'Parent',
                                             'namespace': CORE_NAMESPACE, 'object_id': 'pid'})

        self._construct_with(parent_spec, [child_def], parent_gb)
        self.assertIs(child_db.resolved_spec, sentinel)

    def test_dynamic_table_column_dtype_override(self):
        """The motivating case: a typed column subspec carries an inc-site dtype override.

        After construct, the column builder's resolved_spec is the parent's DatasetSpec for that
        column position, which already reflects the inc-site dtype after namespace resolution.
        """
        # Define a generic typed dataset with no fixed dtype (analogous to VectorData).
        col_def = DatasetSpec(doc='generic column', data_type_def='Child')
        # Position-specific override declares isodatetime on the column inc-site.
        col_subspec = DatasetSpec(doc='date column', data_type_inc='Child',
                                  name='date_of_birth', dtype='isodatetime')
        parent_spec = self._make_parent_spec(datasets=[col_subspec])

        col_db = DatasetBuilder('date_of_birth', ['2020-01-01', '2021-06-15'],
                                attributes={'data_type': 'Child', 'namespace': CORE_NAMESPACE,
                                            'object_id': 'cid'})
        parent_gb = GroupBuilder('p', datasets={'date_of_birth': col_db},
                                 attributes={'attr1': 'v', 'data_type': 'Parent',
                                             'namespace': CORE_NAMESPACE, 'object_id': 'pid'})

        self._construct_with(parent_spec, [col_def], parent_gb)
        self.assertIs(col_db.resolved_spec, col_subspec)
        self.assertEqual(col_db.resolved_spec.dtype, 'isodatetime')
