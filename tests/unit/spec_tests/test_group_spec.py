import json

from hdmf.spec import GroupSpec, DatasetSpec, AttributeSpec, LinkSpec
from hdmf.testing import TestCase


class GroupSpecTests(TestCase):
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
                        attributes=self.dset2_attributes,
                        linkable=True,
                        data_type_def='VoltageArray')
        ]

        self.subgroups = [
            GroupSpec('A test subgroup',
                      name='subgroup1',
                      linkable=False),
            GroupSpec('A test subgroup',
                      name='subgroup2',
                      linkable=False)
        ]

    def test_constructor(self):
        spec = GroupSpec('A test group',
                         name='root_constructor',
                         groups=self.subgroups,
                         datasets=self.datasets,
                         attributes=self.attributes,
                         linkable=False)
        self.assertFalse(spec['linkable'])
        self.assertListEqual(spec['attributes'], self.attributes)
        self.assertListEqual(spec['datasets'], self.datasets)
        self.assertNotIn('data_type_def', spec)
        self.assertIs(spec, self.subgroups[0].parent)
        self.assertIs(spec, self.subgroups[1].parent)
        self.assertIs(spec, self.attributes[0].parent)
        self.assertIs(spec, self.attributes[1].parent)
        self.assertIs(spec, self.datasets[0].parent)
        self.assertIs(spec, self.datasets[1].parent)
        json.dumps(spec)

    def test_constructor_datatype(self):
        spec = GroupSpec('A test group',
                         name='root_constructor_datatype',
                         datasets=self.datasets,
                         attributes=self.attributes,
                         linkable=False,
                         data_type_def='EphysData')
        self.assertFalse(spec['linkable'])
        self.assertListEqual(spec['attributes'], self.attributes)
        self.assertListEqual(spec['datasets'], self.datasets)
        self.assertEqual(spec['data_type_def'], 'EphysData')
        self.assertIs(spec, self.attributes[0].parent)
        self.assertIs(spec, self.attributes[1].parent)
        self.assertIs(spec, self.datasets[0].parent)
        self.assertIs(spec, self.datasets[1].parent)
        self.assertEqual(spec.data_type_def, 'EphysData')
        self.assertIsNone(spec.data_type_inc)
        json.dumps(spec)

    def test_set_parent_exists(self):
        GroupSpec('A test group',
                  name='root_constructor',
                  groups=self.subgroups)
        msg = 'Cannot re-assign parent.'
        with self.assertRaisesWith(AttributeError, msg):
            self.subgroups[0].parent = self.subgroups[1]

    def test_set_dataset(self):
        spec = GroupSpec('A test group',
                         name='root_test_set_dataset',
                         linkable=False,
                         data_type_def='EphysData')
        spec.set_dataset(self.datasets[0])
        self.assertIs(spec, self.datasets[0].parent)

    def test_set_link(self):
        group = GroupSpec(
            doc='A test group',
            name='root'
        )
        link = LinkSpec(
            doc='A test link',
            target_type='LinkTarget',
            name='link_name'
        )
        group.set_link(link)
        self.assertIs(group, link.parent)
        self.assertIs(group.get_link('link_name'), link)

    def test_add_link(self):
        group = GroupSpec(
            doc='A test group',
            name='root'
        )
        group.add_link(
            'A test link',
            'LinkTarget',
            name='link_name'
        )
        self.assertIsInstance(group.get_link('link_name'), LinkSpec)

    def test_set_group(self):
        spec = GroupSpec('A test group',
                         name='root_test_set_group',
                         linkable=False,
                         data_type_def='EphysData')
        spec.set_group(self.subgroups[0])
        spec.set_group(self.subgroups[1])
        self.assertListEqual(spec['groups'], self.subgroups)
        self.assertIs(spec, self.subgroups[0].parent)
        self.assertIs(spec, self.subgroups[1].parent)
        json.dumps(spec)

    def test_add_group(self):
        group = GroupSpec(
            doc='A test group',
            name='root'
        )
        group.add_group(
            'A test group',
            name='subgroup'
        )
        self.assertIsInstance(group.get_group('subgroup'), GroupSpec)

    def test_type_extension(self):
        spec = GroupSpec('A test group',
                         name='parent_type',
                         datasets=self.datasets,
                         attributes=self.attributes,
                         linkable=False,
                         data_type_def='EphysData')
        dset1_attributes_ext = [
            AttributeSpec('dset1_extra_attribute', 'an extra attribute for the first dataset', 'text')
        ]
        ext_datasets = [
            DatasetSpec('my first dataset extension',
                        'int',
                        name='dataset1',
                        attributes=dset1_attributes_ext,
                        linkable=True),
        ]
        ext_attributes = [
            AttributeSpec('ext_extra_attribute', 'an extra attribute for the group', 'text'),
        ]
        ext = GroupSpec('A test group extension',
                        name='child_type',
                        datasets=ext_datasets,
                        attributes=ext_attributes,
                        linkable=False,
                        data_type_inc=spec,
                        data_type_def='SpikeData')
        ext_dset1 = ext.get_dataset('dataset1')
        ext_dset1_attrs = ext_dset1.attributes
        self.assertDictEqual(ext_dset1_attrs[0], dset1_attributes_ext[0])
        self.assertDictEqual(ext_dset1_attrs[1], self.dset1_attributes[0])
        self.assertDictEqual(ext_dset1_attrs[2], self.dset1_attributes[1])
        self.assertEqual(ext.data_type_def, 'SpikeData')
        self.assertEqual(ext.data_type_inc, 'EphysData')

        ext_dset2 = ext.get_dataset('dataset2')
        self.maxDiff = None
        # this will suffice for now,  assertDictEqual doesn't do deep equality checks
        self.assertEqual(str(ext_dset2), str(self.datasets[1]))
        self.assertAttributesEqual(ext_dset2, self.datasets[1])

        res_attrs = ext.attributes
        self.assertDictEqual(res_attrs[0], ext_attributes[0])
        self.assertDictEqual(res_attrs[1], self.attributes[0])
        self.assertDictEqual(res_attrs[2], self.attributes[1])

        # test that inherited specs are tracked appropriate
        for d in self.datasets:
            with self.subTest(dataset=d.name):
                self.assertTrue(ext.is_inherited_spec(d))
                self.assertFalse(spec.is_inherited_spec(d))

        json.dumps(spec)

    def assertDatasetsEqual(self, spec1, spec2):
        spec1_dsets = spec1.datasets
        spec2_dsets = spec2.datasets
        if len(spec1_dsets) != len(spec2_dsets):
            raise AssertionError('different number of AttributeSpecs')
        else:
            for i in range(len(spec1_dsets)):
                self.assertAttributesEqual(spec1_dsets[i], spec2_dsets[i])

    def assertAttributesEqual(self, spec1, spec2):
        spec1_attr = spec1.attributes
        spec2_attr = spec2.attributes
        if len(spec1_attr) != len(spec2_attr):
            raise AssertionError('different number of AttributeSpecs')
        else:
            for i in range(len(spec1_attr)):
                self.assertDictEqual(spec1_attr[i], spec2_attr[i])

    def test_add_attribute(self):
        spec = GroupSpec('A test group',
                         name='root_constructor',
                         groups=self.subgroups,
                         datasets=self.datasets,
                         linkable=False)
        for attrspec in self.attributes:
            spec.add_attribute(**attrspec)
        self.assertListEqual(spec['attributes'], self.attributes)
        self.assertListEqual(spec['datasets'], self.datasets)
        self.assertNotIn('data_type_def', spec)
        self.assertIs(spec, self.subgroups[0].parent)
        self.assertIs(spec, self.subgroups[1].parent)
        self.assertIs(spec, spec.attributes[0].parent)
        self.assertIs(spec, spec.attributes[1].parent)
        self.assertIs(spec, self.datasets[0].parent)
        self.assertIs(spec, self.datasets[1].parent)
        json.dumps(spec)

    def test_update_attribute_spec(self):
        spec = GroupSpec('A test group',
                         name='root_constructor',
                         attributes=[AttributeSpec('attribute1', 'my first attribute', 'text'),
                                     AttributeSpec('attribute2', 'my second attribute', 'text')])
        spec.set_attribute(AttributeSpec('attribute2', 'my second attribute', 'int', value=5))
        res = spec.get_attribute('attribute2')
        self.assertEqual(res.value, 5)
        self.assertEqual(res.dtype, 'int')

    def test_path(self):
        GroupSpec('A test group',
                  name='root_constructor',
                  groups=self.subgroups,
                  datasets=self.datasets,
                  attributes=self.attributes,
                  linkable=False)
        self.assertEqual(self.attributes[0].path, 'root_constructor/attribute1')
        self.assertEqual(self.datasets[0].path, 'root_constructor/dataset1')
        self.assertEqual(self.subgroups[0].path, 'root_constructor/subgroup1')

    def test_path_complicated(self):
        attribute = AttributeSpec('attribute1', 'my fifth attribute', 'text')
        dataset = DatasetSpec('my first dataset',
                              'int',
                              name='dataset1',
                              attributes=[attribute])
        subgroup = GroupSpec('A subgroup',
                             name='subgroup1',
                             datasets=[dataset])
        self.assertEqual(attribute.path, 'subgroup1/dataset1/attribute1')

        _ = GroupSpec('A test group',
                      name='root',
                      groups=[subgroup])

        self.assertEqual(attribute.path, 'root/subgroup1/dataset1/attribute1')

    def test_path_no_name(self):
        attribute = AttributeSpec('attribute1', 'my fifth attribute', 'text')
        dataset = DatasetSpec('my first dataset',
                              'int',
                              data_type_inc='DatasetType',
                              attributes=[attribute])
        subgroup = GroupSpec('A subgroup',
                             data_type_def='GroupType',
                             datasets=[dataset])
        _ = GroupSpec('A test group',
                      name='root',
                      groups=[subgroup])

        self.assertEqual(attribute.path, 'root/GroupType/DatasetType/attribute1')

    def test_data_type_property_value(self):
        """Test that the property data_type has the expected value"""
        test_cases = {
            ('Foo', 'Bar'): 'Bar',
            ('Foo', None): 'Foo',
            (None, 'Bar'): 'Bar',
            (None, None): None,
        }
        for (data_type_inc, data_type_def), data_type in test_cases.items():
            with self.subTest(data_type_inc=data_type_inc,
                              data_type_def=data_type_def, data_type=data_type):
                dataset = DatasetSpec('A dataset', 'int', name='dataset',
                                      data_type_inc=data_type_inc, data_type_def=data_type_def)
                self.assertEqual(dataset.data_type, data_type)

    def test_get_data_type_spec(self):
        expected = AttributeSpec('data_type', 'the data type of this object', 'text', value='MyType')
        self.assertDictEqual(GroupSpec.get_data_type_spec('MyType'), expected)

    def test_get_namespace_spec(self):
        expected = AttributeSpec('namespace', 'the namespace for the data type of this object', 'text', required=False)
        self.assertDictEqual(GroupSpec.get_namespace_spec(), expected)


class TestNotAllowedConfig(TestCase):

    def test_no_name_no_def_no_inc(self):
        msg = ("Cannot create Group or Dataset spec with no name without specifying 'data_type_def' "
               "and/or 'data_type_inc'.")
        with self.assertRaisesWith(ValueError, msg):
            GroupSpec('A test group')

    def test_name_with_multiple(self):
        msg = ("Cannot give specific name to something that can exist multiple times: name='MyGroup', quantity='*'")
        with self.assertRaisesWith(ValueError, msg):
            GroupSpec('A test group', name='MyGroup', quantity='*')


class TestResolveAttrs(TestCase):

    def setUp(self):
        self.def_group_spec = GroupSpec(
            doc='A test group',
            name='root',
            data_type_def='MyGroup',
            attributes=[AttributeSpec('attribute1', 'my first attribute', 'text'),
                        AttributeSpec('attribute2', 'my second attribute', 'text')]
        )
        self.inc_group_spec = GroupSpec(
            doc='A test group',
            name='root',
            data_type_inc='MyGroup',
            attributes=[AttributeSpec('attribute2', 'my second attribute', 'text', value='fixed'),
                        AttributeSpec('attribute3', 'my third attribute', 'text', value='fixed')]
        )
        self.inc_group_spec.resolve_spec(self.def_group_spec)

    def test_resolved(self):
        self.assertTupleEqual(self.inc_group_spec.attributes, (
            AttributeSpec('attribute2', 'my second attribute', 'text', value='fixed'),
            AttributeSpec('attribute3', 'my third attribute', 'text', value='fixed'),
            AttributeSpec('attribute1', 'my first attribute', 'text')
        ))

        self.assertEqual(self.inc_group_spec.get_attribute('attribute1'),
                         AttributeSpec('attribute1', 'my first attribute', 'text'))
        self.assertEqual(self.inc_group_spec.get_attribute('attribute2'),
                         AttributeSpec('attribute2', 'my second attribute', 'text', value='fixed'))
        self.assertEqual(self.inc_group_spec.get_attribute('attribute3'),
                         AttributeSpec('attribute3', 'my third attribute', 'text', value='fixed'))

        self.assertTrue(self.inc_group_spec.resolved)

    def test_is_inherited_spec(self):
        self.assertFalse(self.def_group_spec.is_inherited_spec('attribute1'))
        self.assertFalse(self.def_group_spec.is_inherited_spec('attribute2'))
        self.assertTrue(self.inc_group_spec.is_inherited_spec(
            AttributeSpec('attribute1', 'my first attribute', 'text')
        ))
        self.assertTrue(self.inc_group_spec.is_inherited_spec('attribute1'))
        self.assertTrue(self.inc_group_spec.is_inherited_spec('attribute2'))
        self.assertFalse(self.inc_group_spec.is_inherited_spec('attribute3'))
        self.assertFalse(self.inc_group_spec.is_inherited_spec('attribute4'))

    def test_is_overridden_spec(self):
        self.assertFalse(self.def_group_spec.is_overridden_spec('attribute1'))
        self.assertFalse(self.def_group_spec.is_overridden_spec('attribute2'))
        self.assertFalse(self.inc_group_spec.is_overridden_spec(
            AttributeSpec('attribute1', 'my first attribute', 'text')
        ))
        self.assertFalse(self.inc_group_spec.is_overridden_spec('attribute1'))
        self.assertTrue(self.inc_group_spec.is_overridden_spec('attribute2'))
        self.assertFalse(self.inc_group_spec.is_overridden_spec('attribute3'))
        self.assertFalse(self.inc_group_spec.is_overridden_spec('attribute4'))

    def test_is_inherited_attribute(self):
        self.assertFalse(self.def_group_spec.is_inherited_attribute('attribute1'))
        self.assertFalse(self.def_group_spec.is_inherited_attribute('attribute2'))
        self.assertTrue(self.inc_group_spec.is_inherited_attribute('attribute1'))
        self.assertTrue(self.inc_group_spec.is_inherited_attribute('attribute2'))
        self.assertFalse(self.inc_group_spec.is_inherited_attribute('attribute3'))
        with self.assertRaisesWith(ValueError, "Attribute 'attribute4' not found"):
            self.inc_group_spec.is_inherited_attribute('attribute4')

    def test_is_overridden_attribute(self):
        self.assertFalse(self.def_group_spec.is_overridden_attribute('attribute1'))
        self.assertFalse(self.def_group_spec.is_overridden_attribute('attribute2'))
        self.assertFalse(self.inc_group_spec.is_overridden_attribute('attribute1'))
        self.assertTrue(self.inc_group_spec.is_overridden_attribute('attribute2'))
        self.assertFalse(self.inc_group_spec.is_overridden_attribute('attribute3'))
        with self.assertRaisesWith(ValueError, "Attribute 'attribute4' not found"):
            self.inc_group_spec.is_overridden_attribute('attribute4')


class GroupSpecWithLinksTest(TestCase):

    def test_constructor(self):
        link0 = LinkSpec(doc='Link 0', target_type='TargetType0')
        link1 = LinkSpec(doc='Link 1', target_type='TargetType1')
        links = [link0, link1]
        spec = GroupSpec(
            doc='A test group',
            name='root',
            links=links
        )
        self.assertIs(spec, links[0].parent)
        self.assertIs(spec, links[1].parent)
        json.dumps(spec)

    def test_extension_no_overwrite(self):
        link0 = LinkSpec(doc='Link 0', target_type='TargetType0')  # test unnamed
        link1 = LinkSpec(doc='Link 1', target_type='TargetType1', name='MyType1')  # test named
        link2 = LinkSpec(doc='Link 2', target_type='TargetType2', quantity='*')  # test named, multiple
        links = [link0, link1, link2]
        parent_spec = GroupSpec(
            doc='A test group',
            name='parent',
            links=links,
            data_type_def='ParentType'
        )
        child_spec = GroupSpec(
            doc='A test group',
            name='child',
            data_type_inc=parent_spec,
            data_type_def='ChildType'
        )

        for link in links:
            with self.subTest(link_target_type=link.target_type):
                self.assertTrue(child_spec.is_inherited_spec(link))
                self.assertFalse(child_spec.is_overridden_spec(link))

    def test_extension_overwrite(self):
        link0 = LinkSpec(doc='Link 0', target_type='TargetType0', name='MyType0')
        link1 = LinkSpec(doc='Link 1', target_type='TargetType1', name='MyType1')
        # NOTE overwriting unnamed LinkSpec is not allowed
        # NOTE overwriting spec with quantity that could be >1 is not allowed
        links = [link0, link1]
        parent_spec = GroupSpec(
            doc='A test group',
            name='parent',
            links=links,
            data_type_def='ParentType'
        )

        link0_overwrite = LinkSpec(doc='New link 0', target_type='TargetType0', name='MyType0')
        link1_overwrite = LinkSpec(doc='New link 1', target_type='TargetType1Child', name='MyType1')
        overwritten_links = [link0_overwrite, link1_overwrite]
        child_spec = GroupSpec(
            doc='A test group',
            name='child',
            links=overwritten_links,
            data_type_inc=parent_spec,
            data_type_def='ChildType'
        )

        for link in overwritten_links:
            with self.subTest(link_target_type=link.target_type):
                self.assertTrue(child_spec.is_inherited_spec(link))
                self.assertTrue(child_spec.is_overridden_spec(link))


class SpecWithDupsTest(TestCase):

    def test_two_unnamed_group_same_type(self):
        """Test creating a group contains multiple unnamed groups with type X."""
        child0 = GroupSpec(doc='Group 0', data_type_inc='Type0')
        child1 = GroupSpec(doc='Group 1', data_type_inc='Type0')
        msg = "Cannot have multiple groups/datasets with the same data type without specifying name"
        with self.assertRaisesWith(ValueError, msg):
            GroupSpec(
                doc='A test group',
                name='parent',
                groups=[child0, child1],
                data_type_def='ParentType'
            )

    def test_named_unnamed_group_with_def_same_type(self):
        """Test get_data_type when a group contains both a named and unnamed group with type X."""
        child0 = GroupSpec(doc='Group 0', data_type_def='Type0', name='type0')
        child1 = GroupSpec(doc='Group 1', data_type_inc='Type0')
        parent_spec = GroupSpec(
            doc='A test group',
            name='parent',
            groups=[child0, child1],
            data_type_def='ParentType'
        )
        self.assertIs(parent_spec.get_data_type('Type0'), child1)

    def test_named_unnamed_group_same_type(self):
        """Test get_data_type when a group contains both a named and unnamed group with type X."""
        child0 = GroupSpec(doc='Group 0', data_type_inc='Type0', name='type0')
        child1 = GroupSpec(doc='Group 1', data_type_inc='Type0', name='type1')
        child2 = GroupSpec(doc='Group 2', data_type_inc='Type0')
        parent_spec = GroupSpec(
            doc='A test group',
            name='parent',
            groups=[child0, child1, child2],
            data_type_def='ParentType'
        )
        self.assertIs(parent_spec.get_data_type('Type0'), child2)

    def test_unnamed_named_group_same_type(self):
        """Test get_data_type when a group contains both an unnamed and named group with type X."""
        child0 = GroupSpec(doc='Group 0', data_type_inc='Type0')
        child1 = GroupSpec(doc='Group 1', data_type_inc='Type0', name='type1')
        parent_spec = GroupSpec(
            doc='A test group',
            name='parent',
            groups=[child0, child1],
            data_type_def='ParentType'
        )
        self.assertIs(parent_spec.get_data_type('Type0'), child0)

    def test_two_named_group_same_type(self):
        """Test get_data_type when a group contains multiple named groups with type X."""
        child0 = GroupSpec(doc='Group 0', data_type_inc='Type0', name='group0')
        child1 = GroupSpec(doc='Group 1', data_type_inc='Type0', name='group1')
        parent_spec = GroupSpec(
            doc='A test group',
            name='parent',
            groups=[child0, child1],
            data_type_def='ParentType'
        )
        self.assertEqual(parent_spec.get_data_type('Type0'), [child0, child1])

    def test_two_unnamed_datasets_same_type(self):
        """Test creating a group contains multiple unnamed datasets with type X."""
        child0 = DatasetSpec(doc='Group 0', data_type_inc='Type0')
        child1 = DatasetSpec(doc='Group 1', data_type_inc='Type0')
        msg = "Cannot have multiple groups/datasets with the same data type without specifying name"
        with self.assertRaisesWith(ValueError, msg):
            GroupSpec(
                doc='A test group',
                name='parent',
                datasets=[child0, child1],
                data_type_def='ParentType'
            )

    def test_named_unnamed_dataset_with_def_same_type(self):
        """Test get_data_type when a group contains both a named and unnamed dataset with type X."""
        child0 = DatasetSpec(doc='Group 0', data_type_def='Type0', name='type0')
        child1 = DatasetSpec(doc='Group 1', data_type_inc='Type0')
        parent_spec = GroupSpec(
            doc='A test group',
            name='parent',
            datasets=[child0, child1],
            data_type_def='ParentType'
        )
        self.assertIs(parent_spec.get_data_type('Type0'), child1)

    def test_named_unnamed_dataset_same_type(self):
        """Test get_data_type when a group contains both a named and unnamed dataset with type X."""
        child0 = DatasetSpec(doc='Group 0', data_type_inc='Type0', name='type0')
        child1 = DatasetSpec(doc='Group 1', data_type_inc='Type0')
        parent_spec = GroupSpec(
            doc='A test group',
            name='parent',
            datasets=[child0, child1],
            data_type_def='ParentType'
        )
        self.assertIs(parent_spec.get_data_type('Type0'), child1)

    def test_two_named_unnamed_dataset_same_type(self):
        """Test get_data_type when a group contains two named and one unnamed dataset with type X."""
        child0 = DatasetSpec(doc='Group 0', data_type_inc='Type0', name='type0')
        child1 = DatasetSpec(doc='Group 1', data_type_inc='Type0', name='type1')
        child2 = DatasetSpec(doc='Group 2', data_type_inc='Type0')
        parent_spec = GroupSpec(
            doc='A test group',
            name='parent',
            datasets=[child0, child1, child2],
            data_type_def='ParentType'
        )
        self.assertIs(parent_spec.get_data_type('Type0'), child2)

    def test_unnamed_named_dataset_same_type(self):
        """Test get_data_type when a group contains both an unnamed and named dataset with type X."""
        child0 = DatasetSpec(doc='Group 0', data_type_inc='Type0')
        child1 = DatasetSpec(doc='Group 1', data_type_inc='Type0', name='type1')
        parent_spec = GroupSpec(
            doc='A test group',
            name='parent',
            datasets=[child0, child1],
            data_type_def='ParentType'
        )
        self.assertIs(parent_spec.get_data_type('Type0'), child0)

    def test_two_named_datasets_same_type(self):
        """Test get_data_type when a group contains multiple named datasets with type X."""
        child0 = DatasetSpec(doc='Group 0', data_type_inc='Type0', name='group0')
        child1 = DatasetSpec(doc='Group 1', data_type_inc='Type0', name='group1')
        parent_spec = GroupSpec(
            doc='A test group',
            name='parent',
            datasets=[child0, child1],
            data_type_def='ParentType'
        )
        self.assertEqual(parent_spec.get_data_type('Type0'), [child0, child1])

    def test_three_named_datasets_same_type(self):
        """Test get_target_type when a group contains three named links with type X."""
        child0 = DatasetSpec(doc='Group 0', data_type_inc='Type0', name='group0')
        child1 = DatasetSpec(doc='Group 1', data_type_inc='Type0', name='group1')
        child2 = DatasetSpec(doc='Group 2', data_type_inc='Type0', name='group2')
        parent_spec = GroupSpec(
            doc='A test group',
            name='parent',
            datasets=[child0, child1, child2],
            data_type_def='ParentType'
        )
        self.assertEqual(parent_spec.get_data_type('Type0'), [child0, child1, child2])

    def test_two_unnamed_links_same_type(self):
        """Test creating a group contains multiple unnamed links with type X."""
        child0 = LinkSpec(doc='Group 0', target_type='Type0')
        child1 = LinkSpec(doc='Group 1', target_type='Type0')
        msg = "Cannot have multiple links with the same target type without specifying name"
        with self.assertRaisesWith(ValueError, msg):
            GroupSpec(
                doc='A test group',
                name='parent',
                links=[child0, child1],
                data_type_def='ParentType'
            )

    def test_named_unnamed_link_same_type(self):
        """Test get_target_type when a group contains both a named and unnamed link with type X."""
        child0 = LinkSpec(doc='Group 0', target_type='Type0', name='type0')
        child1 = LinkSpec(doc='Group 1', target_type='Type0')
        parent_spec = GroupSpec(
            doc='A test group',
            name='parent',
            links=[child0, child1],
            data_type_def='ParentType'
        )
        self.assertIs(parent_spec.get_target_type('Type0'), child1)

    def test_two_named_unnamed_link_same_type(self):
        """Test get_target_type when a group contains two named and one unnamed link with type X."""
        child0 = LinkSpec(doc='Group 0', target_type='Type0', name='type0')
        child1 = LinkSpec(doc='Group 1', target_type='Type0', name='type1')
        child2 = LinkSpec(doc='Group 2', target_type='Type0')
        parent_spec = GroupSpec(
            doc='A test group',
            name='parent',
            links=[child0, child1, child2],
            data_type_def='ParentType'
        )
        self.assertIs(parent_spec.get_target_type('Type0'), child2)

    def test_unnamed_named_link_same_type(self):
        """Test get_target_type when a group contains both an unnamed and named link with type X."""
        child0 = LinkSpec(doc='Group 0', target_type='Type0')
        child1 = LinkSpec(doc='Group 1', target_type='Type0', name='type1')
        parent_spec = GroupSpec(
            doc='A test group',
            name='parent',
            links=[child0, child1],
            data_type_def='ParentType'
        )
        self.assertIs(parent_spec.get_target_type('Type0'), child0)

    def test_two_named_links_same_type(self):
        """Test get_target_type when a group contains multiple named links with type X."""
        child0 = LinkSpec(doc='Group 0', target_type='Type0', name='group0')
        child1 = LinkSpec(doc='Group 1', target_type='Type0', name='group1')
        parent_spec = GroupSpec(
            doc='A test group',
            name='parent',
            links=[child0, child1],
            data_type_def='ParentType'
        )
        self.assertEqual(parent_spec.get_target_type('Type0'), [child0, child1])

    def test_three_named_links_same_type(self):
        """Test get_target_type when a group contains three named links with type X."""
        child0 = LinkSpec(doc='Group 0', target_type='Type0', name='type0')
        child1 = LinkSpec(doc='Group 1', target_type='Type0', name='type1')
        child2 = LinkSpec(doc='Group 2', target_type='Type0', name='type2')
        parent_spec = GroupSpec(
            doc='A test group',
            name='parent',
            links=[child0, child1, child2],
            data_type_def='ParentType'
        )
        self.assertEqual(parent_spec.get_target_type('Type0'), [child0, child1, child2])


class SpecWithGroupsLinksTest(TestCase):

    def test_unnamed_group_link_same_type(self):
        child = GroupSpec(doc='Group 0', data_type_inc='Type0')
        link = LinkSpec(doc='Link 0', target_type='Type0')
        parent_spec = GroupSpec(
            doc='A test group',
            name='parent',
            groups=[child],
            links=[link],
            data_type_def='ParentType'
        )
        self.assertIs(parent_spec.get_data_type('Type0'), child)
        self.assertIs(parent_spec.get_target_type('Type0'), link)

    def test_unnamed_dataset_link_same_type(self):
        child = DatasetSpec(doc='Dataset 0', data_type_inc='Type0')
        link = LinkSpec(doc='Link 0', target_type='Type0')
        parent_spec = GroupSpec(
            doc='A test group',
            name='parent',
            datasets=[child],
            links=[link],
            data_type_def='ParentType'
        )
        self.assertIs(parent_spec.get_data_type('Type0'), child)
        self.assertIs(parent_spec.get_target_type('Type0'), link)
