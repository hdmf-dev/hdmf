from pydantic import ValidationError

from hdmf.spec import GroupSpec, DatasetSpec, AttributeSpec, LinkSpec
from hdmf.testing import TestCase


class GroupSpecTests(TestCase):
    def setUp(self):
        self.attributes = [
            AttributeSpec(name='attribute1', dtype='text', doc='my first attribute'),
            AttributeSpec(name='attribute2', dtype='text', doc='my second attribute')
        ]

        self.dset1_attributes = [
            AttributeSpec(name='attribute3', dtype='text', doc='my third attribute'),
            AttributeSpec(name='attribute4', dtype='text', doc='my fourth attribute')
        ]

        self.dset2_attributes = [
            AttributeSpec(name='attribute5', dtype='text', doc='my fifth attribute'),
            AttributeSpec(name='attribute6', dtype='text', doc='my sixth attribute')
        ]

        self.datasets = [
            DatasetSpec(
                name='dataset1',
                dtype='int',
                doc='my first dataset',
                attributes=self.dset1_attributes,
            ),
            DatasetSpec(
                data_type_def='VoltageArray',
                name='dataset2',
                dtype='int',
                doc='my second dataset',
                attributes=self.dset2_attributes,
            )
        ]

        self.subgroups = [
            GroupSpec(name='subgroup1', doc='A test subgroup'),
            GroupSpec(name='subgroup2', doc='Another test subgroup'),
        ]

    def test_constructor(self):
        spec = GroupSpec(
            name='root_constructor',
            doc='A test group',
            attributes=self.attributes,
            datasets=self.datasets,
            groups=self.subgroups,
        )
        self.assertListEqual(spec.attributes, self.attributes)
        self.assertListEqual(spec.datasets, self.datasets)
        self.assertListEqual(spec.groups, self.subgroups)
        self.assertIs(spec, self.subgroups[0].parent)
        self.assertIs(spec, self.subgroups[1].parent)
        self.assertIs(spec, self.attributes[0].parent)
        self.assertIs(spec, self.attributes[1].parent)
        self.assertIs(spec, self.datasets[0].parent)
        self.assertIs(spec, self.datasets[1].parent)
        self.maxDiff = None
        spec_dict = spec.model_dump(exclude_unset=True)
        expected = {
            'name': 'root_constructor',
            'doc': 'A test group',
            'groups': [
                {
                    'name': 'subgroup1',
                    'doc': 'A test subgroup',
                },
                {
                    'name': 'subgroup2',
                    'doc': 'Another test subgroup',
                }
            ],
            'datasets': [
                {
                    'name': 'dataset1',
                    'doc': 'my first dataset',
                    'dtype': 'int',
                    'attributes': [
                        {
                            'name': 'attribute3',
                            'doc': 'my third attribute',
                            'dtype': 'text',
                        },
                        {
                            'name': 'attribute4',
                            'dtype': 'text',
                            'doc': 'my fourth attribute',
                        }
                    ],
                },
                {
                    'name': 'dataset2',
                    'dtype': 'int',
                    'doc': 'my second dataset',
                    'attributes': [
                        {
                            'name': 'attribute5',
                            'dtype': 'text',
                            'doc': 'my fifth attribute',
                        },
                        {
                            'name': 'attribute6',
                            'dtype': 'text',
                            'doc': 'my sixth attribute',
                        }
                    ],
                    'data_type_def': 'VoltageArray'
                }
            ],
            'attributes': [
                {
                    'name': 'attribute1',
                    'dtype': 'text',
                    'doc': 'my first attribute',
                },
                {
                    'name': 'attribute2',
                    'dtype': 'text',
                    'doc': 'my second attribute',
                }
            ],
        }
        self.assertDictEqual(spec_dict, expected)

    def test_constructor_datatype(self):
        spec = GroupSpec(
            data_type_def='EphysData',
            name='root_constructor_datatype',
            doc='A test group',
            attributes=self.attributes,
            datasets=self.datasets,
        )
        self.assertListEqual(spec.attributes, self.attributes)
        self.assertListEqual(spec.datasets, self.datasets)
        self.assertEqual(spec.data_type_def, 'EphysData')
        self.assertIs(spec, self.attributes[0].parent)
        self.assertIs(spec, self.attributes[1].parent)
        self.assertIs(spec, self.datasets[0].parent)
        self.assertIs(spec, self.datasets[1].parent)
        self.assertEqual(spec.data_type_def, 'EphysData')
        self.assertIsNone(spec.data_type_inc)
        spec_dict = spec.model_dump(exclude_unset=True)
        self.assertEqual(spec_dict.get('data_type_def'), 'EphysData')

    def test_set_parent_exists(self):
        GroupSpec(doc='A test group', name='root_constructor', groups=self.subgroups)
        with self.assertRaisesWith(ValueError, 'Parent cannot be changed after being set.'):
            self.subgroups[0].parent = self.subgroups[1]

    def test_set_dataset(self):
        spec = GroupSpec(
            data_type_def='EphysData',
            name='root_test_set_dataset',
            doc='A test group',
        )
        spec.set_dataset(self.datasets[0])
        self.assertIs(spec, self.datasets[0].parent)

    def test_set_link(self):
        group = GroupSpec(
            name='root',
            doc='A test group',
        )
        link = LinkSpec(
            name='link_name',
            target_type='LinkTarget',
            doc='A test link',
        )
        group.set_link(link)
        self.assertIs(group, link.parent)
        self.assertIs(group.get_link('link_name'), link)

    def test_set_group(self):
        spec = GroupSpec(
            data_type_def='EphysData',
            name='root_test_set_group',
            doc='A test group',
        )
        spec.set_group(self.subgroups[0])
        spec.set_group(self.subgroups[1])
        self.assertListEqual(spec.groups, self.subgroups)
        self.assertIs(spec, self.subgroups[0].parent)
        self.assertIs(spec, self.subgroups[1].parent)
        spec_dict = spec.model_dump(exclude_unset=True)
        expected = {
            'data_type_def': 'EphysData',
            'name': 'root_test_set_group',
            'doc': 'A test group',
            'groups': [
                {
                    'name': 'subgroup1',
                    'doc': 'A test subgroup',
                },
                {
                    'name': 'subgroup2',
                    'doc': 'Another test subgroup',
                }
            ],
        }
        self.assertDictEqual(spec_dict, expected)


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

    def test_update_attribute_spec(self):
        spec = GroupSpec(
            name='root_constructor',
            doc='A test group',
            attributes=[
                AttributeSpec(name='attribute1', dtype='text', doc='my first attribute'),
                AttributeSpec(name='attribute2', dtype='text', doc='my second attribute'),
            ],
        )
        with self.assertRaisesWith(ValueError, "Attribute 'attribute2' already exists in spec 'root_constructor'."):
            spec.set_attribute(AttributeSpec(name='attribute2', dtype='int', value=5, doc='my second attribute'))

    def test_path(self):
        GroupSpec(
            name='root_constructor',
            doc='A test group',
            attributes=self.attributes,
            datasets=self.datasets,
            groups=self.subgroups,
        )
        self.assertEqual(self.attributes[0].path, 'root_constructor/attribute1')
        self.assertEqual(self.datasets[0].path, 'root_constructor/dataset1')
        self.assertEqual(self.subgroups[0].path, 'root_constructor/subgroup1')

    def test_path_complicated(self):
        attribute = AttributeSpec(name='attribute1', dtype='text', doc='my fifth attribute')
        dataset = DatasetSpec(name='dataset1',
                              dtype='int',
                              doc='my first dataset',
                              attributes=[attribute])
        subgroup = GroupSpec(name='subgroup1',
                             doc='A subgroup',
                             datasets=[dataset])
        self.assertEqual(attribute.path, 'subgroup1/dataset1/attribute1')

        _ = GroupSpec(name='root',
                      doc='A test group',
                      groups=[subgroup])

        self.assertEqual(attribute.path, 'root/subgroup1/dataset1/attribute1')

    def test_path_no_name(self):
        attribute = AttributeSpec(name='attribute1', dtype='text', doc='my fifth attribute')
        dataset = DatasetSpec(data_type_inc='DatasetType',
                              dtype='int',
                              doc='my first dataset',
                              attributes=[attribute])
        subgroup = GroupSpec(data_type_def='GroupType',
                             doc='A subgroup',
                             datasets=[dataset])
        _ = GroupSpec(name='root',
                      doc='A test group',
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
                dataset = DatasetSpec(data_type_def=data_type_def, data_type_inc=data_type_inc,
                                      name='dataset', dtype='int', doc='A dataset')
                self.assertEqual(dataset.data_type, data_type)

    def test_build_warn_extra_args(self):
        spec_dict = {
            'name': 'group1',
            'doc': 'test group',
            'required': True,
        }
        with self.assertRaisesRegex(ValidationError, r'required\s*Extra inputs are not permitted'):
            GroupSpec.build_spec(spec_dict)


class TestNotAllowedConfig(TestCase):

    def test_no_name_no_def_no_inc(self):
        msg = r"At least one of 'name', 'data_type_def', or 'data_type_inc' must be specified\."
        with self.assertRaisesRegex(ValidationError, msg):
            GroupSpec(doc='A test group')

    def test_name_with_multiple(self):
        msg = r"Cannot specify 'name' on a spec that can exist multiple times\."
        with self.assertRaisesRegex(ValidationError, msg):
            GroupSpec(name='MyGroup', doc='A test group', quantity='*')

    def test_same_data_type_def_inc(self):
        msg = r"Cannot specify the same value for 'data_type_def' and 'data_type_inc': MyType"
        with self.assertRaisesRegex(ValueError, msg):
            GroupSpec(data_type_def='MyType', data_type_inc='MyType', doc='A test group')



class GroupSpecWithLinksTest(TestCase):

    def test_constructor(self):
        link0 = LinkSpec(target_type='TargetType0', doc='Link 0')
        link1 = LinkSpec(target_type='TargetType1', doc='Link 1')
        links = [link0, link1]
        spec = GroupSpec(
            name='root',
            doc='A test group',
            links=links
        )
        self.assertIs(spec, links[0].parent)
        self.assertIs(spec, links[1].parent)
        spec_dict = spec.model_dump(exclude_unset=True)
        expected = {
            'name': 'root',
            'doc': 'A test group',
            'links': [
                {
                    'target_type': 'TargetType0',
                    'doc': 'Link 0',
                },
                {
                    'target_type': 'TargetType1',
                    'doc': 'Link 1',
                }
            ],
        }
        self.assertDictEqual(spec_dict, expected)


class SpecWithDupsTest(TestCase):

    def test_two_unnamed_groups_same_type(self):
        """Test creating a group contains multiple unnamed groups with type X."""
        child0 = GroupSpec(data_type_inc='Type0', doc='Group 0')
        child1 = GroupSpec(data_type_inc='Type0', doc='Group 1')
        msg = r"Duplicate data_type: 'Type0' is used in multiple unnamed subspecs in group 'parent'\."
        with self.assertRaisesRegex(ValidationError, msg):
            GroupSpec(
                data_type_def='ParentType',
                name='parent',
                doc='A test group',
                groups=[child0, child1],
            )

    def test_named_unnamed_groups_with_def_same_type(self):
        """Test get_data_type when a group contains both a named and unnamed group with type X."""
        child0 = GroupSpec(data_type_def='Type0', name='type0', doc='Group 0')
        child1 = GroupSpec(data_type_inc='Type0', doc='Group 1')
        parent_spec = GroupSpec(
            data_type_def='ParentType',
            name='parent',
            doc='A test group',
            groups=[child0, child1],
        )
        self.assertIs(parent_spec.get_data_type('Type0'), child1)

    def test_named_unnamed_groups_same_type(self):
        """Test get_data_type when a group contains both a named and unnamed group with type X."""
        child0 = GroupSpec(data_type_inc='Type0', name='type0', doc='Group 0')
        child1 = GroupSpec(data_type_inc='Type0', name='type1', doc='Group 1')
        child2 = GroupSpec(data_type_inc='Type0', doc='Group 2')
        parent_spec = GroupSpec(
            data_type_def='ParentType',
            name='parent',
            doc='A test group',
            groups=[child0, child1, child2],
        )
        self.assertIs(parent_spec.get_data_type('Type0'), child2)

    def test_unnamed_named_groups_same_type(self):
        """Test get_data_type when a group contains both an unnamed and named group with type X."""
        child0 = GroupSpec(data_type_inc='Type0', doc='Group 0')
        child1 = GroupSpec(data_type_inc='Type0', name='type1', doc='Group 1')
        parent_spec = GroupSpec(
            data_type_def='ParentType',
            name='parent',
            doc='A test group',
            groups=[child0, child1],
        )
        self.assertIs(parent_spec.get_data_type('Type0'), child0)

    def test_two_named_groups_same_type(self):
        """Test get_data_type when a group contains multiple named groups with type X."""
        child0 = GroupSpec(data_type_inc='Type0', name='group0', doc='Group 0')
        child1 = GroupSpec(data_type_inc='Type0', name='group1', doc='Group 1')
        parent_spec = GroupSpec(
            data_type_def='ParentType',
            name='parent',
            doc='A test group',
            groups=[child0, child1],
        )
        self.assertEqual(parent_spec.get_data_type('Type0'), [child0, child1])

    def test_two_unnamed_datasets_same_type(self):
        """Test creating a group contains multiple unnamed datasets with type X."""
        child0 = DatasetSpec(data_type_inc='Type0', doc='Group 0')
        child1 = DatasetSpec(data_type_inc='Type0', doc='Group 1')
        msg = r"Duplicate data_type: 'Type0' is used in multiple unnamed subspecs in group 'parent'\."
        with self.assertRaisesRegex(ValidationError, msg):
            GroupSpec(
                data_type_def='ParentType',
                name='parent',
                doc='A test group',
                datasets=[child0, child1],
            )

    def test_named_unnamed_datasets_with_def_same_type(self):
        """Test get_data_type when a group contains both a named and unnamed dataset with type X."""
        child0 = DatasetSpec(data_type_def='Type0', name='type0', doc='Group 0')
        child1 = DatasetSpec(data_type_inc='Type0', doc='Group 1')
        parent_spec = GroupSpec(
            data_type_def='ParentType',
            name='parent',
            doc='A test group',
            datasets=[child0, child1],
        )
        self.assertIs(parent_spec.get_data_type('Type0'), child1)

    def test_named_unnamed_dataset_same_type(self):
        """Test get_data_type when a group contains both a named and unnamed dataset with type X."""
        child0 = DatasetSpec(data_type_inc='Type0', name='type0', doc='Group 0')
        child1 = DatasetSpec(data_type_inc='Type0', doc='Group 1')
        parent_spec = GroupSpec(
            data_type_def='ParentType',
            name='parent',
            doc='A test group',
            datasets=[child0, child1],
        )
        self.assertIs(parent_spec.get_data_type('Type0'), child1)

    def test_two_named_unnamed_dataset_same_type(self):
        """Test get_data_type when a group contains two named and one unnamed dataset with type X."""
        child0 = DatasetSpec(data_type_inc='Type0', name='type0', doc='Group 0')
        child1 = DatasetSpec(data_type_inc='Type0', name='type1', doc='Group 1')
        child2 = DatasetSpec(data_type_inc='Type0', doc='Group 2')
        parent_spec = GroupSpec(
            data_type_def='ParentType',
            name='parent',
            doc='A test group',
            datasets=[child0, child1, child2],
        )
        self.assertIs(parent_spec.get_data_type('Type0'), child2)

    def test_unnamed_named_dataset_same_type(self):
        """Test get_data_type when a group contains both an unnamed and named dataset with type X."""
        child0 = DatasetSpec(data_type_inc='Type0', doc='Group 0')
        child1 = DatasetSpec(data_type_inc='Type0', name='type1', doc='Group 1')
        parent_spec = GroupSpec(
            data_type_def='ParentType',
            name='parent',
            doc='A test group',
            datasets=[child0, child1],
        )
        self.assertIs(parent_spec.get_data_type('Type0'), child0)

    def test_two_named_datasets_same_type(self):
        """Test get_data_type when a group contains multiple named datasets with type X."""
        child0 = DatasetSpec(data_type_inc='Type0', name='group0', doc='Group 0')
        child1 = DatasetSpec(data_type_inc='Type0', name='group1', doc='Group 1')
        parent_spec = GroupSpec(
            data_type_def='ParentType',
            name='parent',
            doc='A test group',
            datasets=[child0, child1],
        )
        self.assertEqual(parent_spec.get_data_type('Type0'), [child0, child1])

    def test_three_named_datasets_same_type(self):
        """Test get_target_type when a group contains three named links with type X."""
        child0 = DatasetSpec(data_type_inc='Type0', name='group0', doc='Group 0')
        child1 = DatasetSpec(data_type_inc='Type0', name='group1', doc='Group 1')
        child2 = DatasetSpec(data_type_inc='Type0', name='group2', doc='Group 2')
        parent_spec = GroupSpec(
            data_type_def='ParentType',
            name='parent',
            doc='A test group',
            datasets=[child0, child1, child2],
        )
        self.assertEqual(parent_spec.get_data_type('Type0'), [child0, child1, child2])

    def test_two_unnamed_links_same_type(self):
        """Test creating a group contains multiple unnamed links with type X."""
        child0 = LinkSpec(target_type='Type0', doc='Group 0')
        child1 = LinkSpec(target_type='Type0', doc='Group 1')
        msg = r"Duplicate target_type: 'Type0' is used in multiple unnamed links in group 'parent'\."
        with self.assertRaisesRegex(ValueError, msg):
            GroupSpec(
                data_type_def='ParentType',
                name='parent',
                doc='A test group',
                links=[child0, child1],
            )

    def test_named_unnamed_link_same_type(self):
        """Test get_target_type when a group contains both a named and unnamed link with type X."""
        child0 = LinkSpec(name='type0', target_type='Type0', doc='Group 0')
        child1 = LinkSpec(target_type='Type0', doc='Group 1')
        parent_spec = GroupSpec(
            data_type_def='ParentType',
            name='parent',
            doc='A test group',
            links=[child0, child1],
        )
        self.assertIs(parent_spec.get_target_type('Type0'), child1)

    def test_two_named_unnamed_link_same_type(self):
        """Test get_target_type when a group contains two named and one unnamed link with type X."""
        child0 = LinkSpec(name='type0', target_type='Type0', doc='Group 0')
        child1 = LinkSpec(name='type1', target_type='Type0', doc='Group 1')
        child2 = LinkSpec(target_type='Type0', doc='Group 2')
        parent_spec = GroupSpec(
            data_type_def='ParentType',
            name='parent',
            doc='A test group',
            links=[child0, child1, child2],
        )
        self.assertIs(parent_spec.get_target_type('Type0'), child2)

    def test_unnamed_named_link_same_type(self):
        """Test get_target_type when a group contains both an unnamed and named link with type X."""
        child0 = LinkSpec(target_type='Type0', doc='Group 0')
        child1 = LinkSpec(name='type1', target_type='Type0', doc='Group 1')
        parent_spec = GroupSpec(
            data_type_def='ParentType',
            name='parent',
            doc='A test group',
            links=[child0, child1],
        )
        self.assertIs(parent_spec.get_target_type('Type0'), child0)

    def test_two_named_links_same_type(self):
        """Test get_target_type when a group contains multiple named links with type X."""
        child0 = LinkSpec(name='group0', target_type='Type0', doc='Group 0')
        child1 = LinkSpec(name='group1', target_type='Type0', doc='Group 1')
        parent_spec = GroupSpec(
            data_type_def='ParentType',
            name='parent',
            doc='A test group',
            links=[child0, child1],
        )
        self.assertEqual(parent_spec.get_target_type('Type0'), [child0, child1])

    def test_three_named_links_same_type(self):
        """Test get_target_type when a group contains three named links with type X."""
        child0 = LinkSpec(name='type0', target_type='Type0', doc='Group 0')
        child1 = LinkSpec(name='type1', target_type='Type0', doc='Group 1')
        child2 = LinkSpec(name='type2', target_type='Type0', doc='Group 2')
        parent_spec = GroupSpec(
            data_type_def='ParentType',
            name='parent',
            doc='A test group',
            links=[child0, child1, child2],
        )
        self.assertEqual(parent_spec.get_target_type('Type0'), [child0, child1, child2])


class SpecWithGroupsLinksTest(TestCase):

    def test_unnamed_group_link_same_type(self):
        child = GroupSpec(data_type_inc='Type0', doc='Group 0')
        link = LinkSpec(target_type='Type0', doc='Link 0')
        parent_spec = GroupSpec(
            data_type_def='ParentType',
            name='parent',
            doc='A test group',
            groups=[child],
            links=[link],
        )
        self.assertIs(parent_spec.get_data_type('Type0'), child)
        self.assertIs(parent_spec.get_target_type('Type0'), link)

    def test_unnamed_dataset_link_same_type(self):
        child = DatasetSpec(data_type_inc='Type0', doc='Dataset 0')
        link = LinkSpec(target_type='Type0', doc='Link 0')
        parent_spec = GroupSpec(
            data_type_def='ParentType',
            name='parent',
            doc='A test group',
            datasets=[child],
            links=[link],
        )
        self.assertIs(parent_spec.get_data_type('Type0'), child)
        self.assertIs(parent_spec.get_target_type('Type0'), link)
