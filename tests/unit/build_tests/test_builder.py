from hdmf.build import GroupBuilder, DatasetBuilder, LinkBuilder, ReferenceBuilder, RegionBuilder
from hdmf.testing import TestCase


class TestGroupBuilder(TestCase):

    def test_constructor(self):
        gb1 = GroupBuilder('gb1', source='source')
        gb2 = GroupBuilder('gb2', parent=gb1)
        self.assertIs(gb1.name, 'gb1')
        self.assertIsNone(gb1.parent)
        self.assertEqual(gb1.source, 'source')
        self.assertIs(gb2.parent, gb1)

    def test_repr(self):
        gb1 = GroupBuilder('gb1')
        expected = "gb1 GroupBuilder {'attributes': {}, 'groups': {}, 'datasets': {}, 'links': {}}"
        self.assertEqual(gb1.__repr__(), expected)

    def test_set_source(self):
        """Test that setting source sets the children builder source."""
        gb1 = GroupBuilder('gb1')
        db = DatasetBuilder('db', list(range(10)))
        lb = LinkBuilder(gb1, 'lb')
        gb2 = GroupBuilder('gb1', {'gb1': gb1}, {'db': db}, {}, {'lb': lb})
        gb2.source = 'source'
        self.assertEqual(gb2.source, 'source')
        self.assertEqual(gb1.source, 'source')
        self.assertEqual(db.source, 'source')
        self.assertEqual(lb.source, 'source')

    def test_set_source_no_reset(self):
        """Test that setting source does not set the children builder source if children already have a source."""
        gb1 = GroupBuilder('gb1', source='original')
        db = DatasetBuilder('db', list(range(10)), source='original')
        lb = LinkBuilder(gb1, 'lb', source='original')
        gb2 = GroupBuilder('gb1', {'gb1': gb1}, {'db': db}, {}, {'lb': lb})
        gb2.source = 'source'
        self.assertEqual(gb1.source, 'original')
        self.assertEqual(db.source, 'original')
        self.assertEqual(lb.source, 'original')

    def test_constructor_dset_none(self):
        gb1 = GroupBuilder('gb1', datasets={'empty': None})
        self.assertEqual(len(gb1.datasets), 0)

    def test_set_location(self):
        gb1 = GroupBuilder('gb1')
        gb1.location = 'location'
        self.assertEqual(gb1.location, 'location')

    def test_overwrite_location(self):
        gb1 = GroupBuilder('gb1')
        gb1.location = 'location'
        gb1.location = 'new location'
        self.assertEqual(gb1.location, 'new location')


class TestGroupBuilderSetters(TestCase):

    def test_set_attribute(self):
        gb = GroupBuilder('gb')
        gb.set_attribute('key', 'value')
        self.assertIn('key', gb.obj_type)
        self.assertIn('key', gb.attributes)
        self.assertEqual(gb['key'], 'value')

    def test_set_group(self):
        gb1 = GroupBuilder('gb1')
        gb2 = GroupBuilder('gb2')
        gb1.set_group(gb2)
        self.assertIs(gb2.parent, gb1)
        self.assertIn('gb2', gb1.obj_type)
        self.assertIn('gb2', gb1.groups)
        self.assertIs(gb1['gb2'], gb2)

    def test_set_dataset(self):
        gb = GroupBuilder('gb')
        db = DatasetBuilder('db', list(range(10)))
        gb.set_dataset(db)
        self.assertIs(db.parent, gb)
        self.assertIn('db', gb.obj_type)
        self.assertIn('db', gb.datasets)
        self.assertIs(gb['db'], db)

    def test_set_link(self):
        gb1 = GroupBuilder('gb1')
        gb2 = GroupBuilder('gb2')
        lb = LinkBuilder(gb2)
        gb1.set_link(lb)
        self.assertIs(lb.parent, gb1)
        self.assertIn('gb2', gb1.obj_type)
        self.assertIn('gb2', gb1.links)
        self.assertIs(gb1['gb2'], lb)

    def test_setitem_disabled(self):
        """Test __setitem__ is disabled"""
        gb = GroupBuilder('gb')
        with self.assertRaises(NotImplementedError):
            gb['key'] = 'value'

    def test_set_exists_wrong_type(self):
        gb1 = GroupBuilder('gb1')
        gb2 = GroupBuilder('gb2')
        db = DatasetBuilder('gb2')
        gb1.set_group(gb2)
        msg = "'gb2' already exists in gb1.groups, cannot set in datasets."
        with self.assertRaisesWith(ValueError, msg):
            gb1.set_dataset(db)


class TestGroupBuilderGetters(TestCase):

    def setUp(self):
        self.subgroup1 = GroupBuilder('subgroup1')
        self.dataset1 = DatasetBuilder('dataset1', list(range(10)))
        self.link1 = LinkBuilder(self.subgroup1, 'link1')
        self.int_attr = 1
        self.str_attr = "my_str"

        self.group1 = GroupBuilder('group1', {'subgroup1': self.subgroup1})
        self.gb = GroupBuilder(
            name='gb',
            groups={'group1': self.group1},
            datasets={'dataset1': self.dataset1},
            attributes={'int_attr': self.int_attr,
                        'str_attr': self.str_attr},
            links={'link1': self.link1}
        )

    def test_path(self):
        self.assertEqual(self.subgroup1.path, 'gb/group1/subgroup1')
        self.assertEqual(self.dataset1.path, 'gb/dataset1')
        self.assertEqual(self.link1.path, 'gb/link1')
        self.assertEqual(self.group1.path, 'gb/group1')
        self.assertEqual(self.gb.path, 'gb')

    def test_getitem_group(self):
        """Test __getitem__ for groups"""
        self.assertIs(self.gb['group1'], self.group1)

    def test_getitem_group_deeper(self):
        """Test __getitem__ for groups deeper in hierarchy"""
        self.assertIs(self.gb['group1/subgroup1'], self.subgroup1)

    def test_getitem_dataset(self):
        """Test __getitem__ for datasets"""
        self.assertIs(self.gb['dataset1'], self.dataset1)

    def test_getitem_attr(self):
        """Test __getitem__ for attributes"""
        self.assertEqual(self.gb['int_attr'], self.int_attr)
        self.assertEqual(self.gb['str_attr'], self.str_attr)

    def test_getitem_invalid_key(self):
        """Test __getitem__ for invalid key"""
        with self.assertRaises(KeyError):
            self.gb['invalid_key']

    def test_getitem_invalid_key_deeper(self):
        """Test __getitem__ for invalid key"""
        with self.assertRaises(KeyError):
            self.gb['group/invalid_key']

    def test_getitem_link(self):
        """Test __getitem__ for links"""
        self.assertIs(self.gb['link1'], self.link1)

    def test_get_group(self):
        """Test get() for groups"""
        self.assertIs(self.gb.get('group1'), self.group1)

    def test_get_group_deeper(self):
        """Test get() for groups deeper in hierarchy"""
        self.assertIs(self.gb.get('group1/subgroup1'), self.subgroup1)

    def test_get_dataset(self):
        """Test get() for datasets"""
        self.assertIs(self.gb.get('dataset1'), self.dataset1)

    def test_get_attr(self):
        """Test get() for attributes"""
        self.assertEqual(self.gb.get('int_attr'), self.int_attr)
        self.assertEqual(self.gb.get('str_attr'), self.str_attr)

    def test_get_link(self):
        """Test get() for links"""
        self.assertIs(self.gb.get('link1'), self.link1)

    def test_get_invalid_key(self):
        """Test get() for invalid key"""
        self.assertIs(self.gb.get('invalid_key'), None)

    def test_items(self):
        """Test items()"""
        items = (
            ('group1', self.group1),
            ('dataset1', self.dataset1),
            ('int_attr', self.int_attr),
            ('str_attr', self.str_attr),
            ('link1', self.link1),
        )
        # self.assertSetEqual(items, set(self.gb.items()))
        try:
            self.assertCountEqual(items, self.gb.items())
        except AttributeError:
            self.assertItemsEqual(items, self.gb.items())

    def test_keys(self):
        """Test keys()"""
        keys = (
            'group1',
            'dataset1',
            'int_attr',
            'str_attr',
            'link1',
        )
        try:
            self.assertCountEqual(keys, self.gb.keys())
        except AttributeError:
            self.assertItemsEqual(keys, self.gb.keys())

    def test_values(self):
        """Test values()"""
        values = (
            self.group1,
            self.dataset1,
            self.int_attr,
            self.str_attr,
            self.link1,
        )
        try:
            self.assertCountEqual(values, self.gb.values())
        except AttributeError:
            self.assertItemsEqual(values, self.gb.values())


class TestGroupBuilderIsEmpty(TestCase):

    def test_is_empty_true(self):
        """Test empty when group has nothing in it"""
        gb = GroupBuilder('gb')
        self.assertTrue(gb.is_empty())

    def test_is_empty_true_group_empty(self):
        """Test is_empty() when group has an empty subgroup"""
        gb1 = GroupBuilder('my_subgroup')
        gb2 = GroupBuilder('gb', {'my_subgroup': gb1})
        self.assertTrue(gb2.is_empty())

    def test_is_empty_false_dataset(self):
        """Test is_empty() when group has a dataset"""
        gb = GroupBuilder('gb', datasets={'my_dataset': DatasetBuilder('my_dataset')})
        self.assertFalse(gb.is_empty())

    def test_is_empty_false_group_dataset(self):
        """Test is_empty() when group has a subgroup with a dataset"""
        gb1 = GroupBuilder('my_subgroup', datasets={'my_dataset': DatasetBuilder('my_dataset')})
        gb2 = GroupBuilder('gb', {'my_subgroup': gb1})
        self.assertFalse(gb2.is_empty())

    def test_is_empty_false_attribute(self):
        """Test is_empty() when group has an attribute"""
        gb = GroupBuilder('gb', attributes={'my_attr': 'attr_value'})
        self.assertFalse(gb.is_empty())

    def test_is_empty_false_group_attribute(self):
        """Test is_empty() when group has subgroup with an attribute"""
        gb1 = GroupBuilder('my_subgroup', attributes={'my_attr': 'attr_value'})
        gb2 = GroupBuilder('gb', {'my_subgroup': gb1})
        self.assertFalse(gb2.is_empty())

    def test_is_empty_false_link(self):
        """Test is_empty() when group has a link"""
        gb1 = GroupBuilder('target')
        gb2 = GroupBuilder('gb', links={'my_link': LinkBuilder(gb1)})
        self.assertFalse(gb2.is_empty())

    def test_is_empty_false_group_link(self):
        """Test is_empty() when group has subgroup with a link"""
        gb1 = GroupBuilder('target')
        gb2 = GroupBuilder('my_subgroup', links={'my_link': LinkBuilder(gb1)})
        gb3 = GroupBuilder('gb', {'my_subgroup': gb2})
        self.assertFalse(gb3.is_empty())


class TestDatasetBuilder(TestCase):

    def test_constructor(self):
        gb1 = GroupBuilder('gb1')
        db1 = DatasetBuilder(
            name='db1',
            data=[1, 2, 3],
            dtype=int,
            attributes={'attr1': 10},
            maxshape=10,
            chunks=True,
            parent=gb1,
            source='source',
        )
        self.assertEqual(db1.name, 'db1')
        self.assertListEqual(db1.data, [1, 2, 3])
        self.assertEqual(db1.dtype, int)
        self.assertDictEqual(db1.attributes, {'attr1': 10})
        self.assertEqual(db1.maxshape, 10)
        self.assertTrue(db1.chunks)
        self.assertIs(db1.parent, gb1)
        self.assertEqual(db1.source, 'source')

    def test_constructor_data_builder_no_dtype(self):
        db1 = DatasetBuilder(name='db1', dtype=int)
        db2 = DatasetBuilder(name='db2', data=db1)
        self.assertEqual(db2.dtype, DatasetBuilder.OBJECT_REF_TYPE)

    def test_constructor_data_builder_dtype(self):
        db1 = DatasetBuilder(name='db1', dtype=int)
        db2 = DatasetBuilder(name='db2', data=db1, dtype=float)
        self.assertEqual(db2.dtype, float)

    def test_set_data(self):
        db1 = DatasetBuilder(name='db1')
        db1.data = [4, 5, 6]
        self.assertEqual(db1.data, [4, 5, 6])

    def test_set_dtype(self):
        db1 = DatasetBuilder(name='db1')
        db1.dtype = float
        self.assertEqual(db1.dtype, float)

    def test_overwrite_data(self):
        db1 = DatasetBuilder(name='db1', data=[1, 2, 3])
        msg = "Cannot overwrite data."
        with self.assertRaisesWith(AttributeError, msg):
            db1.data = [4, 5, 6]

    def test_overwrite_dtype(self):
        db1 = DatasetBuilder(name='db1', data=[1, 2, 3], dtype=int)
        msg = "Cannot overwrite dtype."
        with self.assertRaisesWith(AttributeError, msg):
            db1.dtype = float

    def test_overwrite_source(self):
        db1 = DatasetBuilder(name='db1', data=[1, 2, 3], source='source')
        msg = 'Cannot overwrite source.'
        with self.assertRaisesWith(AttributeError, msg):
            db1.source = 'new source'

    def test_overwrite_parent(self):
        gb1 = GroupBuilder('gb1')
        db1 = DatasetBuilder(name='db1', data=[1, 2, 3], parent=gb1)
        msg = 'Cannot overwrite parent.'
        with self.assertRaisesWith(AttributeError, msg):
            db1.parent = gb1

    def test_repr(self):
        gb1 = GroupBuilder('gb1')
        db1 = DatasetBuilder(
            name='db1',
            data=[1, 2, 3],
            dtype=int,
            attributes={'attr2': 10},
            maxshape=10,
            chunks=True,
            parent=gb1,
            source='source',
        )
        expected = "gb1/db1 DatasetBuilder {'attributes': {'attr2': 10}, 'data': [1, 2, 3]}"
        self.assertEqual(db1.__repr__(), expected)


class TestLinkBuilder(TestCase):

    def test_constructor(self):
        gb = GroupBuilder('gb1')
        db = DatasetBuilder('db1', [1, 2, 3])
        lb = LinkBuilder(db, 'link_name', gb, 'link_source')
        self.assertIs(lb.builder, db)
        self.assertEqual(lb.name, 'link_name')
        self.assertIs(lb.parent, gb)
        self.assertEqual(lb.source, 'link_source')

    def test_constructor_no_name(self):
        db = DatasetBuilder('db1', [1, 2, 3])
        lb = LinkBuilder(db)
        self.assertIs(lb.builder, db)
        self.assertEqual(lb.name, 'db1')


class TestReferenceBuilder(TestCase):

    def test_constructor(self):
        db = DatasetBuilder('db1', [1, 2, 3])
        rb = ReferenceBuilder(db)
        self.assertIs(rb.builder, db)


class TestRegionBuilder(TestCase):

    def test_constructor(self):
        db = DatasetBuilder('db1', [1, 2, 3])
        rb = RegionBuilder(slice(1, 3), db)
        self.assertEqual(rb.region, slice(1, 3))
        self.assertIs(rb.builder, db)
