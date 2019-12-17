from hdmf.utils import LabelledDict
from hdmf.testing import TestCase


class MyTestClass:

    def __init__(self, prop1, prop2):
        self._prop1 = prop1
        self._prop2 = prop2

    @property
    def prop1(self):
        return self._prop1

    @property
    def prop2(self):
        return self._prop2


class TestLabelledDict(TestCase):

    def test_constructor(self):
        """Test that constructor sets arguments properly"""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        self.assertEqual(ld.label, 'all_objects')
        self.assertEqual(ld.key_attr, 'prop1')

    def test_constructor_default(self):
        """Test that constructor sets default key attribute"""
        ld = LabelledDict(label='all_objects')
        self.assertEqual(ld.key_attr, 'name')

    def test_set_key_attr(self):
        """Test that the key attribute cannot be set after initialization"""
        ld = LabelledDict(label='all_objects')
        with self.assertRaisesWith(AttributeError, "can't set attribute"):
            ld.key_attr = 'another_name'

    def test_getitem_unknown_val(self):
        """Test that dict[val] raises an error if there are no matches for val"""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        with self.assertRaisesWith(KeyError, "'unknown_val'"):
            ld['unknown_val']

    def test_getitem_eqeq_unknown_val(self):
        """Test that dict[key_attr == val] raises an error if there are no matches for val"""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        with self.assertRaisesWith(KeyError, "'unknown_val'"):
            ld['prop1 == unknown_val']

    def test_getitem_eqeq_other_key_attr(self):
        """Test that dict[key_attr == val] raises an error if there are no matches for other_attr == val"""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        with self.assertRaisesWith(KeyError, "'unknown_val'"):
            ld['other_attr == unknown_val']

    def test_getitem_eqeq_no_key_attr(self):
        """Test that dict[key_attr == val] raises an error if key_attr is not given"""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        with self.assertRaisesWith(ValueError, "An attribute name is required before '=='."):
            ld[' == unknown_key']

    def test_getitem_eqeq_no_val(self):
        """Test that dict[key_attr == val] raises an error if val is not given"""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        with self.assertRaisesWith(ValueError, "A value is required after '=='."):
            ld['prop1 == ']

    def test_getitem_eqeq_no_key_attr_no_val(self):
        """Test that dict[key_attr == val] raises an error if key_attr is not given and val is not given"""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        with self.assertRaisesWith(ValueError, "An attribute name is required before '=='."):
            ld[' == ']

    def test_add_basic(self):
        """Test add function on object with correct key_attr"""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')
        ld.add(obj1)
        self.assertIs(ld['a'], obj1)

    def test_add_value_missing_key(self):
        """Test that add raises an error if the value being set does not have the attribute key_attr"""
        ld = LabelledDict(label='all_objects', key_attr='prop3')
        obj1 = MyTestClass('a', 'b')

        err_msg = r"Cannot set value '<.*>' in LabelledDict\. Value must have key 'prop3'\."
        with self.assertRaisesRegex(ValueError, err_msg):
            ld.add(obj1)

    def test_setitem_getitem_basic(self):
        """Test that setitem and getitem properly set and get the object"""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')
        ld.add(obj1)
        self.assertIs(ld['a'], obj1)

    def test_setitem_value_missing_key(self):
        """Test that setitem raises an error if the value being set does not have the attribute key_attr"""
        ld = LabelledDict(label='all_objects', key_attr='prop3')
        obj1 = MyTestClass('a', 'b')

        err_msg = r"Cannot set value '<.*>' in LabelledDict\. Value must have key 'prop3'\."
        with self.assertRaisesRegex(ValueError, err_msg):
            ld['a'] = obj1

    def test_setitem_value_wrong_value(self):
        """Test that setitem raises an error if the value being set has a different value for attribute key_attr
        than the given key"""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')

        err_msg = r"Key 'b' must equal attribute 'prop1' of '<.*>'\."
        with self.assertRaisesRegex(KeyError, err_msg):
            ld['b'] = obj1

    def test_addval_getitem_eqeq(self):
        """Test that dict[key_attr == val] returns the single matching object"""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')
        ld.add(obj1)
        self.assertIs(ld['prop1 == a'], obj1)

    def test_addval_getitem_eqeq_unknown_val(self):
        """Test that dict[key_attr == val] with an unknown val raises an error even if there are other objects in
        dict"""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')
        ld.add(obj1)
        with self.assertRaisesWith(KeyError, "'unknown_val'"):
            ld['prop1 == unknown_val']

    def test_addval_getitem_eqeq_unknown_key_val(self):
        """Test that dict[key_attr == val] where prop3 does not match any objects in the dict raises an error"""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')
        ld['a'] = obj1
        with self.assertRaisesWith(KeyError, "'unknown_val'"):
            ld['prop3 == unknown_val']

    def test_addval_getitem_other_key(self):
        """Test that dict[other_key == val] returns a list of matching objects"""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')
        ld.add(obj1)
        self.assertSetEqual(ld['prop2 == b'], {obj1})

    def test_addval_getitem_other_key_multi(self):
        """Test that dict[other_key == val] returns a list of matching objects"""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')
        obj2 = MyTestClass('d', 'b')
        obj3 = MyTestClass('f', 'e')
        ld.add(obj1)
        ld.add(obj2)
        ld.add(obj3)
        self.assertSetEqual(ld['prop2 == b'], set([obj1, obj2]))

    def test_addval_getitem_other_key_none(self):
        """Test that dict[other_key == val] raises an error if val does not equal any of the other_key attribute values
        in the dict, even when the other_key attribute exists"""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')
        obj2 = MyTestClass('d', 'b')
        obj3 = MyTestClass('f', 'e')
        ld.add(obj1)
        ld.add(obj2)
        ld.add(obj3)
        with self.assertRaisesWith(KeyError, "'c'"):
            ld['prop2 == c']
