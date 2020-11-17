from hdmf.testing import TestCase
from hdmf.utils import LabelledDict


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
        """Test that constructor sets arguments properly."""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        self.assertEqual(ld.label, 'all_objects')
        self.assertEqual(ld.key_attr, 'prop1')

    def test_constructor_default(self):
        """Test that constructor sets default key attribute."""
        ld = LabelledDict(label='all_objects')
        self.assertEqual(ld.key_attr, 'name')

    def test_set_key_attr(self):
        """Test that the key attribute cannot be set after initialization."""
        ld = LabelledDict(label='all_objects')
        with self.assertRaisesWith(AttributeError, "can't set attribute"):
            ld.key_attr = 'another_name'

    def test_getitem_unknown_val(self):
        """Test that dict[unknown_key] where the key unknown_key is not in the dict raises an error."""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        with self.assertRaisesWith(KeyError, "'unknown_key'"):
            ld['unknown_key']

    def test_getitem_eqeq_unknown_val(self):
        """Test that dict[unknown_attr == val] where there are no query matches returns an empty set."""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        self.assertSetEqual(ld['unknown_attr == val'], set())

    def test_getitem_eqeq_other_key(self):
        """Test that dict[other_attr == val] where there are no query matches returns an empty set."""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        self.assertSetEqual(ld['prop2 == val'], set())

    def test_getitem_eqeq_no_key_attr(self):
        """Test that dict[key_attr == val] raises an error if key_attr is not given."""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        with self.assertRaisesWith(ValueError, "An attribute name is required before '=='."):
            ld[' == unknown_key']

    def test_getitem_eqeq_no_val(self):
        """Test that dict[key_attr == val] raises an error if val is not given."""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        with self.assertRaisesWith(ValueError, "A value is required after '=='."):
            ld['prop1 == ']

    def test_getitem_eqeq_no_key_attr_no_val(self):
        """Test that dict[key_attr == val] raises an error if key_attr is not given and val is not given."""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        with self.assertRaisesWith(ValueError, "An attribute name is required before '=='."):
            ld[' == ']

    def test_add_basic(self):
        """Test add method on object with correct key_attr."""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')
        ld.add(obj1)
        self.assertIs(ld['a'], obj1)

    def test_add_value_missing_key(self):
        """Test that add raises an error if the value being set does not have the attribute key_attr."""
        ld = LabelledDict(label='all_objects', key_attr='unknown_key')
        obj1 = MyTestClass('a', 'b')

        err_msg = r"Cannot set value '<.*>' in LabelledDict\. Value must have attribute 'unknown_key'\."
        with self.assertRaisesRegex(ValueError, err_msg):
            ld.add(obj1)

    def test_setitem_getitem_basic(self):
        """Test that setitem and getitem properly set and get the object."""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')
        ld.add(obj1)
        self.assertIs(ld['a'], obj1)

    def test_setitem_value_missing_key(self):
        """Test that setitem raises an error if the value being set does not have the attribute key_attr."""
        ld = LabelledDict(label='all_objects', key_attr='unknown_key')
        obj1 = MyTestClass('a', 'b')

        err_msg = r"Cannot set value '<.*>' in LabelledDict\. Value must have attribute 'unknown_key'\."
        with self.assertRaisesRegex(ValueError, err_msg):
            ld['a'] = obj1

    def test_setitem_value_inconsistent_key(self):
        """Test that setitem raises an error if the value being set has an inconsistent key."""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')

        err_msg = r"Key 'b' must equal attribute 'prop1' of '<.*>'\."
        with self.assertRaisesRegex(KeyError, err_msg):
            ld['b'] = obj1

    def test_setitem_value_duplicate_key(self):
        """Test that setitem raises an error if the key already exists in the dict."""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')
        obj2 = MyTestClass('a', 'c')

        ld['a'] = obj1

        err_msg = "Key 'a' is already in this dict. Cannot reset items in a LabelledDict."
        with self.assertRaisesWith(TypeError, err_msg):
            ld['a'] = obj2

    def test_add_callable(self):
        """Test that add properly adds the object and calls the add_callable function."""
        self.signal = None

        def func(v):
            self.signal = v

        ld = LabelledDict(label='all_objects', key_attr='prop1', add_callable=func)
        obj1 = MyTestClass('a', 'b')
        ld.add(obj1)
        self.assertIs(ld['a'], obj1)
        self.assertIs(self.signal, obj1)

    def test_setitem_callable(self):
        """Test that setitem properly sets the object and calls the add_callable function."""
        self.signal = None

        def func(v):
            self.signal = v

        ld = LabelledDict(label='all_objects', key_attr='prop1', add_callable=func)
        obj1 = MyTestClass('a', 'b')
        ld['a'] = obj1
        self.assertIs(ld['a'], obj1)
        self.assertIs(self.signal, obj1)

    def test_getitem_eqeq_nonempty(self):
        """Test that dict[key_attr == val] returns the single matching object."""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')
        ld.add(obj1)
        self.assertIs(ld['prop1 == a'], obj1)

    def test_getitem_eqeq_nonempty_key_attr_no_match(self):
        """Test that dict[key_attr == unknown_val] where a matching value is not found raises a KeyError."""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')
        ld.add(obj1)
        with self.assertRaisesWith(KeyError, "'unknown_val'"):
            ld['prop1 == unknown_val']  # same as ld['unknown_val']

    def test_getitem_eqeq_nonempty_unknown_attr(self):
        """Test that dict[unknown_attr == val] where unknown_attr is not a field on the values raises an error."""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')
        ld['a'] = obj1
        self.assertSetEqual(ld['unknown_attr == unknown_val'], set())

    def test_getitem_nonempty_other_key(self):
        """Test that dict[other_key == val] returns a set of matching objects."""
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')
        obj2 = MyTestClass('d', 'b')
        obj3 = MyTestClass('f', 'e')
        ld.add(obj1)
        ld.add(obj2)
        ld.add(obj3)
        self.assertSetEqual(ld['prop2 == b'], {obj1, obj2})

    def test_pop_nocallback(self):
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')
        ld.add(obj1)

        ret = ld.pop('a')
        self.assertEqual(ret, obj1)
        self.assertEqual(ld, dict())

    def test_pop_callback(self):
        self.signal = None

        def func(v):
            self.signal = v

        ld = LabelledDict(label='all_objects', key_attr='prop1', remove_callable=func)
        obj1 = MyTestClass('a', 'b')
        ld.add(obj1)

        ret = ld.pop('a')
        self.assertEqual(ret, obj1)
        self.assertEqual(self.signal, obj1)
        self.assertEqual(ld, dict())

    def test_popitem_nocallback(self):
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')
        ld.add(obj1)

        ret = ld.popitem()
        self.assertEqual(ret, ('a', obj1))
        self.assertEqual(ld, dict())

    def test_popitem_callback(self):
        self.signal = None

        def func(v):
            self.signal = v

        ld = LabelledDict(label='all_objects', key_attr='prop1', remove_callable=func)
        obj1 = MyTestClass('a', 'b')
        ld.add(obj1)

        ret = ld.popitem()
        self.assertEqual(ret, ('a', obj1))
        self.assertEqual(self.signal, obj1)
        self.assertEqual(ld, dict())

    def test_clear_nocallback(self):
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')
        obj2 = MyTestClass('d', 'b')
        ld.add(obj1)
        ld.add(obj2)
        ld.clear()
        self.assertEqual(ld, dict())

    def test_clear_callback(self):
        self.signal = set()

        def func(v):
            self.signal.add(v)

        ld = LabelledDict(label='all_objects', key_attr='prop1', remove_callable=func)
        obj1 = MyTestClass('a', 'b')
        obj2 = MyTestClass('d', 'b')
        ld.add(obj1)
        ld.add(obj2)
        ld.clear()
        self.assertSetEqual(self.signal, {obj2, obj1})
        self.assertEqual(ld, dict())

    def test_delitem_nocallback(self):
        ld = LabelledDict(label='all_objects', key_attr='prop1')
        obj1 = MyTestClass('a', 'b')
        ld.add(obj1)

        del ld['a']
        self.assertEqual(ld, dict())

    def test_delitem_callback(self):
        self.signal = None

        def func(v):
            self.signal = v

        ld = LabelledDict(label='all_objects', key_attr='prop1', remove_callable=func)
        obj1 = MyTestClass('a', 'b')
        ld.add(obj1)

        del ld['a']
        self.assertEqual(self.signal, obj1)
        self.assertEqual(ld, dict())

    def test_update_callback(self):
        ld = LabelledDict(label='all_objects', key_attr='prop1')

        with self.assertRaisesWith(TypeError, "update is not supported for LabelledDict"):
            ld.update(object())

    def test_setdefault_callback(self):
        ld = LabelledDict(label='all_objects', key_attr='prop1')

        with self.assertRaisesWith(TypeError, "setdefault is not supported for LabelledDict"):
            ld.setdefault(object())
