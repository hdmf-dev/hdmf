import unittest

from hdmf.container import AbstractContainer, Container, Data


class TestAbstractContainer(unittest.TestCase):

    def test_constructor(self):
        """Test that constructor properly sets parent and both child and parent have an object_id
        """
        parent_obj = AbstractContainer('obj1')
        child_obj = AbstractContainer.__new__(AbstractContainer, parent=parent_obj)
        self.assertIs(child_obj.parent, parent_obj)
        self.assertIs(parent_obj.children[0], child_obj)
        self.assertIsNotNone(parent_obj.object_id)
        self.assertIsNotNone(child_obj.object_id)

    def test_constructor_object_id_none(self):
        """Test that setting object_id to None in __new__ is OK and the object ID is set on get
        """
        parent_obj = AbstractContainer('obj1')
        child_obj = AbstractContainer.__new__(AbstractContainer, parent=parent_obj, object_id=None)
        self.assertIsNotNone(child_obj.object_id)

    def test_set_parent(self):
        """Test that parent setter properly sets parent
        """
        parent_obj = AbstractContainer('obj1')
        child_obj = AbstractContainer('obj2')
        child_obj.parent = parent_obj
        self.assertIs(child_obj.parent, parent_obj)
        self.assertIs(parent_obj.children[0], child_obj)

    def test_set_parent_overwrite(self):
        """Test that parent setter properly blocks overwriting
        """
        parent_obj = AbstractContainer('obj1')
        child_obj = AbstractContainer('obj2')
        child_obj.parent = parent_obj
        self.assertIs(parent_obj.children[0], child_obj)

        another_obj = AbstractContainer('obj3')
        with self.assertRaisesRegex(ValueError,
                                    'Cannot reassign parent to AbstractContainer: %s. Parent is already: %s.'
                                    % (repr(child_obj), repr(child_obj.parent))):
            child_obj.parent = another_obj
        self.assertIs(child_obj.parent, parent_obj)
        self.assertIs(parent_obj.children[0], child_obj)

    def test_set_parent_overwrite_proxy(self):
        """Test that parent setter properly blocks overwriting with proxy/object
        """
        child_obj = AbstractContainer('obj2')
        child_obj.parent = object()

        with self.assertRaisesRegex(ValueError,
                                    r"Got None for parent of '[^/]+' - cannot overwrite Proxy with NoneType"):
            child_obj.parent = None

    def test_slash_restriction(self):
        self.assertRaises(ValueError, AbstractContainer, 'bad/name')

    def test_set_modified_parent(self):
        """Test that set modified properly sets parent modified
        """
        parent_obj = AbstractContainer('obj1')
        child_obj = AbstractContainer('obj2')
        child_obj.parent = parent_obj
        parent_obj.set_modified(False)
        child_obj.set_modified(False)
        self.assertFalse(child_obj.parent.modified)
        child_obj.set_modified()
        self.assertTrue(child_obj.parent.modified)

    def test_add_child(self):
        """Test that add child creates deprecation warning and also properly sets child's parent and modified
        """
        parent_obj = AbstractContainer('obj1')
        child_obj = AbstractContainer('obj2')
        parent_obj.set_modified(False)
        with self.assertWarnsRegex(DeprecationWarning,
                                   r'add_child is deprecated\. Set the parent attribute instead\.'):
            parent_obj.add_child(child_obj)
        self.assertIs(child_obj.parent, parent_obj)
        self.assertTrue(parent_obj.modified)
        self.assertIs(parent_obj.children[0], child_obj)

    def test_set_parent_exists(self):
        """Test that setting a parent a second time does nothing
        """
        parent_obj = AbstractContainer('obj1')
        child_obj = AbstractContainer('obj2')
        child_obj3 = AbstractContainer('obj3')
        child_obj.parent = parent_obj
        child_obj.parent = parent_obj
        child_obj3.parent = parent_obj
        self.assertEqual(len(parent_obj.children), 2)
        self.assertIs(parent_obj.children[0], child_obj)
        self.assertIs(parent_obj.children[1], child_obj3)

    def test_reassign_container_source(self):
        """Test that reassign container source throws error
        """
        parent_obj = AbstractContainer('obj1')
        parent_obj.container_source = 'a source'
        with self.assertRaisesRegex(Exception, 'cannot reassign container_source'):
            parent_obj.container_source = 'some other source'

    def test_type_hierarchy(self):
        self.assertEqual(AbstractContainer.type_hierarchy(), (AbstractContainer, object))


class Subcontainer(Container):

    __fields__ = ('data1', 'data2', 'data3')

    def __init__(self, name, data1, data2, data3=None):
        super(Subcontainer, self).__init__(name=name)
        self.data1 = data1
        self.data2 = data2
        self.data3 = data3


class TestContainer(unittest.TestCase):

    def test_repr(self):
        parent_obj = Container('obj1')
        self.assertRegex(str(parent_obj), r"obj1 hdmf.container.Container at 0x%d" % id(parent_obj))


class TestContainerDimCoords(unittest.TestCase):

    def test_set_get_dim_coord(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        obj1.set_dim_coord('data1', 0, 'letters', 'data2')
        self.assertEqual(obj1.get_dim_coord('data1', 0, 'letters'), obj1.data2)

    def test_set_dim_coord_data_not_found(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        with self.assertRaisesRegex(ValueError, "Field name 'data0' not found in Subcontainer"):
            obj1.set_dim_coord('data0', 0, 'letters', 'data2')

    def test_set_dim_coord_coord_not_found(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        with self.assertRaisesRegex(ValueError, "Dim coord name 'data3' not found in Subcontainer"):
            obj1.set_dim_coord('data1', 0, 'letters', 'data3')

    def test_set_dim_coord_out_bounds(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        with self.assertRaisesRegex(ValueError, 'Axis 1 does not exist for dim coords of data1 in Subcontainer'):
            obj1.set_dim_coord('data1', 1, 'letters', 'data2')
        with self.assertRaisesRegex(ValueError, 'Axis -1 does not exist for dim coords of data1 in Subcontainer'):
            obj1.set_dim_coord('data1', -1, 'letters', 'data2')

    def test_set_dim_coord_exists(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'], data3=['A', 'B', 'C'])
        obj1.set_dim_coord('data1', 0, 'letters', 'data2')
        obj1.set_dim_coord('data1', 0, 'letters', 'data3')
        self.assertEqual(obj1.get_dim_coord('data1', 0, 'letters'), obj1.data3)

    def test_get_dim_coord_data_not_found(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        obj1.set_dim_coord('data1', 0, 'letters', 'data2')
        with self.assertRaisesRegex(ValueError, "Field name 'data0' not found in Subcontainer"):
            obj1.get_dim_coord('data0', 0, 'letters')

    def test_get_dim_coord_coord_not_found(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        obj1.set_dim_coord('data1', 0, 'letters', 'data2')
        with self.assertRaisesRegex(ValueError, "Dim coord label 'symbols' not found in Subcontainer"):
            obj1.get_dim_coord('data1', 0, 'symbols')

    def test_get_dim_coord_out_bounds(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        obj1.set_dim_coord('data1', 0, 'letters', 'data2')
        with self.assertRaisesRegex(ValueError, 'Axis 1 does not exist for dim coords of data1 in Subcontainer'):
            obj1.get_dim_coord('data1', 1, 'letters')
        with self.assertRaisesRegex(ValueError, 'Axis -1 does not exist for dim coords of data1 in Subcontainer'):
            obj1.get_dim_coord('data1', -1, 'letters')

    def test_test(self):
        obj1 = Subcontainer('obj1', data1=[1, 2, 3], data2=['a', 'b', 'c'])
        obj1.set_dim_coord('data1', 0, 'letters', 'data2', '234324', 345)  # TODO should trigger extra argument error?


class TestData(unittest.TestCase):

    def test_bool_true(self):
        """Test that __bool__ method works correctly on data with len
        """
        data_obj = Data('my_data', [1, 2, 3, 4, 5])
        self.assertTrue(data_obj)

    def test_bool_false(self):
        """Test that __bool__ method works correctly on empty data
        """
        data_obj = Data('my_data', [])
        self.assertFalse(data_obj)


if __name__ == '__main__':
    unittest.main()
