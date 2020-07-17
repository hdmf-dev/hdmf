import numpy as np

from hdmf.container import AbstractContainer, Container, Data
from hdmf.testing import TestCase


class Subcontainer(Container):
    pass


class TestContainer(TestCase):

    def test_constructor(self):
        """Test that constructor properly sets parent and both child and parent have an object_id
        """
        parent_obj = Container('obj1')
        child_obj = Container.__new__(Container, parent=parent_obj)
        self.assertIs(child_obj.parent, parent_obj)
        self.assertIs(parent_obj.children[0], child_obj)
        self.assertIsNotNone(parent_obj.object_id)
        self.assertIsNotNone(child_obj.object_id)

    def test_constructor_object_id_none(self):
        """Test that setting object_id to None in __new__ is OK and the object ID is set on get
        """
        parent_obj = Container('obj1')
        child_obj = Container.__new__(Container, parent=parent_obj, object_id=None)
        self.assertIsNotNone(child_obj.object_id)

    def test_set_parent(self):
        """Test that parent setter properly sets parent
        """
        parent_obj = Container('obj1')
        child_obj = Container('obj2')
        child_obj.parent = parent_obj
        self.assertIs(child_obj.parent, parent_obj)
        self.assertIs(parent_obj.children[0], child_obj)

    def test_set_parent_overwrite(self):
        """Test that parent setter properly blocks overwriting
        """
        parent_obj = Container('obj1')
        child_obj = Container('obj2')
        child_obj.parent = parent_obj
        self.assertIs(parent_obj.children[0], child_obj)

        another_obj = Container('obj3')
        with self.assertRaisesWith(ValueError,
                                   'Cannot reassign parent to Container: %s. Parent is already: %s.'
                                   % (repr(child_obj), repr(child_obj.parent))):
            child_obj.parent = another_obj
        self.assertIs(child_obj.parent, parent_obj)
        self.assertIs(parent_obj.children[0], child_obj)

    def test_set_parent_overwrite_proxy(self):
        """Test that parent setter properly blocks overwriting with proxy/object
        """
        child_obj = Container('obj2')
        child_obj.parent = object()

        with self.assertRaisesRegex(ValueError,
                                    r"Got None for parent of '[^/]+' - cannot overwrite Proxy with NoneType"):
            child_obj.parent = None

    def test_slash_restriction(self):
        self.assertRaises(ValueError, Container, 'bad/name')

    def test_set_modified_parent(self):
        """Test that set modified properly sets parent modified
        """
        parent_obj = Container('obj1')
        child_obj = Container('obj2')
        child_obj.parent = parent_obj
        parent_obj.set_modified(False)
        child_obj.set_modified(False)
        self.assertFalse(child_obj.parent.modified)
        child_obj.set_modified()
        self.assertTrue(child_obj.parent.modified)

    def test_add_child(self):
        """Test that add child creates deprecation warning and also properly sets child's parent and modified
        """
        parent_obj = Container('obj1')
        child_obj = Container('obj2')
        parent_obj.set_modified(False)
        with self.assertWarnsWith(DeprecationWarning, 'add_child is deprecated. Set the parent attribute instead.'):
            parent_obj.add_child(child_obj)
        self.assertIs(child_obj.parent, parent_obj)
        self.assertTrue(parent_obj.modified)
        self.assertIs(parent_obj.children[0], child_obj)

    def test_set_parent_exists(self):
        """Test that setting a parent a second time does nothing
        """
        parent_obj = Container('obj1')
        child_obj = Container('obj2')
        child_obj3 = Container('obj3')
        child_obj.parent = parent_obj
        child_obj.parent = parent_obj
        child_obj3.parent = parent_obj
        self.assertEqual(len(parent_obj.children), 2)
        self.assertIs(parent_obj.children[0], child_obj)
        self.assertIs(parent_obj.children[1], child_obj3)

    def test_reassign_container_source(self):
        """Test that reassign container source throws error
        """
        parent_obj = Container('obj1')
        parent_obj.container_source = 'a source'
        with self.assertRaisesWith(Exception, 'cannot reassign container_source'):
            parent_obj.container_source = 'some other source'

    def test_repr(self):
        parent_obj = Container('obj1')
        self.assertRegex(str(parent_obj), r"obj1 hdmf.container.Container at 0x\d+")

    def test_type_hierarchy(self):
        self.assertEqual(Container.type_hierarchy(), (Container, AbstractContainer, object))
        self.assertEqual(Subcontainer.type_hierarchy(), (Subcontainer, Container, AbstractContainer, object))

    def test_generate_new_id_parent(self):
        """Test that generate_new_id sets a new ID on the container and its children and sets modified on all."""
        parent_obj = Container('obj1')
        child_obj = Container('obj2')
        child_obj.parent = parent_obj
        old_parent_id = parent_obj.object_id
        old_child_id = child_obj.object_id

        parent_obj.set_modified(False)
        child_obj.set_modified(False)
        parent_obj.generate_new_id()
        self.assertNotEqual(old_parent_id, parent_obj.object_id)
        self.assertNotEqual(old_child_id, child_obj.object_id)
        self.assertTrue(parent_obj.modified)
        self.assertTrue(child_obj.modified)

    def test_generate_new_id_child(self):
        """Test that generate_new_id sets a new ID on the container and not its parent and sets modified on both."""
        parent_obj = Container('obj1')
        child_obj = Container('obj2')
        child_obj.parent = parent_obj
        old_parent_id = parent_obj.object_id
        old_child_id = child_obj.object_id

        parent_obj.set_modified(False)
        child_obj.set_modified(False)
        child_obj.generate_new_id()
        self.assertEqual(old_parent_id, parent_obj.object_id)
        self.assertNotEqual(old_child_id, child_obj.object_id)
        self.assertTrue(parent_obj.modified)
        self.assertTrue(child_obj.modified)

    def test_generate_new_id_parent_no_recurse(self):
        """Test that generate_new_id(recurse=False) sets a new ID on the container and not its children."""
        parent_obj = Container('obj1')
        child_obj = Container('obj2')
        child_obj.parent = parent_obj
        old_parent_id = parent_obj.object_id
        old_child_id = child_obj.object_id

        parent_obj.set_modified(False)
        child_obj.set_modified(False)
        parent_obj.generate_new_id(recurse=False)
        self.assertNotEqual(old_parent_id, parent_obj.object_id)
        self.assertEqual(old_child_id, child_obj.object_id)
        self.assertTrue(parent_obj.modified)
        self.assertFalse(child_obj.modified)

    def test_remove_child(self):
        """Test that removing a child removes only the child.
        """
        parent_obj = Container('obj1')
        child_obj = Container('obj2')
        child_obj3 = Container('obj3')
        child_obj.parent = parent_obj
        child_obj3.parent = parent_obj
        parent_obj._remove_child(child_obj)
        self.assertTupleEqual(parent_obj.children, (child_obj3, ))
        self.assertTrue(parent_obj.modified)
        self.assertTrue(child_obj.modified)

    def test_remove_child_noncontainer(self):
        """Test that removing a non-Container child raises an error.
        """
        msg = "Cannot remove non-AbstractContainer object from children."
        with self.assertRaisesWith(ValueError, msg):
            Container('obj1')._remove_child(object())

    def test_remove_child_nonchild(self):
        """Test that removing a non-Container child raises an error.
        """
        msg = "Container 'dummy' is not a child of Container 'obj1'."
        with self.assertRaisesWith(ValueError, msg):
            Container('obj1')._remove_child(Container('dummy'))


class TestData(TestCase):

    def test_constructor_scalar(self):
        """Test that constructor works correctly on scalar data
        """
        data_obj = Data('my_data', 'foobar')
        self.assertEqual(data_obj.data, 'foobar')

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

    def test_shape_nparray(self):
        """
        Test that shape works for np.array
        """
        data_obj = Data('my_data', np.arange(10).reshape(2, 5))
        self.assertTupleEqual(data_obj.shape, (2, 5))

    def test_shape_list(self):
        """
        Test that shape works for np.array
        """
        data_obj = Data('my_data', [[0, 1, 2, 3, 4], [0, 1, 2, 3, 4]])
        self.assertTupleEqual(data_obj.shape, (2, 5))
