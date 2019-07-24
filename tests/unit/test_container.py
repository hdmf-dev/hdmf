import unittest2 as unittest

from tests.unit.test_utils import Foo
from hdmf.container import Container
from hdmf.query import HDMFDataset
from hdmf.data_utils import DataIO
from copy import deepcopy


class Subcontainer(Container):
    pass


class TestContainer(unittest.TestCase):

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
        """Test that setting object_id to None in __new__ is OK
        """
        parent_obj = Container('obj1')
        child_obj = Container.__new__(Container, parent=parent_obj, object_id=None)
        self.assertIsNone(child_obj.object_id)

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
        with self.assertRaisesRegex(ValueError,
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
        with self.assertWarnsRegex(DeprecationWarning,
                                   r'add_child is deprecated\. Set the parent attribute instead\.'):
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
        with self.assertRaisesRegex(Exception, 'cannot reassign container_source'):
            parent_obj.container_source = 'some other source'

    def test_repr(self):
        parent_obj = Container('obj1')
        self.assertRegex(str(parent_obj), r"<Container 'obj1' at 0x\d+>")

    def test_type_hierarchy(self):
        self.assertEqual(Container.type_hierarchy(), (Container, object))
        self.assertEqual(Subcontainer.type_hierarchy(), (Subcontainer, Container, object))

    def test_deepcopy(self):
        parent_obj = Container('obj1')
        parent_obj.container_source = 'a file'
        parent_obj.set_modified(False)
        child_obj = Container('obj2')
        child_obj.parent = parent_obj
        child_obj.container_source = 'a file'
        child_obj.set_modified(False)
        child_child_obj = Container('obj3')
        child_child_obj.parent = child_obj
        child_child_obj.container_source = 'a file'

        parent_copy = deepcopy(parent_obj)
        self.assertEqual(parent_copy.name, 'obj1')
        self.assertEqual(parent_copy.children[0].name, 'obj2')
        self.assertEqual(parent_copy.children[0].children[0].name, 'obj3')
        self.assertNotEqual(parent_copy.object_id, parent_obj.object_id)
        self.assertNotEqual(parent_copy.children[0].object_id, child_obj.object_id)
        self.assertNotEqual(parent_copy.children[0].children[0].object_id, child_child_obj.object_id)
        self.assertIsNone(parent_copy.container_source)
        self.assertIsNone(parent_copy.children[0].container_source)
        self.assertIsNone(parent_copy.children[0].children[0].container_source)
        self.assertTrue(parent_copy.modified)
        self.assertTrue(parent_copy.children[0].modified)
        self.assertTrue(parent_copy.children[0].children[0].modified)

        child_copy = deepcopy(child_obj)
        self.assertIsNone(child_copy.parent)

    def test_deepcopy_data(self):
        parent_obj = Foo('obj1', HDMFDataset([1, 2, 3, 4, 5]), 'a string', 10)
        parent_obj.container_source = 'a file'
        parent_obj.set_modified(False)
        child_obj = Foo('obj2', DataIO(HDMFDataset([1, 2, 3, 4, 5])), 'a string2', 20)
        child_obj.parent = parent_obj
        child_obj.container_source = 'a file'

        parent_copy = deepcopy(parent_obj)
        self.assertListEqual(parent_copy.my_data, [1, 2, 3, 4, 5])
        self.assertEqual(parent_copy.attr1, 'a string')
        self.assertEqual(parent_copy.attr2, 10)
        self.assertListEqual(parent_copy.children[0].my_data, [1, 2, 3, 4, 5])

        # TODO test deepcopy with references and H5Dataset


if __name__ == '__main__':
    unittest.main()
