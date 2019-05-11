import unittest2 as unittest

from hdmf.container import Container

class Subcontainer(Container):
    pass

class TestContainer(unittest.TestCase):

    def test_constructor(self):
        """Test that constructor properly sets parent and parent knows its child
        """
        parent_obj = Container('obj1')
        child_obj = Container('obj2', parent_obj)
        self.assertIs(child_obj.parent, parent_obj)
        self.assertIs(parent_obj.children[0], child_obj)

    def test_set_parent(self):
        """Test that parent setter properly sets parent
        """
        parent_obj = Container('obj1')
        child_obj = Container('obj2')
        child_obj.parent = parent_obj
        self.assertIs(child_obj.parent, parent_obj)

    def test_set_parent_overwrite(self):
        """Test that parent setter properly blocks overwriting
        """
        parent_obj = Container('obj1')
        child_obj = Container('obj2')
        child_obj.parent = parent_obj

        another_obj = Container('obj3')
        with self.assertRaisesRegexp(Exception, 'cannot reassign parent'):
            child_obj.parent = another_obj
        self.assertIs(child_obj.parent, parent_obj)

    def test_set_parent_overwrite_proxy(self):
        """Test that parent setter properly blocks overwriting with proxy/object
        """
        parent_obj = Container('obj1')
        child_obj = Container('obj2')
        child_obj.parent = object()

        with self.assertRaisesRegex(Exception, \
                r"got None for parent of '[^/]+' - cannot overwrite Proxy with NoneType"):
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
        """Test that add child and properly sets child's parent and modified
        """
        parent_obj = Container('obj1')
        child_obj = Container('obj2')
        parent_obj.set_modified(False)
        parent_obj.add_child(child_obj)
        self.assertIs(child_obj.parent, parent_obj)
        self.assertTrue(parent_obj.modified)

    def test_add_child_none(self):
        """Test that add child does nothing if child is none
        """
        parent_obj = Container('obj1')
        parent_obj.set_modified(False)
        with self.assertWarnsRegex(UserWarning, r'cannot add None as child to a container .+'):
            parent_obj.add_child(None)
        self.assertEqual(len(parent_obj.children), 0)
        self.assertFalse(parent_obj.modified)

    def test_reassign_container_source(self):
        """Test that reassign container source throws error
        """
        parent_obj = Container('obj1', container_source='a source')
        with self.assertRaisesRegex(Exception, 'cannot reassign container_source'):
            parent_obj.container_source = 'some other source'

        another_obj = Container('obj2')
        another_obj.container_source = 'a source'
        with self.assertRaisesRegex(Exception, 'cannot reassign container_source'):
            another_obj.container_source = 'some other source'

    def test_repr(self):
        parent_obj = Container('obj1')
        self.assertRegex(str(parent_obj), r"<Container 'obj1' at 0x\d+>")

    def test_type_hierarchy(self):
        self.assertEqual(Container.type_hierarchy(), (Container, object))
        self.assertEqual(Subcontainer.type_hierarchy(), (Subcontainer, Container, object))


if __name__ == '__main__':
    unittest.main()
