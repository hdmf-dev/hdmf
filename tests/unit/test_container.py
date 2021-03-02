import numpy as np
from hdmf.container import AbstractContainer, Container, Data
from hdmf.testing import TestCase
from hdmf.utils import docval


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


class TestAbstractContainerFieldsConf(TestCase):

    def test_bad_fields_type(self):
        msg = "'__fields__' must be of type tuple"
        with self.assertRaisesWith(TypeError, msg):
            class BadFieldsType(AbstractContainer):
                __fields__ = {'name': 'field1'}

    def test_bad_field_conf_key(self):
        msg = "Unrecognized key 'child' in __fields__ config 'field1' on BadFieldConfKey"
        with self.assertRaisesWith(ValueError, msg):
            class BadFieldConfKey(AbstractContainer):
                __fields__ = ({'name': 'field1', 'child': True}, )

    def test_bad_field_missing_name(self):
        msg = "must specify 'name' if using dict in __fields__"
        with self.assertRaisesWith(ValueError, msg):
            class BadFieldConfKey(AbstractContainer):
                __fields__ = ({'child': True}, )

    @staticmethod
    def find_all_properties(klass):
        return [attr for attr in dir(klass) if isinstance(getattr(klass, attr, None), property)]

    def test_empty_fields(self):
        class EmptyFields(AbstractContainer):
            __fields__ = tuple()

        self.assertTupleEqual(EmptyFields.__fields__, tuple())
        self.assertTupleEqual(EmptyFields._get_fields(), tuple())
        self.assertTupleEqual(EmptyFields.get_fields_conf(), tuple())

        props = TestAbstractContainerFieldsConf.find_all_properties(EmptyFields)
        expected = ['children', 'container_source', 'fields', 'modified', 'name', 'object_id', 'parent']
        self.assertListEqual(props, expected)

    def test_named_fields(self):
        class NamedFields(AbstractContainer):
            __fields__ = ('field1', 'field2')

            @docval({'name': 'field2', 'doc': 'field2 doc', 'type': str})
            def __init__(self, **kwargs):
                super().__init__('test name')
                self.field2 = kwargs['field2']

        self.assertTupleEqual(NamedFields.__fields__, ('field1', 'field2'))
        self.assertIs(NamedFields._get_fields(), NamedFields.__fields__)

        expected = ({'doc': None, 'name': 'field1'},
                    {'doc': 'field2 doc', 'name': 'field2'})
        self.assertTupleEqual(NamedFields.get_fields_conf(), expected)

        props = TestAbstractContainerFieldsConf.find_all_properties(NamedFields)
        expected = ['children', 'container_source', 'field1', 'field2', 'fields', 'modified', 'name', 'object_id',
                    'parent']
        self.assertListEqual(props, expected)

        f1_doc = getattr(NamedFields, 'field1').__doc__
        self.assertIsNone(f1_doc)

        f2_doc = getattr(NamedFields, 'field2').__doc__
        self.assertEqual(f2_doc, 'field2 doc')

        obj = NamedFields('field2 value')
        self.assertIsNone(obj.field1)
        self.assertEqual(obj.field2, 'field2 value')

        obj.field1 = 'field1 value'
        msg = "can't set attribute 'field2' -- already set"
        with self.assertRaisesWith(AttributeError, msg):
            obj.field2 = 'field2 value'
        obj.field2 = None  # None value does nothing
        self.assertEqual(obj.field2, 'field2 value')

    def test_with_doc(self):
        """Test that __fields__ related attributes are set correctly.

        Also test that the docstring for fields are not overridden by the docstring in the docval of __init__ if a doc
        is provided in cls.__fields__.
        """
        class NamedFieldsWithDoc(AbstractContainer):
            __fields__ = ({'name': 'field1', 'doc': 'field1 orig doc'},
                          {'name': 'field2', 'doc': 'field2 orig doc'})

            @docval({'name': 'field2', 'doc': 'field2 doc', 'type': str})
            def __init__(self, **kwargs):
                super().__init__('test name')
                self.field2 = kwargs['field2']

        expected = ({'doc': 'field1 orig doc', 'name': 'field1'},
                    {'doc': 'field2 orig doc', 'name': 'field2'})
        self.assertTupleEqual(NamedFieldsWithDoc.get_fields_conf(), expected)

        f1_doc = getattr(NamedFieldsWithDoc, 'field1').__doc__
        self.assertEqual(f1_doc, 'field1 orig doc')

        f2_doc = getattr(NamedFieldsWithDoc, 'field2').__doc__
        self.assertEqual(f2_doc, 'field2 orig doc')

    def test_not_settable(self):
        """Test that __fields__ related attributes are set correctly.

        Also test that the docstring for fields are not overridden by the docstring in the docval of __init__ if a doc
        is provided in cls.__fields__.
        """
        class NamedFieldsNotSettable(AbstractContainer):
            __fields__ = ({'name': 'field1', 'settable': True},
                          {'name': 'field2', 'settable': False})

        expected = ({'doc': None, 'name': 'field1', 'settable': True},
                    {'doc': None, 'name': 'field2', 'settable': False})
        self.assertTupleEqual(NamedFieldsNotSettable.get_fields_conf(), expected)

        obj = NamedFieldsNotSettable('test name')

        obj.field1 = 'field1 value'
        with self.assertRaisesWith(AttributeError, "can't set attribute"):
            obj.field2 = 'field2 value'

    def test_inheritance(self):
        class NamedFields(AbstractContainer):
            __fields__ = ({'name': 'field1', 'doc': 'field1 doc', 'settable': False}, )

        class NamedFieldsChild(NamedFields):
            __fields__ = ({'name': 'field2'}, )

        self.assertTupleEqual(NamedFieldsChild.__fields__, ('field1', 'field2'))
        self.assertIs(NamedFieldsChild._get_fields(), NamedFieldsChild.__fields__)

        expected = ({'doc': 'field1 doc', 'name': 'field1', 'settable': False},
                    {'doc': None, 'name': 'field2'})
        self.assertTupleEqual(NamedFieldsChild.get_fields_conf(), expected)

        props = TestAbstractContainerFieldsConf.find_all_properties(NamedFieldsChild)
        expected = ['children', 'container_source', 'field1', 'field2', 'fields', 'modified', 'name', 'object_id',
                    'parent']
        self.assertListEqual(props, expected)

    def test_inheritance_override(self):
        class NamedFields(AbstractContainer):
            __fields__ = ({'name': 'field1'}, )

        class NamedFieldsChild(NamedFields):
            __fields__ = ({'name': 'field1', 'doc': 'overridden field', 'settable': False}, )

        self.assertEqual(NamedFieldsChild._get_fields(), ('field1', ))
        ret = NamedFieldsChild.get_fields_conf()
        self.assertEqual(ret[0], {'name': 'field1', 'doc': 'overridden field', 'settable': False})

        # obj = NamedFieldsChild('test name')
        # with self.assertRaisesWith(AttributeError, "can't set attribute"):
        #     obj.field1 = 'field1 value'

    def test_mult_inheritance_base_mixin(self):
        class NamedFields(AbstractContainer):
            __fields__ = ({'name': 'field1', 'doc': 'field1 doc', 'settable': False}, )

        class BlankMixin:
            pass

        class NamedFieldsChild(NamedFields, BlankMixin):
            __fields__ = ({'name': 'field2'}, )

        self.assertTupleEqual(NamedFieldsChild.__fields__, ('field1', 'field2'))
        self.assertIs(NamedFieldsChild._get_fields(), NamedFieldsChild.__fields__)

    def test_mult_inheritance_base_container(self):
        class NamedFields(AbstractContainer):
            __fields__ = ({'name': 'field1', 'doc': 'field1 doc', 'settable': False}, )

        class BlankMixin:
            pass

        class NamedFieldsChild(BlankMixin, NamedFields):
            __fields__ = ({'name': 'field2'}, )

        self.assertTupleEqual(NamedFieldsChild.__fields__, ('field1', 'field2'))
        self.assertIs(NamedFieldsChild._get_fields(), NamedFieldsChild.__fields__)


class TestContainerFieldsConf(TestCase):

    def test_required_name(self):
        class ContainerRequiredName(Container):
            __fields__ = ({'name': 'field1', 'required_name': 'field1 value'}, )

            @docval({'name': 'field1', 'doc': 'field1 doc', 'type': None, 'default': None})
            def __init__(self, **kwargs):
                super().__init__('test name')
                self.field1 = kwargs['field1']

        msg = ("Field 'field1' on ContainerRequiredName has a required name and must be a subclass of "
               "AbstractContainer.")
        with self.assertRaisesWith(ValueError, msg):
            ContainerRequiredName('field1 value')

        obj1 = Container('test container')
        msg = "Field 'field1' on ContainerRequiredName must be named 'field1 value'."
        with self.assertRaisesWith(ValueError, msg):
            ContainerRequiredName(obj1)

        obj2 = Container('field1 value')
        obj3 = ContainerRequiredName(obj2)
        self.assertIs(obj3.field1, obj2)

        obj4 = ContainerRequiredName()
        self.assertIsNone(obj4.field1)

    def test_child(self):
        class ContainerWithChild(Container):
            __fields__ = ({'name': 'field1', 'child': True}, )

            @docval({'name': 'field1', 'doc': 'field1 doc', 'type': None, 'default': None})
            def __init__(self, **kwargs):
                super().__init__('test name')
                self.field1 = kwargs['field1']

        child_obj1 = Container('test child 1')
        obj1 = ContainerWithChild(child_obj1)
        self.assertIs(child_obj1.parent, obj1)

        child_obj2 = Container('test child 2')
        obj3 = ContainerWithChild((child_obj1, child_obj2))
        self.assertIs(child_obj1.parent, obj1)  # child1 parent is already set
        self.assertIs(child_obj2.parent, obj3)  # child1 parent is already set

        child_obj3 = Container('test child 3')
        obj4 = ContainerWithChild({'test child 3': child_obj3})
        self.assertIs(child_obj3.parent, obj4)

        obj2 = ContainerWithChild()
        self.assertIsNone(obj2.field1)


class TestChangeFieldsName(TestCase):

    def test_fields(self):
        class ContainerNewFields(Container):
            _fieldsname = '__newfields__'
            __newfields__ = ({'name': 'field1', 'doc': 'field1 doc'}, )

            @docval({'name': 'field1', 'doc': 'field1 doc', 'type': None, 'default': None})
            def __init__(self, **kwargs):
                super().__init__('test name')
                self.field1 = kwargs['field1']

        self.assertTupleEqual(ContainerNewFields.__newfields__, ('field1', ))
        self.assertIs(ContainerNewFields._get_fields(), ContainerNewFields.__newfields__)

        expected = ({'doc': 'field1 doc', 'name': 'field1'}, )
        self.assertTupleEqual(ContainerNewFields.get_fields_conf(), expected)

    def test_fields_inheritance(self):
        class ContainerOldFields(Container):
            __fields__ = ({'name': 'field1', 'doc': 'field1 doc'}, )

            @docval({'name': 'field1', 'doc': 'field1 doc', 'type': None, 'default': None})
            def __init__(self, **kwargs):
                super().__init__('test name')
                self.field1 = kwargs['field1']

        class ContainerNewFields(ContainerOldFields):
            _fieldsname = '__newfields__'
            __newfields__ = ({'name': 'field2', 'doc': 'field2 doc'}, )

            @docval({'name': 'field1', 'doc': 'field1 doc', 'type': None, 'default': None},
                    {'name': 'field2', 'doc': 'field2 doc', 'type': None, 'default': None})
            def __init__(self, **kwargs):
                super().__init__(kwargs['field1'])
                self.field2 = kwargs['field2']

        self.assertTupleEqual(ContainerNewFields.__newfields__, ('field1', 'field2'))
        self.assertIs(ContainerNewFields._get_fields(), ContainerNewFields.__newfields__)

        expected = ({'doc': 'field1 doc', 'name': 'field1'},
                    {'doc': 'field2 doc', 'name': 'field2'}, )
        self.assertTupleEqual(ContainerNewFields.get_fields_conf(), expected)
