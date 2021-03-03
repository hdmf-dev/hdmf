import inspect

from hdmf.container import Container, Data, MultiContainerInterface
from hdmf.testing import TestCase
from hdmf.utils import LabelledDict, get_docval


class OData(Data):

    pass


class Foo(MultiContainerInterface):

    __clsconf__ = [
        {
            'attr': 'containers',
            'add': 'add_container',
            'type': (Container, ),
            'get': 'get_container',
        },
        {
            'attr': 'data',
            'add': 'add_data',
            'type': (Data, OData),
        },
        {
            'attr': 'foo_data',
            'add': 'add_foo_data',
            'type': OData,
            'create': 'create_foo_data',
        },
        {
            'attr': 'things',
            'add': 'add_thing',
            'type': (Container, Data, OData),
        },
    ]


class FooSingle(MultiContainerInterface):

    __clsconf__ = {
        'attr': 'containers',
        'add': 'add_container',
        'type': (Container, ),
    }


class Baz(MultiContainerInterface):

    __containers = dict()

    __clsconf__ = [
        {
            'attr': 'containers',
            'add': 'add_container',
            'type': Container,
            'get': 'get_container',
        },
    ]

    # use custom keyword arguments
    def __init__(self, name, other_arg, my_containers):
        super().__init__(name=name)
        self.other_arg = other_arg
        self.containers = {'my ' + v.name: v for v in my_containers}

    @property
    def containers(self):
        return self.__containers

    @containers.setter
    def containers(self, value):
        self.__containers = value


class TestBasic(TestCase):

    def test_init_docval(self):
        """Test that the docval for the __init__ method is set correctly."""
        dv = get_docval(Foo.__init__)
        self.assertEqual(dv[0]['name'], 'containers')
        self.assertEqual(dv[1]['name'], 'data')
        self.assertEqual(dv[2]['name'], 'foo_data')
        self.assertEqual(dv[3]['name'], 'things')
        self.assertTupleEqual(dv[0]['type'], (list, tuple, dict, Container))
        self.assertTupleEqual(dv[1]['type'], (list, tuple, dict, Data, OData))
        self.assertTupleEqual(dv[2]['type'], (list, tuple, dict, OData))
        self.assertTupleEqual(dv[3]['type'], (list, tuple, dict, Container, Data, OData))
        self.assertEqual(dv[0]['doc'], 'Container to store in this interface')
        self.assertEqual(dv[1]['doc'], 'Data or OData to store in this interface')
        self.assertEqual(dv[2]['doc'], 'OData to store in this interface')
        self.assertEqual(dv[3]['doc'], 'Container, Data, or OData to store in this interface')
        for i in range(4):
            self.assertDictEqual(dv[i]['default'], {})
        self.assertEqual(dv[4]['name'], 'name')
        self.assertEqual(dv[4]['type'], str)
        self.assertEqual(dv[4]['doc'], 'the name of this container')
        self.assertEqual(dv[4]['default'], 'Foo')

    def test_add_docval(self):
        """Test that the docval for the add method is set correctly."""
        dv = get_docval(Foo.add_container)
        self.assertEqual(dv[0]['name'], 'containers')
        self.assertTupleEqual(dv[0]['type'], (list, tuple, dict, Container))
        self.assertEqual(dv[0]['doc'], 'the Container to add')
        self.assertFalse('default' in dv[0])

    def test_create_docval(self):
        """Test that the docval for the create method is set correctly."""
        dv = get_docval(Foo.create_foo_data)
        self.assertEqual(dv[0]['name'], 'name')
        self.assertEqual(dv[1]['name'], 'data')

    def test_getter_docval(self):
        """Test that the docval for the get method is set correctly."""
        dv = get_docval(Foo.get_container)
        self.assertEqual(dv[0]['doc'], 'the name of the Container')
        self.assertIsNone(dv[0]['default'])

    def test_getitem_docval(self):
        """Test that the docval for __getitem__ is set correctly."""
        dv = get_docval(Baz.__getitem__)
        self.assertEqual(dv[0]['doc'], 'the name of the Container')
        self.assertIsNone(dv[0]['default'])

    def test_attr_property(self):
        """Test that a property is created for the attribute."""
        properties = inspect.getmembers(Foo, lambda o: isinstance(o, property))
        match = [p for p in properties if p[0] == 'containers']
        self.assertEqual(len(match), 1)

    def test_attr_getter(self):
        """Test that the getter for the attribute dict returns a LabelledDict."""
        foo = Foo()
        self.assertTrue(isinstance(foo.containers, LabelledDict))

    def test_init_empty(self):
        """Test that initializing the MCI with no arguments initializes the attribute dict empty."""
        foo = Foo()
        self.assertDictEqual(foo.containers, {})
        self.assertEqual(foo.name, 'Foo')

    def test_init_multi(self):
        """Test that initializing the MCI with no arguments initializes the attribute dict empty."""
        obj1 = Container('obj1')
        data1 = Data('data1', [1, 2, 3])
        foo = Foo(containers=obj1, data=data1)
        self.assertDictEqual(foo.containers, {'obj1': obj1})
        self.assertDictEqual(foo.data, {'data1': data1})

    def test_init_custom_name(self):
        """Test that initializing the MCI with a custom name works."""
        foo = Foo(name='test_foo')
        self.assertEqual(foo.name, 'test_foo')

    # init, create, and setter calls add, so just test add
    def test_add_single(self):
        """Test that adding a container to the attribute dict correctly adds the container."""
        obj1 = Container('obj1')
        foo = Foo()
        foo.add_container(obj1)
        self.assertDictEqual(foo.containers, {'obj1': obj1})
        self.assertIs(obj1.parent, foo)

    def test_add_single_not_parent(self):
        """Test that adding a container with a parent to the attribute dict correctly adds the container."""
        obj1 = Container('obj1')
        obj2 = Container('obj2')
        obj1.parent = obj2
        foo = Foo()
        foo.add_container(obj1)
        self.assertDictEqual(foo.containers, {'obj1': obj1})
        self.assertIs(obj1.parent, obj2)

    def test_add_single_dup(self):
        """Test that adding a container to the attribute dict correctly adds the container."""
        obj1 = Container('obj1')
        foo = Foo(obj1)
        msg = "'obj1' already exists in Foo 'Foo'"
        with self.assertRaisesWith(ValueError, msg):
            foo.add_container(obj1)

    def test_add_list(self):
        """Test that adding a list to the attribute dict correctly adds the items."""
        obj1 = Container('obj1')
        obj2 = Container('obj2')
        foo = Foo()
        foo.add_container([obj1, obj2])
        self.assertDictEqual(foo.containers, {'obj1': obj1, 'obj2': obj2})

    def test_add_dict(self):
        """Test that adding a dict to the attribute dict correctly adds the input dict values."""
        obj1 = Container('obj1')
        obj2 = Container('obj2')
        foo = Foo()
        foo.add_container({'a': obj1, 'b': obj2})
        self.assertDictEqual(foo.containers, {'obj1': obj1, 'obj2': obj2})

    def test_attr_setter_none(self):
        """Test that setting the attribute dict to None does not alter the dict."""
        obj1 = Container('obj1')
        foo = Foo(obj1)
        foo.containers = None
        self.assertDictEqual(foo.containers, {'obj1': obj1})

    def test_remove_child(self):
        """Test that removing a child container from the attribute dict resets the parent to None."""
        obj1 = Container('obj1')
        foo = Foo(obj1)
        del foo.containers['obj1']
        self.assertDictEqual(foo.containers, {})
        self.assertIsNone(obj1.parent)

    def test_remove_non_child(self):
        """Test that removing a non-child container from the attribute dict resets the parent to None."""
        obj1 = Container('obj1')
        obj2 = Container('obj2')
        obj1.parent = obj2
        foo = Foo(obj1)
        del foo.containers['obj1']
        self.assertDictEqual(foo.containers, {})
        self.assertIs(obj1.parent, obj2)

    def test_getter_empty(self):
        """Test that calling the getter with no args and no items in the attribute dict raises an error."""
        foo = Foo()
        msg = "containers of Foo 'Foo' is empty."
        with self.assertRaisesWith(ValueError, msg):
            foo.get_container()

    def test_getter_none(self):
        """Test that calling the getter with no args and one item in the attribute returns the item."""
        obj1 = Container('obj1')
        foo = Foo(obj1)
        self.assertIs(foo.get_container(), obj1)

    def test_getter_none_multiple(self):
        """Test that calling the getter with no args and multiple items in the attribute dict raises an error."""
        obj1 = Container('obj1')
        obj2 = Container('obj2')
        foo = Foo([obj1, obj2])
        msg = "More than one element in containers of Foo 'Foo' -- must specify a name."
        with self.assertRaisesWith(ValueError, msg):
            foo.get_container()

    def test_getter_name(self):
        """Test that calling the getter with a correct key works."""
        obj1 = Container('obj1')
        foo = Foo(obj1)
        self.assertIs(foo.get_container('obj1'), obj1)

    def test_getter_name_not_found(self):
        """Test that calling the getter with a key not in the attribute dict raises a KeyError."""
        foo = Foo()
        msg = "\"'obj1' not found in containers of Foo 'Foo'.\""
        with self.assertRaisesWith(KeyError, msg):
            foo.get_container('obj1')

    def test_getitem_multiconf(self):
        """Test that classes with multiple attribute configurations cannot use getitem."""
        foo = Foo()
        msg = "'Foo' object is not subscriptable"
        with self.assertRaisesWith(TypeError, msg):
            foo['aa']

    def test_getitem(self):
        """Test that getitem works."""
        obj1 = Container('obj1')
        foo = FooSingle(obj1)
        self.assertIs(foo['obj1'], obj1)

    def test_getitem_single_none(self):
        """Test that getitem works wwhen there is a single item and no name is given to getitem."""
        obj1 = Container('obj1')
        foo = FooSingle(obj1)
        self.assertIs(foo[None], obj1)

    def test_getitem_empty(self):
        """Test that an error is raised if the attribute dict is empty and no name is given to getitem."""
        foo = FooSingle()
        msg = "FooSingle 'FooSingle' is empty."
        with self.assertRaisesWith(ValueError, msg):
            foo[None]

    def test_getitem_multiple(self):
        """Test that an error is raised if the attribute dict has multiple values and no name is given to getitem."""
        obj1 = Container('obj1')
        obj2 = Container('obj2')
        foo = FooSingle([obj1, obj2])
        msg = "More than one Container in FooSingle 'FooSingle' -- must specify a name."
        with self.assertRaisesWith(ValueError, msg):
            foo[None]

    def test_getitem_not_found(self):
        """Test that a KeyError is raised if the key is not found using getitem."""
        obj1 = Container('obj1')
        foo = FooSingle(obj1)
        msg = "\"'obj2' not found in FooSingle 'FooSingle'.\""
        with self.assertRaisesWith(KeyError, msg):
            foo['obj2']


class TestOverrideInit(TestCase):

    def test_override_init(self):
        """Test that overriding __init__ works."""
        obj1 = Container('obj1')
        obj2 = Container('obj2')
        containers = [obj1, obj2]

        baz = Baz(name='test_baz', other_arg=1, my_containers=containers)
        self.assertEqual(baz.name, 'test_baz')
        self.assertEqual(baz.other_arg, 1)

    def test_override_property(self):
        """Test that overriding the attribute property works."""
        obj1 = Container('obj1')
        obj2 = Container('obj2')
        containers = [obj1, obj2]
        baz = Baz(name='test_baz', other_arg=1, my_containers=containers)
        self.assertDictEqual(baz.containers, {'my obj1': obj1, 'my obj2': obj2})
        self.assertFalse(isinstance(baz.containers, LabelledDict))
        self.assertIs(baz.get_container('my obj1'), obj1)
        baz.containers = {}
        self.assertDictEqual(baz.containers, {})


class TestNoClsConf(TestCase):

    def test_mci_init(self):
        """Test that MultiContainerInterface cannot be instantiated."""
        msg = "Can't instantiate class MultiContainerInterface."
        with self.assertRaisesWith(TypeError, msg):
            MultiContainerInterface(name='a')

    def test_init_no_cls_conf(self):
        """Test that defining an MCI subclass without __clsconf__ raises an error."""

        class Bar(MultiContainerInterface):
            pass

        msg = ("MultiContainerInterface subclass Bar is missing __clsconf__ attribute. Please check that "
               "the class is properly defined.")
        with self.assertRaisesWith(TypeError, msg):
            Bar(name='a')

    def test_init_superclass_no_cls_conf(self):
        """Test that a subclass of an MCI class without a __clsconf__ can be initialized."""

        class Bar(MultiContainerInterface):
            pass

        class Qux(Bar):

            __clsconf__ = {
                'attr': 'containers',
                'add': 'add_container',
                'type': Container,
            }

        obj1 = Container('obj1')
        qux = Qux(obj1)
        self.assertDictEqual(qux.containers, {'obj1': obj1})


class TestBadClsConf(TestCase):

    def test_wrong_type(self):
        """Test that an error is raised if __clsconf__ is missing the add key."""

        msg = "'__clsconf__' for MultiContainerInterface subclass Bar must be a dict or a list of dicts."
        with self.assertRaisesWith(TypeError, msg):

            class Bar(MultiContainerInterface):

                __clsconf__ = (
                    {
                        'attr': 'data',
                        'add': 'add_data',
                        'type': (Data, ),
                    },
                )

    def test_missing_add(self):
        """Test that an error is raised if __clsconf__ is missing the add key."""

        msg = "MultiContainerInterface subclass Bar is missing 'add' key in __clsconf__"
        with self.assertRaisesWith(ValueError, msg):

            class Bar(MultiContainerInterface):

                __clsconf__ = {}

    def test_missing_attr(self):
        """Test that an error is raised if __clsconf__ is missing the attr key."""

        msg = "MultiContainerInterface subclass Bar is missing 'attr' key in __clsconf__"
        with self.assertRaisesWith(ValueError, msg):

            class Bar(MultiContainerInterface):

                __clsconf__ = {
                    'add': 'add_container',
                }

    def test_missing_type(self):
        """Test that an error is raised if __clsconf__ is missing the type key."""

        msg = "MultiContainerInterface subclass Bar is missing 'type' key in __clsconf__"
        with self.assertRaisesWith(ValueError, msg):

            class Bar(MultiContainerInterface):

                __clsconf__ = {
                    'add': 'add_container',
                    'attr': 'containers',
                }

    def test_create_multiple_types(self):
        """Test that an error is raised if __clsconf__ specifies 'create' key with multiple types."""

        msg = ("Cannot specify 'create' key in __clsconf__ for MultiContainerInterface subclass Bar "
               "when 'type' key is not a single type")
        with self.assertRaisesWith(ValueError, msg):

            class Bar(MultiContainerInterface):

                __clsconf__ = {
                    'attr': 'data',
                    'add': 'add_data',
                    'type': (Data, ),
                    'create': 'create_data',
                }

    def test_missing_add_multi(self):
        """Test that an error is raised if one item of a __clsconf__ list is missing the add key."""

        msg = "MultiContainerInterface subclass Bar is missing 'add' key in __clsconf__ at index 1"
        with self.assertRaisesWith(ValueError, msg):

            class Bar(MultiContainerInterface):

                __clsconf__ = [
                    {
                        'attr': 'data',
                        'add': 'add_data',
                        'type': (Data, ),
                    },
                    {}
                ]

    def test_missing_attr_multi(self):
        """Test that an error is raised if one item of a __clsconf__ list is missing the attr key."""

        msg = "MultiContainerInterface subclass Bar is missing 'attr' key in __clsconf__ at index 1"
        with self.assertRaisesWith(ValueError, msg):

            class Bar(MultiContainerInterface):

                __clsconf__ = [
                    {
                        'attr': 'data',
                        'add': 'add_data',
                        'type': (Data, ),
                    },
                    {
                        'add': 'add_container',
                    }
                ]

    def test_missing_type_multi(self):
        """Test that an error is raised if one item of a __clsconf__ list is missing the type key."""

        msg = "MultiContainerInterface subclass Bar is missing 'type' key in __clsconf__ at index 1"
        with self.assertRaisesWith(ValueError, msg):

            class Bar(MultiContainerInterface):

                __clsconf__ = [
                    {
                        'attr': 'data',
                        'add': 'add_data',
                        'type': (Data, ),
                    },
                    {
                        'add': 'add_container',
                        'attr': 'containers',
                    }
                ]

    def test_create_multiple_types_multi(self):
        """Test that an error is raised if one item of a __clsconf__ list specifies 'create' key with multiple types."""

        msg = ("Cannot specify 'create' key in __clsconf__ for MultiContainerInterface subclass Bar "
               "when 'type' key is not a single type at index 1")
        with self.assertRaisesWith(ValueError, msg):

            class Bar(MultiContainerInterface):

                __clsconf__ = [
                    {
                        'attr': 'data',
                        'add': 'add_data',
                        'type': (Data, ),
                    },
                    {
                        'add': 'add_container',
                        'attr': 'containers',
                        'type': (Container, ),
                        'create': 'create_container',
                    }
                ]
