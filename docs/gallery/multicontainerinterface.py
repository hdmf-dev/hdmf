"""
MultiContainerInterface
========================

This is a guide to creating custom API classes with the ``MultiContainerInterface`` class.

"""

###############################################################################
# Introduction
# ------------
# The :py:class:`~hdmf.container.MultiContainerInterface` class provides an easy
# and convenient way to create standard methods for a container class that contains
# a collection of containers of a specified type. For
# example, let's say you want to define a class ``MyContainerHolder`` that
# contains a collection of ``MyContainer`` objects. By having ``MyContainerHolder``
# extend :py:class:`~hdmf.container.MultiContainerInterface`
# and specifying certain configuration settings
# in the class, your ``MyContainerHolder`` class would be generated with:
#
# 1. an attribute for a labelled dictionary that holds ``MyContainer`` objects
# 2. an ``__init__`` method to initialize ``MyContainerHolder`` with a collection of
#    ``MyContainer`` objects
# 3. a method to add ``MyContainer`` objects to the dictionary
# 4. access of items from the dictionary using ``__getitem__`` (square bracket notation)
# 5. a method to get ``MyContainer`` objects from the dictionary (optional)
# 6. a method to create ``MyContainer`` objects and add them to the dictionary (optional)
#

###############################################################################
# Specifying the class configuration
# ----------------------------------
# To specify the class configuration for a
# :py:class:`~hdmf.container.MultiContainerInterface` subclass, define the variable
# ``__clsconf__`` in the new class. ``__clsconf__`` should be set to a dictionary
# with three required keys, ``'attr'``, ``'type'``, and ``'add'``.
#
# The ``'attr'`` key should map to a string value that is the name of the attribute that
# will be created to hold the collection of container objects.
#
# The ``'type'`` key should map to a type or a tuple of types that says what objects
# are allowed in this collection.
#
# The ``'add'`` key should map to a string value that is the name of the
# method to be generated that allows users to add a container to the collection.

# sphinx_gallery_thumbnail_path = 'figures/gallery_thumbnail_multicontainerinterface.png'
from hdmf.container import Container, MultiContainerInterface


class ContainerHolder(MultiContainerInterface):

    __clsconf__ = {
        'attr': 'containers',
        'type': Container,
        'add': 'add_container',
    }


###############################################################################
# The above code will generate:
#
# 1. the attribute ``containers`` as a :py:class:`~hdmf.utils.LabelledDict` that
#    holds ``Container`` objects
# 2. the ``__init__`` method which accepts a collection of ``Container`` objects
# 3. the ``add_container`` method that allows users to add ``Container`` objects
#    to the ``containers`` dictionary.
#
# Here is an example of instantiating the new ``ContainerHolder`` class and
# using the generated add method.

obj1 = Container('obj1')
obj2 = Container('obj2')
holder1 = ContainerHolder()
holder1.add_container(obj1)
holder1.add_container(obj2)
holder1.containers  # this is a LabelledDict where the keys are the name of the container
# i.e., {'obj1': obj1, 'obj2': obj2}

###############################################################################
# Constructor options
# ----------------------------------
# The constructor accepts a dict/list/tuple of ``Container`` objects, a single
# ``Container`` object, or None. If a dict is passed, only the dict values are used.
# You can specify the argument as a keyword argument with the attribute name as
# the keyword argument key.

holder2 = ContainerHolder(obj1)
holder3 = ContainerHolder([obj1, obj2])
holder4 = ContainerHolder({'unused_key1': obj1, 'unused_key2': obj2})
holder5 = ContainerHolder(containers=obj1)

###############################################################################
# By default, the new class has the 'name' attribute set to the name of the class,
# but a user-specified name can be provided in the constructor.

named_holder = ContainerHolder(name='My Holder')

###############################################################################
# Adding containers to the collection
# -----------------------------------
# Similar to the constructor, the generated add method accepts a dict/list/tuple
# of ``Container`` objects or a single ``Container`` object. If a dict is passed,
# only the dict values are used.

holder6 = ContainerHolder()
holder6.add_container(obj1)

holder7 = ContainerHolder()
holder7.add_container([obj1, obj2])

holder8 = ContainerHolder()
holder8.add_container({'unused_key1': obj1, 'unused_key2': obj2})

holder9 = ContainerHolder()
holder9.add_container(containers=obj1)

###############################################################################
# Getting items from the collection
# -----------------------------------
# You can access a container in the collection by using the name of the
# container within square brackets. As a convenience, if there is
# only one item in the collection, you can use None within square brackets.

holder10 = ContainerHolder(obj1)
holder10['obj1']
holder10[None]

###############################################################################
# Getting items from the collection using a custom getter
# --------------------------------------------------------
# You can use the ``'get'`` key in ``__clsconf__`` to generate a getter method as
# an alternative to using the square bracket notation for accessing items from
# the collection. Like the square bracket notation, if there is only one item
# in the collection, you can omit the name or pass None to the getter method.
#
# The ``'get'`` key should map to a string value that is the name of the getter
# method to be generated. The ``'get'`` key in ``__clsconf__`` is optional.


class ContainerHolderWithGet(MultiContainerInterface):

    __clsconf__ = {
        'attr': 'containers',
        'type': Container,
        'add': 'add_container',
        'get': 'get_container',
    }


holder11 = ContainerHolderWithGet(obj1)
holder11.get_container('obj1')
holder11.get_container()

###############################################################################
# Creating and adding items to the collection using a custom create method
# ------------------------------------------------------------------------
# You can use the ``'create'`` key in ``__clsconf__`` to generate a create method
# as a convenience method so that users do not need to initialize the
# ``Container`` object and then add it to the collection. Those two steps are
# combined into one line. The arguments to the custom create method are the
# same as the arguments to the Container's ``__init__`` method, but the
# ``__init__`` method `must` be defined using :py:func:`~hdmf.utils.docval`.
# The created object will be returned by the create method.
#
# The ``'create'`` key should map to a string value that is the name of the create
# method to be generated. The ``'create'`` key in ``__clsconf__`` is optional.


class ContainerHolderWithCreate(MultiContainerInterface):

    __clsconf__ = {
        'attr': 'containers',
        'type': Container,
        'add': 'add_container',
        'create': 'create_container',
    }


holder12 = ContainerHolderWithCreate()
holder12.create_container('obj1')

###############################################################################
# Specifying multiple types allowed in the collection
# ------------------------------------------------------------------------
# The ``'type'`` key in ``__clsconf__`` allows specifying a single type or a
# list/tuple of types.
#
# You cannot specify the ``'create'`` key in ``__clsconf__``
# when multiple types are allowed in the collection because it cannot be
# determined which type to initialize.

from hdmf.container import Data


class ContainerHolderWithMultipleTypes(MultiContainerInterface):

    __clsconf__ = {
        'attr': 'things',
        'type': (Container, Data),
        'add': 'add_thing',
    }


###############################################################################
# Specifying multiple collections
# ------------------------------------------------------------------------
# You can support multiple collections in your
# :py:class:`~hdmf.container.MultiContainerInterface`
# subclass by setting the ``__clsconf__`` variable to a list of dicts instead
# of a single dict.
#
# When specifying multiple collections, square bracket notation access of items
# (i.e., calling ``__getitem__``) is not supported, because it is not clear
# which collection to get the desired item from.

from hdmf.container import Data


class MultiCollectionHolder(MultiContainerInterface):

    __clsconf__ = [
        {
            'attr': 'containers',
            'type': Container,
            'add': 'add_container',
        },
        {
            'attr': 'data',
            'type': Data,
            'add': 'add_data',
        },
    ]


###############################################################################
# Managing container parents
# ------------------------------------------------------------------------
# If the parent of the container being added is not already set, then the parent
# will be set to the containing object.

obj3 = Container('obj3')
holder13 = ContainerHolder(obj3)
obj3.parent  # this is holder13

###############################################################################
# :py:class:`~hdmf.utils.LabelledDict` objects support removal of an item using
# the del operator or the :py:meth:`~hdmf.utils.LabelledDict.pop`
# method. If the parent of the container being removed is the containing object,
# then its parent will be reset to None.

del holder13.containers['obj3']
obj3.parent  # this is back to None

###############################################################################
# Using a custom constructor
# ------------------------------------------------------------------------
# You can override the automatically generated constructor for your
# :py:class:`~hdmf.container.MultiContainerInterface` subclass.


class ContainerHolderWithCustomInit(MultiContainerInterface):

    __clsconf__ = {
        'attr': 'containers',
        'type': Container,
        'add': 'add_container',
    }

    def __init__(self, name, my_containers):
        super().__init__(name=name)
        self.containers = my_containers
        self.add_container(Container('extra_container'))


holder14 = ContainerHolderWithCustomInit('my_name', [obj1, obj2])
holder14.containers  # contains the 'extra_container' container
