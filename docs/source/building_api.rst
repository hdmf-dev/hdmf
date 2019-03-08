Building API classes
====================

After you have written an extension, you will need a Pythonic way to interact with the data model. To do this,
you will need to write some classes that represent the data you defined in your specificiation extensions.
The :py:mod:`hdmf.core` module has various tools to make it easier to write classes that behave like
the rest of the HDMF API.

The :py:mod:`hdmf.container` defines two base classes that represent the primitive structures supported by
the schema. :py:class:`~hdmf.core.Data` represents datasets and :py:class:`~hdmf.core.Container`
represents groups. Additionally, :py:mod:`hdmf.core` offers subclasses of these two classes for
writing classes that come with more functionality.

``register_class``
------------------

When defining a class that represents a *data_type* (i.e. anything that has a *data_type_def*)
from your extension, you can tell HDMF which *data_type* it represents using the function
:py:func:`~hdmf.register_class`. This class can be called on its own, or used as a class decorator. The
first argument should be the *data_type* and the second argument should be the *namespace* name.

The following example demonstrates how to register a class as the Python class reprsentation of the
*data_type* "MyContainer" from the *namespace* "my_ns".

.. code-block:: python

    from hdmf import register_class
    from hdmf.core import Container
    from hdmf.build import TypeMap

    type_map = TypeMap(...)

    class MyContainer(Container):
        ...

    type_map.register_class('MyContainer', 'my_ns', MyContainer)


Alternatively, you can use :py:func:`~hdmf.register_class` as a decorator.

.. code-block:: python

    from hdmf import register_class
    from hdmf.core import Container
    from hdmf.build import TypeMap

    type_map = TypeMap(...)

    @type_map.register_class('MyContainer', 'my_ns')
    class MyContainer(Container):
        ...

:py:func:`~hdmf.build.Type.register_class` is used with :py:class:`~hdmf.core.Data` the same way it is used with
:py:class:`~hdmf.core.Container`.
