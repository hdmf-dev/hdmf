Building API Classes
====================

After you have written an extension, you will need a Pythonic way to interact with the data model. To do this,
you will need to write some classes that represent the data you defined in your specification extensions.

The :py:mod:`hdmf.container` module defines two base classes that represent the primitive structures supported by
the schema. :py:class:`~hdmf.container.Data` represents datasets and :py:class:`~hdmf.container.Container`
represents groups. See the classes in the `:py:mod:hdmf.common` package for examples.

The register_class function/decorator
-------------------------------------

When defining a class that represents a *data_type* (i.e. anything that has a *data_type_def*)
from your extension, you can tell HDMF which *data_type* it represents using the function
:py:func:`~hdmf.common.register_class`. This class can be called on its own, or used as a class decorator. The
first argument should be the *data_type* and the second argument should be the *namespace* name.

The following example demonstrates how to register a class as the Python class representation of the
*data_type* "MyContainer" from the *namespace* "my_ns". The namespace must be loaded prior to the below code using
the :py:func:`~hdmf.common.load_namespaces` function.

.. code-block:: python

    from hdmf.common import register_class
    from hdmf.container import Container

    class MyContainer(Container):
        ...

    register_class(data_type='MyContainer', namespace='my_ns', container_cls=MyContainer)


Alternatively, you can use :py:func:`~hdmf.common.register_class` as a decorator.

.. code-block:: python

    from hdmf.common import register_class
    from hdmf.container import Container

    @type_map.register_class('MyContainer', 'my_ns')
    class MyContainer(Container):
        ...

:py:func:`~hdmf.common.register_class` is used with :py:class:`~hdmf.container.Data` the same way it is used with
:py:class:`~hdmf.container.Container`.
