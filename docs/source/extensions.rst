.. _extending-standard:

Extending Standards
===================

The following page will discuss how to extend a standard using HDMF.

.. _creating-extensions:

Creating new Extensions
-----------------------

Standards specified using HDMF are designed to be extended. Extension for a standard can be done so using classes
provided in the :py:mod:`hdmf.spec` module. The classes :py:class:`~hdmf.spec.spec.GroupSpec`,
:py:class:`~hdmf.spec.spec.DatasetSpec`, :py:class:`~hdmf.spec.spec.AttributeSpec`, and :py:class:`~hdmf.spec.spec.LinkSpec`
can be used to define custom types.

Attribute Specifications
^^^^^^^^^^^^^^^^^^^^^^^^

Specifying attributes is done with :py:class:`~hdmf.spec.spec.AttributeSpec`.

.. code-block:: python

    from hdmf.spec import AttributeSpec

    spec = AttributeSpec('bar', 'a value for bar', 'float')

Dataset Specifications
^^^^^^^^^^^^^^^^^^^^^^

Specifying datasets is done with :py:class:`~hdmf.spec.spec.DatasetSpec`.

.. code-block:: python

    from hdmf.spec import DatasetSpec

    spec = DatasetSpec('A custom data type',
                        name='qux',
                        attribute=[
                            AttributeSpec('baz', 'a value for baz', 'str'),
                        ],
                        shape=(None, None))


Using datasets to specify tables
++++++++++++++++++++++++++++++++

Tables can be specified using :py:class:`~hdmf.spec.spec.DtypeSpec`. To specify a table, provide a
list of :py:class:`~hdmf.spec.spec.DtypeSpec` objects to the *dtype* argument.

.. code-block:: python

    from hdmf.spec import DatasetSpec, DtypeSpec

    spec = DatasetSpec('A custom data type',
                        name='qux',
                        attribute=[
                            AttributeSpec('baz', 'a value for baz', 'str'),
                        ],
                        dtype=[
                            DtypeSpec('foo', 'column for foo', 'int'),
                            DtypeSpec('bar', 'a column for bar', 'float')
                        ])

Group Specifications
^^^^^^^^^^^^^^^^^^^^

Specifying groups is done with the :py:class:`~hdmf.spec.spec.GroupSpec` class.

.. code-block:: python

    from hdmf.spec import GroupSpec

    spec = GroupSpec('A custom data type',
                        name='quux',
                        attributes=[...],
                        datasets=[...],
                        groups=[...])

Data Type Specifications
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:py:class:`~hdmf.spec.spec.GroupSpec` and :py:class:`~hdmf.spec.spec.DatasetSpec` use the arguments `data_type_inc` and
`data_type_def` for declaring new types and extending existing types. New types are specified by setting the argument
`data_type_def`. New types can extend an existing type by specifying the argument `data_type_inc`.

Create a new type

.. code-block:: python

    from hdmf.spec import GroupSpec

    # A list of AttributeSpec objects to specify new attributes
    addl_attributes = [...]
    # A list of DatasetSpec objects to specify new datasets
    addl_datasets = [...]
    # A list of DatasetSpec objects to specify new groups
    addl_groups = [...]
    spec = GroupSpec('A custom data type',
                        attributes=addl_attributes,
                        datasets=addl_datasets,
                        groups=addl_groups,
                        data_type_def='MyNewType')

Extend an existing type

.. code-block:: python

    from hdmf.spec import GroupSpec

    # A list of AttributeSpec objects to specify additional attributes or attributes to be overridden
    addl_attributes = [...]
    # A list of DatasetSpec objects to specify additional datasets or datasets to be overridden
    addl_datasets = [...]
    # A list of GroupSpec objects to specify additional groups or groups to be overridden
    addl_groups = [...]
    spec = GroupSpec('An extended data type',
                        attributes=addl_attributes,
                        datasets=addl_datasets,
                        groups=addl_groups,
                        data_type_inc='SpikeEventSeries',
                        data_type_def='MyExtendedSpikeEventSeries')

Existing types can be instantiated by specifying `data_type_inc` alone.

.. code-block:: python

    from hdmf.spec import GroupSpec

    # use another GroupSpec object to specify that a group of type
    # ElectricalSeries should be present in the new type defined below
    addl_groups = [ GroupSpec('An included ElectricalSeries instance',
                                 data_type_inc='ElectricalSeries') ]

    spec = GroupSpec('An extended data type',
                        groups=addl_groups,
                        data_type_inc='SpikeEventSeries',
                        data_type_def='MyExtendedSpikeEventSeries')


Datasets can be extended in the same manner (with regard to `data_type_inc` and `data_type_def`,
by using the class :py:class:`~hdmf.spec.spec.DatasetSpec`.

.. _saving-extensions:

Saving Extensions
-----------------

Extensions are used by including them in a loaded namespace. Namespaces and extensions need to be saved to file
for downstream use. The class :py:class:`~hdmf.spec.write.NamespaceBuilder` can be used to create new namespace and
specification files.

Create a new namespace with extensions

.. code-block:: python

    from hdmf.spec import GroupSpec, NamespaceBuilder

    # create a builder for the namespace
    ns_builder = NamespaceBuilder("Extension for use in my laboratory", "mylab", ...)

    # create extensions
    ext1 = GroupSpec('A custom SpikeEventSeries interface',
                        attributes=[...]
                        datasets=[...],
                        groups=[...],
                        data_type_inc='SpikeEventSeries',
                        data_type_def='MyExtendedSpikeEventSeries')

    ext2 = GroupSpec('A custom EventDetection interface',
                        attributes=[...]
                        datasets=[...],
                        groups=[...],
                        data_type_inc='EventDetection',
                        data_type_def='MyExtendedEventDetection')


    # add the extension
    ext_source = 'mylab.specs.yaml'
    ns_builder.add_spec(ext_source, ext1)
    ns_builder.add_spec(ext_source, ext2)

    # include an existing namespace - this will include all specifications in that namespace
    ns_builder.include_namespace('collab_ns')

    # save the namespace and extensions
    ns_path = 'mylab.namespace.yaml'
    ns_builder.export(ns_path)


.. tip::

    Using the API to generate extensions (rather than writing YAML sources directly) helps avoid errors in the specification
    (e.g., due to missing required keys or invalid values) and ensure compliance of the extension definition with the
    HDMF specification language. It also helps with maintenance of extensions, e.g., if extensions have to be ported to
    newer versions of the `specification language <https://schema-language.readthedocs.io/en/latest/>`_
    in the future.

.. _incorporating-extensions:

Incorporating extensions
------------------------

HDMF supports extending existing data types.
Extensions must be registered with HDMF to be used for reading and writing of custom data types.

The following code demonstrates how to load custom namespaces.

.. code-block:: python

    from hdmf import load_namespaces
    namespace_path = 'my_namespace.yaml'
    load_namespaces(namespace_path)

.. note::

    This will register all namespaces defined in the file ``'my_namespace.yaml'``.

Container : Representing custom data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To read and write custom data, corresponding :py:class:`~hdmf.container.Container` classes must be associated with their
respective specifications. :py:class:`~hdmf.container.Container` classes are associated with their respective
specification using the decorator :py:func:`~hdmf.common.register_class`.

The following code demonstrates how to associate a specification with the :py:class:`~hdmf.container.Container` class
that represents it.

.. code-block:: python

    from hdmf.common import register_class
    from hdmf.container import Container

    @register_class('MyExtension', 'my_namespace')
    class MyExtensionContainer(Container):
        ...

:py:func:`~hdmf.common.register_class` can also be used as a function.

.. code-block:: python

    from hdmf.common import register_class
    from hdmf.container import Container

    class MyExtensionContainer(Container):
        ...

    register_class(data_type='MyExtension', namespace='my_namespace', container_cls=MyExtensionContainer)

If you do not have an :py:class:`~hdmf.container.Container` subclass to associate with your extension specification,
a dynamically created class is created by default.

To use the dynamic class, you will need to retrieve the class object using the function :py:func:`~hdmf.common.get_class`.
Once you have retrieved the class object, you can use it just like you would a statically defined class.

.. code-block:: python

    from hdmf.common import get_class
    MyExtensionContainer = get_class('my_namespace', 'MyExtension')
    my_ext_inst = MyExtensionContainer(...)


If using iPython, you can access documentation for the class's constructor using the help command.

ObjectMapper : Customizing the mapping between Container and the Spec
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If your :py:class:`~hdmf.container.Container` extension requires custom mapping of the
:py:class:`~hdmf.container.Container` class for reading and writing, you will need to implement and register a custom
:py:class:`~hdmf.build.objectmapper.ObjectMapper`.

:py:class:`~hdmf.build.objectmapper.ObjectMapper` extensions are registered with the decorator
:py:func:`~hdmf.common.register_map`.

.. code-block:: python

    from hdmf.common import register_map
    from hdmf.build import ObjectMapper

    @register_map(MyExtensionContainer)
    class MyExtensionMapper(ObjectMapper)
        ...

:py:func:`~hdmf.common.register_map` can also be used as a function.

.. code-block:: python

    from hdmf.common import register_map
    from hdmf.build import ObjectMapper

    class MyExtensionMapper(ObjectMapper)
        ...

    register_map(MyExtensionContainer, MyExtensionMapper)

.. tip::

    ObjectMappers allow you to customize how objects in the spec are mapped to attributes of your Container in
    Python. This is useful, e.g., in cases where you want to customize the default mapping.
    For an overview of the concepts of containers, spec, builders, object mappers in HDMF see also
    :ref:`software-architecture`


.. _documenting-extensions:

Documenting Extensions
----------------------

Coming soon!

Further Reading
---------------

* **Specification Language:** For a detailed overview of the specification language itself see https://hdmf-schema-language.readthedocs.io/en/latest/index.html
