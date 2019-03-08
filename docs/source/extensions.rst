.. _extending-standard:

Extending standards
===================

The following page will discuss how to extend a standard using HDMF.

.. note::

    A simple example demonstrating the creation and use of a custom extension is available as part of the
    tutorial :ref:`tutorial-extending-standard`.

.. _creating-extensions:

Creating new Extensions
-----------------------

Standards specified using HDMF are designed to be extended. Extension for a standard can be done so using classes provided in the :py:mod:`hdmf.spec` module.
The classes :py:class:`~hdmf.spec.GroupSpec`, :py:class:`~hdmf.spec.DatasetSpec`, :py:class:`~hdmf.spec.AttributeSpec`, and :py:class:`~hdmf.spec.LinkSpec`
can be used to define custom types.

Attribute Specifications
^^^^^^^^^^^^^^^^^^^^^^^^

Specifying attributes is done with :py:class:`~hdmf.spec.AttributeSpec`.

.. code-block:: python

    from hdmf.spec import AttributeSpec

    spec = AttributeSpec('bar', 'a value for bar', 'float')

Dataset Specifications
^^^^^^^^^^^^^^^^^^^^^^

Specifying datasets is done with :py:class:`~hdmf.spec.DatasetSpec`.

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

Tables can be specified using :py:class:`~hdmf.spec.DtypeSpec`. To specify a table, provide a
list of :py:class:`~hdmf.spec.DtypeSpec` objects to the *dtype* argument.

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

Specifying groups is done with the :py:class:`~hdmf.spec.GroupSpec` class.

.. code-block:: python

    from hdmf.spec import GroupSpec

    spec = GroupSpec('A custom data type',
                        name='quux',
                        attributes=[...],
                        datasets=[...],
                        groups=[...])

Neurodata Type Specifications
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

:py:class:`~hdmf.spec.GroupSpec` and :py:class:`~hdmf.spec.DatasetSpec` use the arguments `neurodata_type_inc` and `neurodata_type_def` for
declaring new types and extending existing types. New types are specified by setting the argument `neurodata_type_def`. New types can extend an existing type
by specifying the argument `neurodata_type_inc`.

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
                        neurodata_type_def='MyNewType')

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
                        neurodata_type_inc='SpikeEventSeries',
                        neurodata_type_def='MyExtendedSpikeEventSeries')

Existing types can be instantiated by specifying `neurodata_type_inc` alone.

.. code-block:: python

    from hdmf.spec import GroupSpec

    # use another GroupSpec object to specify that a group of type
    # ElectricalSeries should be present in the new type defined below
    addl_groups = [ GroupSpec('An included ElectricalSeries instance',
                                 neurodata_type_inc='ElectricalSeries') ]

    spec = GroupSpec('An extended data type',
                        groups=addl_groups,
                        neurodata_type_inc='SpikeEventSeries',
                        neurodata_type_def='MyExtendedSpikeEventSeries')


Datasets can be extended in the same manner (with regard to `neurodata_type_inc` and `neurodata_type_def`,
by using the class :py:class:`~hdmf.spec.DatasetSpec`.

.. _saving-extensions:

Saving Extensions
-----------------

Extensions are used by including them in a loaded namespace. Namespaces and extensions need to be saved to file
for downstream use. The class :py:class:`~hdmf.spec.NamespaceBuilder` can be used to create new namespace and
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
                        neurodata_type_inc='SpikeEventSeries',
                        neurodata_type_def='MyExtendedSpikeEventSeries')

    ext2 = GroupSpec('A custom EventDetection interface',
                        attributes=[...]
                        datasets=[...],
                        groups=[...],
                        neurodata_type_inc='EventDetection',
                        neurodata_type_def='MyExtendedEventDetection')


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

HDMF supports extending existing data types (See :ref:`extending-standard` for more details on creating extensions).
Extensions must be registered with HDMF to be used for reading and writing of custom neurodata types.

The following code demonstrates how to load custom namespaces.

.. code-block:: python

    from hdmf import load_namespaces
    namespace_path = 'my_namespace.yaml'
    load_namespaces(namespace_path)

.. note::

    This will register all namespaces defined in the file ``'my_namespace.yaml'``.

Container : Representing custom data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To read and write custom data, corresponding :py:class:`~hdmf.core.Container` classes must be associated with their respective specifications.
:py:class:`~hdmf.core.Container` classes are associated with their respective specification using the decorator :py:func:`~hdmf.register_class`.

The following code demonstrates how to associate a specification with the :py:class:`~hdmf.core.Container` class that represents it.

.. code-block:: python

    from hdmf import register_class
    @register_class('MyExtension', 'my_namespace')
    class MyExtensionContainer(Container):
        ...

:py:func:`~hdmf.register_class` can also be used as a function.

.. code-block:: python

    from hdmf import register_class
    class MyExtensionContainer(Container):
        ...
    register_class('my_namespace', 'MyExtension', MyExtensionContainer)

If you do not have an :py:class:`~hdmf.core.Container` subclass to associate with your extension specification,
a dynamically created class is created by default.

To use the dynamic class, you will need to retrieve the class object using the function :py:func:`~hdmf.get_class`.
Once you have retrieved the class object, you can use it just like you would a statically defined class.

.. code-block:: python

    from hdmf import get_class
    MyExtensionContainer = get_class('my_namespace', 'MyExtension')
    my_ext_inst = MyExtensionContainer(...)


If using iPython, you can access documentation for the class's constructor using the help command.

ObjectMapper : Customizing the mapping between Container and the Spec
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

If your :py:class:`~hdmf.core.Container` extension requires custom mapping of the :py:class:`~hdmf.core.Container`
class for reading and writing, you will need to implement and register a custom :py:class:`~hdmf..build.map.ObjectMapper`.

:py:class:`~hdmf..build.map.ObjectMapper` extensions are registered with the decorator :py:func:`~hdmf.register_map`.

.. code-block:: python

    from hdmf import register_map
    from form import ObjectMapper
    @register_map(MyExtensionContainer)
    class MyExtensionMapper(ObjectMapper)
        ...

:py:func:`~hdmf.register_map` can also be used as a function.

.. code-block:: python

    from hdmf import register_map
    from form import ObjectMapper
    class MyExtensionMapper(ObjectMapper)
        ...
    register_map(MyExtensionContainer, MyExtensionMapper)

.. tip::

    ObjectMappers allow you to customize how objects in the spec are mapped to attributes of your Container in
    Python. This is useful, e.g., in cases where you want ot customize the default mapping. For example in
    TimeSeries the attribute ``unit`` which is defined on the dataset ``data`` (i.e., ``data.unit``) would
    by default be mapped to the attribute ``data_unit`` on :py:class:`~hdmf.base.TimeSeries`. The ObjectMapper
    :py:class:`~hdmf.io.base.TimeSeriesMap` then changes this mapping to map ``data.unit`` to the attribute ``unit``
    on :py:class:`~hdmf.base.TimeSeries` . ObjectMappers also allow you to customize how constructor arguments
    for your ``Container`` are constructed. E.g., in TimeSeries instead of explicit ``timestamps`` we
    may only have a ``starting_time`` and ``rate``. In the ObjectMapper we could then construct ``timestamps``
    from this data on data load to always have ``timestamps`` available for the user.
    For an overview of the concepts of containers, spec, builders, object mappers in HDMF see also
    :ref:`software-architecture`


.. _documenting-extensions:

Documenting Extensions
----------------------

Using the same tools used to generate the documentation for the `NWB-N core format <https://nwb-schema.readthedocs.io/en/latest/>`_
one can easily generate documentation in HTML, PDF, ePub and many other format for extensions as well.

Code to generate this documentation is maintained in a separate repo: https://github.com/hdmf-dev/nwb-docutils. To use these utilities, install the package with pip:

.. code-block:: text

    pip install nwb-docutils

For the purpose of this example, we assume that our current directory has the following structure.


.. code-block:: text

    - my_extension/
      - my_extension_source/
          - mylab.namespace.yaml
          - mylab.specs.yaml
          - ...
          - docs/  (Optional)
              - mylab_description.rst
              - mylab_release_notes.rst

In addition to Python 3.x, you will also need ``sphinx`` (including the ``sphinx-quickstart`` tool) installed.
Sphinx is available here http://www.sphinx-doc.org/en/stable/install.html .

We can now create the sources of our documentation as follows:

.. code-block:: text

    python3 nwb_init_sphinx_extension_doc  \
                 --project test \
                 --author "Dr. Master Expert" \
                 --version "1.2.3" \
                 --release alpha \
                 --output my_extension_docs \
                 --spec_dir my_extension_source \
                 --namespace_filename mylab.namespace.yaml \
                 --default_namespace mylab
                 --external_description my_extension_source/docs/mylab_description.rst \  (Optional)
                 --external_release_notes my_extension_source/docs/mylab_release_notes.rst \  (Optional)

To automatically generate the RST documentation files from the YAML (or JSON) sources of the extension simply run:

.. code-block:: text

    cd my_extension_docs
    make apidoc

Finally, to generate the HTML version of the docs run:

.. code-block:: text

    make html

.. tip::

    Additional instructions for how to use and customize the extension documentations are also available
    in the ``Readme.md`` file that  ``init_sphinx_extension_doc.py`` automatically adds to the docs.

.. tip::

    See ``make help`` for a list of available options for building the documentation in many different
    output formats (e.g., PDF, ePub, LaTeX, etc.).

.. tip::

    See ``python3 init_sphinx_extension_doc.py --help`` for a complete list of option to customize the documentation
    directly during initialization.

.. tip::

    The above example included additional description and release note docs as part of the specification. These are
    included in the docs via ``.. include`` commands so that changes in those files are automatically picked up
    when rebuilding to docs. Alternatively, we can also add custom documentation directly to the docs.
    In this case the options ``--custom_description format_description.rst``
    and ``--custom_release_notes format_release_notes.rst`` of the ``init_sphinx_extension_doc.py`` script are useful
    to automatically generate the basic setup for those files so that one can easily start to add content directly
    without having to worry about the additional setup.


Further Reading
---------------

* **Using Extensions:** See :ref:`extending-standard` for an example on how to use extensions during read and write.
* **Specification Language:** For a detailed overview of the specification language itself see https://schema-language.readthedocs.io/en/latest/
