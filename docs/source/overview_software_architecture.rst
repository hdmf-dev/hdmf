.. _software-architecture:

Software Architecture
=====================

The main goal of HDMF is to enable users and developers to efficiently interact with the
hierarchical object data. The following figures provide an overview of the high-level architecture of
HDMF and functionality of the various components.

.. _fig-software-architecture:

.. figure:: figures/software_architecture.*
   :width: 100%
   :alt: HDMF Software Architecture

   Overview of the high-level software architecture of HDMF (click to enlarge).


.. _fig-software-architecture-purpose:

.. figure:: figures/software_architecture_design_choices.*
   :width: 100%
   :alt: HDMF Software Architecture Functions

   We choose a modular design for HDMF to enable flexibility and separate the
   various levels of standardizing hierarchical data (click to enlarge).

.. raw:: latex

    \clearpage \newpage


Main Concepts
-------------


.. _fig-software-architecture-concepts:

.. figure:: figures/software_architecture_concepts.*
   :width: 100%
   :alt: HDMF Software Architecture Concepts

   Overview of the main concepts/classes in HDMF and their location in the overall software architecture (click to enlarge).

Container
^^^^^^^^^

* In memory objects
* Interface for (most) applications
* Similar to a table row
* HDMF does not provide these. They are left for standards developers to define
  how users interact with data.
* There are two Container base classes:

   * :py:class:`~hdmf.container.Container` - represents a collection of objects
   * :py:class:`~hdmf.container.Data` - represents data

* **Main Module:** :py:class:`hdmf.container`

Builder
^^^^^^^

* Intermediary objects for I/O
* Interface for I/O
* Backend readers and writers must return and accept these
* There are different kinds of builders for different base types:

   * :py:class:`~hdmf.build.builders.GroupBuilder` - represents a collection of objects
   * :py:class:`~hdmf.build.builders.DatasetBuilder` - represents data
   * :py:class:`~hdmf.build.builders.LinkBuilder` - represents soft-links
   * :py:class:`~hdmf.build.builders.RegionBuilder` - represents a slice into data (Subclass of :py:class:`~hdmf.build.builders.DatasetBuilder`)

* **Main Module:** :py:class:`hdmf.build.builders`

Spec
^^^^

* Interact with format specifications
* Data structures to specify data types and what said types consist of
* Python representation for YAML specifications
* Interface for writing extensions or custom specification
* There are several main specification classes:

   * :py:class:`~hdmf.spec.spec.AttributeSpec` - specification for metadata
   * :py:class:`~hdmf.spec.spec.GroupSpec` - specification for a collection of
     objects (i.e. subgroups, datasets, link)
   * :py:class:`~hdmf.spec.spec.DatasetSpec` - specification for dataset (like
     and n-dimensional array). Specifies data type, dimensions, etc.
   * :py:class:`~hdmf.spec.spec.LinkSpec` - specification for link (like a POSIX
     soft link)
   * :py:class:`~hdmf.spec.spec.RefSpec` - specification for references
     (References are like links, but stored as data)
   * :py:class:`~hdmf.spec.spec.DtypeSpec` - specification for compound data
     types. Used to build complex data type specification, e.g., to define
     tables (used only in :py:class:`~hdmf.spec.spec.DatasetSpec` and
     correspondingly :py:class:`~hdmf.spec.spec.DatasetSpec`)

* **Main Modules:** :py:class:`hdmf.spec`

.. note::

   A ``data_type`` defines a reusable type in a format specification that can be
   referenced and used elsewhere in other specifications.  The specification of
   the standard is basically a collection of ``data_types``,

   * ``data_type_inc`` is used to include an existing type and
   * ``data_type_def`` is used to define a new type

   i.e, if both keys are defined then we create a new type that uses/inherits
   an existing type as a base.

ObjectMapper
^^^^^^^^^^^^

* Maintains the mapping between `Container`_ attributes and `Spec`_ components
* Provides a way of converting between `Container`_ and `Builder`_, while
  leaving standards developers with the flexibility of presenting data
  to users in a user-friendly manner, while storing data in an efficient manner
* ObjectMappers are constructed using a `Spec`_
* Ideally, one ObjectMapper for each data type
* Things an ObjectMapper should do:

   * Given a `Builder`_, return a Container representation
   * Given a `Container`_, return a Builder representation

* **Main Module:** :py:class:`hdmf.build.objectmapper`

.. _fig-software-architecture-mainconcepts:

.. figure:: figures/software_architecture_mainconcepts.*
   :width: 100%
   :alt: HDMF Software Architecture Main Concepts

   Relationship between `Container`_, `Builder`_, `ObjectMapper`_, and `Spec`_


Additional Concepts
-------------------

Namespace, NamespaceCatalog, NamespaceBuilder
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

* **Namespace**

   * A namespace for specifications
   * Necessary for making standards extensions and standard core specification
   * Contains basic info about who created extensions

* :py:class:`~hdmf.spec.namespace.NamespaceCatalog` -- A class for managing namespaces
* :py:class:`~hdmf.spec.write.NamespaceBuilder` -- A utility for building extensions


TypeMap
^^^^^^^

* Map between data types, Container classes (i.e. a Python class object) and corresponding ObjectMapper classes
* Constructed from a NamespaceCatalog
* Things a TypeMap does:

   * Given a data_type, return the associated Container class
   * Given a Container class, return the associated ObjectMapper

* HDMF has one of these classes:

   * the base class (i.e. :py:class:`~hdmf.build.manager.TypeMap`)

* TypeMaps can be merged, which is useful when combining extensions


BuildManager
^^^^^^^^^^^^

* Responsible for `memoizing <https://en.wikipedia.org/wiki/Memoization>`_ `Builder`_ and `Container`_
* Constructed from a `TypeMap`_
* HDMF only has one of these: :py:class:`hdmf.build.manager.BuildManager`

.. _fig-software-architecture-buildmanager:

.. figure:: figures/software_architecture_buildmanager.*
   :width: 100%
   :alt: HDMF Software Architecture BuildManager and TypeMap

   Overview of `BuildManager`_ (and `TypeMap`_) (click to enlarge).


HDMFIO
^^^^^^

* Abstract base class for I/O
* :py:class:`HDMFIO <hdmf.backends.io.HDMFIO>` has two key abstract methods:

   * :py:meth:`~hdmf.backends.io.HDMFIO.write_builder` – given a builder, write data to storage format
   * :py:meth:`~hdmf.backends.io.HDMFIO.read_builder` – given a handle to storage format, return builder representation
   * Others: :py:meth:`~hdmf.backends.io.HDMFIO.open` and :py:meth:`~hdmf.backends.io.HDMFIO.close`

* Constructed with a `BuildManager`_
* Extend this for creating a new I/O backend
* HDMF has one concrete form of this:

   * :py:class:`~hdmf.backends.hdf5.h5tools.HDF5IO` - reading and writing HDF5


.. _fig-software-architecture-hdmfio:

.. figure:: figures/software_architecture_hdmfio.*
   :width: 100%
   :alt: HDMF Software Architecture FormIO

   Overview of `HDMFIO`_ (click to enlarge).
