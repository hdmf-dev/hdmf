.. _api_docs:

==================
API Documentation
==================

.. include:: _autosummary/hdmf.rst


Common Data Structures
======================

The `hdmf-common-schema <https://hdmf-common-schema.readthedocs.io/>`_ defines a collection of reusable data
structures---e.g., :py:class:`~hdfm.common.table.DynamicTable` for extensible data tables---that are useful
for defining new data standards. The following modules define the user API for these common data types
defined in `hdmf-common-schema`_

.. autosummary::
   :toctree: _autosummary
   :recursive:

   hdmf.common
   hdmf.common.io

I/O
====

The following modules define I/O backend functionality for reading/writing data. HDMF defines with
:py:class:`~hdmf.backends.io.HDMFIO` a generic, extensible interface for I/O backends.
:py:class:~hdmf.backends.io.hdf5.h5tools.HDF5IO` then implements :py:class:`~hdmf.backends.io.HDMFIO` to
support storing HDMF data in HDF5. HDMFs decouples the  I/O backends from the data API and format schema.
I.e., an I/O backend only needs to be able to interact with :py:mod:`~hdmf.build.builders` describing
common data primitives (e.g., Groups, Datasets, Attributes etc.), but does not need to know anything
about the specifics of a particiular data standard.

.. autosummary::
   :toctree: _autosummary
   :recursive:

   hdmf.backends
   hdmf.backends.hdf5

Data translation
================

The role of the data translation is to translate and integrate data between data APIs (i.e., Containers),
format schema, and data I.O. Given a data Container the data translation generates :py:mod:`~hdmf.build.builders`
for I/O using the relevant :py:mod:`~hdmf.build.objectmapper` to translate attributes from the container
to definitions in a format schema. Conversely, given a collection of :py:mod:`~hdmf.build.builders`, the
data translation will construct corresponding frontend :py:mod:`~hdmf.container` using
:py:mod:`~hdmf.build.objectmapper` to translate fields in the format schema to :py:mod:`~hdmf.container` attributes.
The :py:mod:`~hdmf.build.manager` then defines functionality to manage the build process to translate containers
to builders and vice-versa.

.. autosummary::
   :toctree: _autosummary
   :recursive:

   hdmf.build

Data Format Specification
=========================

The following modules define data structures and functionality for specifying format schema
:py:mod:`~hdmf.spec.spec`, managing and reading collections of schema definitions via :py:mod:`~hdmf.spec.namespace`
and :py:mod:`~hdmf.spec.catalog`, and writing schema via :py:mod:`~hdmf.spec.write`.

.. autosummary::
   :toctree: _autosummary
   :recursive:

   hdmf.spec

Data API
========

The modules contained directly at the main hdmf level are used to create user APIs for data standards and
to enhance interaction with data.

.. autosummary::
   :toctree: _autosummary
   :recursive:

   hdmf
   hdmf.container
   hdmf.utils
   hdmf.data_utils
   hdmf.array
   hdmf.region
   hdmf.monitor
   hdmf.query
   hdmf.region

Testing and Validation Utilities
================================

Components used to facilitate the definition of unit tests, validation of data files, and validation of format schema.

.. autosummary::
   :toctree: _autosummary
   :recursive:

   hdmf.validate
   hdmf.testing


:ref:`modindex`
