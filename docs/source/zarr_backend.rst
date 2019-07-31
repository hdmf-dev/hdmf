=============
Zarr backend
=============

The Zarr backend is an alternative storage backend to store data using HDMF via the Zarr library.


Currently, the Zarr backend supports:

- Write/Read of basic datatypes, strings and compound data types
- Chunking
- Links
- Object references
- Writing/loading namespaces/specifications

The following features available in :py:class:`~hdmf.backends.hdf5.h5tools.HDF5IO` are not yet supported
by :py:class:`~hdmf.backends.zarr.zarr_tools.ZarrIO` backend:

- Compression
- Region reference (see ``ZarrIO.__get_ref``)
- Iterative data write using AbstractDataChunkIterator

.. warning::

    The Zarr backend is currently experimental and may still change.

.. note::

    Links and reference are not natively supported by Zarr. Links and references are implemented
    in the Zarr backend in HDMF in an OS independent fashion. The backend reserves attributes to store the paths
    of the target objects in the two functions.

.. note::
    Attributes are stored as JSON documents in Zarr (using the DirectoryStore). As such, all attributes
    must be JSON serializable. The Zarr backend attempts to cast types to JSON serializable types as much as possible.

.. note::

    Currently the Zarr backend uses Zarr's DirectoryStore only. Other stores could be added but will require
    proper treatement of links and references for those backends.

.. note::

    For specific TODO items relate to the Zarr backend see ``hdmf/backends/zarr/zarr_tools.py