=============
Zarr backend
=============

The Zarr backend is an alternative storage backend to store data using HDMF via the Zarr library.

Currently, the Zarr backend supports:

- Write/Read of basic datatypes, strings and compound data types
- Chunking
- Links
- Object references

The following features available in the h5py backend are not yet supported by the Zarr backend:

- Compression
- Region reference (see ``ZarrIO.__get_ref``)
- Iterative data write using AbstractDataChunkIterator
- loading/writing namespaces/specifications


**Note:** The link and reference in Zarr backend are OS independent. The backend reserves attributes to store the paths of the target objects in the two functions.
