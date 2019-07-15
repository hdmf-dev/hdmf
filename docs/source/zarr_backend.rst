============= 
Zarr backend 
=============

The Zarr backend is an alternative format to store data in HDMF except for h5py.

Currently, Zarr backend supports functions:

- Write/Read basic datatypes, strings and compound data types 
- Chunking
- Link
- Object reference

Functions which are in h5py backend but not supported by the Zarr backend are:

- Compression
- Region reference

**Note:** The link and reference in Zarr backend are OS independent. The backend reserves attributes to store the paths of the target objects in the two functions.
