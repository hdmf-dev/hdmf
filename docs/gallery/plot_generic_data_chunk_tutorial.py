"""

.. _genericdci-tutorial:

GenericDataChunkIterator Tutorial
==================================

This is a tutorial for interacting with :py:class:`~hdmf.data_utils.GenericDataChunkIterator` objects. This tutorial
is written for beginners and does not describe the full capabilities and nuances
of the functionality. This tutorial is designed to give
you basic familiarity with how :py:class:`~hdmf.data_utils.GenericDataChunkIterator` works and help you get started
with creating a specific instance for your data format or API access pattern.

Introduction
------------
The :py:class:`~hdmf.data_utils.GenericDataChunkIterator` class represents a semi-abstract
version of a :py:class:`~hdmf.data_utils.AbstractDataChunkIterator` that automatically handles the selection
of buffer regions
and resolves communication of compatible chunk regions within a H5DataIO wrapper. It does not,
however, know how data (values) or metadata (data type, full shape) ought to be directly
accessed. This is by intention to be fully agnostic to a range of indexing methods and
format-independent APIs, rather than make strong assumptions about how data ranges are to be sliced.

Constructing a simple child class
---------------------------------
We will begin with a simple example case of data access to a standard Numpy array.
To create a :py:class:`~hdmf.data_utils.GenericDataChunkIterator` that accomplishes this,
we begin by defining our child class.
"""

# sphinx_gallery_thumbnail_path = 'figures/gallery_thumbnail_generic_data_chunk_tutorial.png'
import numpy as np

from hdmf.data_utils import GenericDataChunkIterator


class NumpyArrayDataChunkIterator(GenericDataChunkIterator):
    def __init__(self, array: np.ndarray, **kwargs):
        self.array = array
        super().__init__(**kwargs)

    def _get_data(self, selection):
        return self.array[selection]

    def _get_maxshape(self):
        return self.array.shape

    def _get_dtype(self):
        return self.array.dtype


# To instantiate this class on an array to allow iteration over buffer_shapes,
my_array = np.random.randint(low=0, high=10, size=(12, 6), dtype="int16")
my_custom_iterator = NumpyArrayDataChunkIterator(array=my_array)

# and this iterator now behaves as a standard Python generator (i.e., it can only be exhausted once)
# that returns DataChunk objects for each buffer.
for buffer in my_custom_iterator:
    print(buffer.data)

###############################################################################
# Intended use for advanced data I/O
# ----------------------------------
# Of course, the real use case for this class is intended for when the amount of data stored on a
# hard drive is larger than what can be read into RAM. Hence the goal is to read only an amount of
# data with a size in gigabytes (GB) at or below the `buffer_gb` argument (defaults to 1 GB).

# This design can be seen if we increase the amount of data in our example code
my_array = np.random.randint(low=0, high=10, size=(20000, 5000), dtype="int32")
my_custom_iterator = NumpyArrayDataChunkIterator(array=my_array, buffer_gb=0.2)

for j, buffer in enumerate(my_custom_iterator, start=1):
    print(f"Buffer number {j} returns data from selection: {buffer.selection}")

###############################################################################
# .. note::
#   Technically, in this example the total data is still fully loaded into RAM from the initial Numpy array.
#   A more accurate use case would be achieved from writing the test_array to a temporary file on your system
#   and loading it back with np.memmap, which is a subtype of Numpy arrays that do not immediately load the data.

###############################################################################
# Writing to an HDF5 file with full control of shape arguments
# ------------------------------------------------------------
# The true intention of returning data selections of this form, and within a DataChunk object,
# is to write these piecewise to an HDF5 dataset.

# This is where the importance of the underlying `chunk_shape` comes in, and why it is critical to performance
# that it perfectly subsets the `buffer_shape`.
import h5py

maxshape = (20000, 5000)
buffer_shape = (10000, 2500)
chunk_shape = (1000, 250)

my_array = np.random.randint(low=0, high=10, size=maxshape, dtype="int32")
my_custom_iterator = NumpyArrayDataChunkIterator(array=my_array, buffer_shape=buffer_shape, chunk_shape=chunk_shape)
out_file = "my_temporary_test_file.hdf5"
with h5py.File(name=out_file, mode="w") as f:
    dset = f.create_dataset(name="test", shape=maxshape, dtype="int16", chunks=my_custom_iterator.chunk_shape)
    for buffer in my_custom_iterator:
        dset[buffer.selection] = buffer.data
# Remember to remove the temporary file after running this and exploring the contents!

###############################################################################
# .. note::
#   Here we explicitly set the `chunks` value in the HDF5 dataset object; however, a nice part of the design of this
#   iterator is that when wrapped in a ``hdmf.backends.hdf5.h5_utils.H5DataIO`` that is called within a
#   ``hdmf.backends.hdf5.h5tools.HDF5IO`` context with a corresponding ``hdmf.container.Container``, these details
#   will be automatically parsed.

###############################################################################
# .. note::
#   There is some overlap here in nomenclature between HDMF and HDF5. The term *chunk* in both
#   HDMF and HDF5 refer to a subset of dataset, however, in HDF5 a chunk is a piece of dataset on disk,
#   whereas in the context of the  :py:class:`~hdmf.data_utils.DataChunk` iteration is a block of data in memory.
#   As such, the
#   requirements on the shape and size of chunks are different. In HDF5 these chunks are pieces
#   of a dataset that get compressed and cached together, and they should usually be small in size for
#   optimal performance  (typically 1 MB or less). In contrast, a :py:class:`~hdmf.data_utils.DataChunk` in
#   HDMF acts as a block of data for writing data to dataset, and spans multiple HDF5 chunks to improve performance.
#   This is achieved by avoiding repeat
#   updates to the same ``Chunk`` in the HDF5 file, :py:class:`~hdmf.data_utils.DataChunk` objects for write
#   should align with ``Chunks`` in the HDF5 file, i.e., the ``DataChunk.selection``
#   should fully cover one or more ``Chunks`` in the HDF5 file to avoid repeat updates to the same
#   ``Chunks`` in the HDF5 file. This is what the `buffer` of the :py:class`~hdmf.data_utils.GenericDataChunkIterator`
#   does, which upon each iteration returns a single
#   :py:class:`~hdmf.data_utils.DataChunk` object (by default > 1 GB) that perfectly spans many HDF5 chunks
#   (by default < 1 MB) to help reduce the number of small I/O operations
#   and help improve performance. In practice, the `buffer` should usually be even larger than the default, i.e,
#   as much free RAM as can be safely used.

###############################################################################
# Remove the test file
import os
if os.path.exists(out_file):
    os.remove(out_file)
