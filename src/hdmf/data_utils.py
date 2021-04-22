import copy
from abc import ABCMeta, abstractmethod
from collections.abc import Iterable
from warnings import warn

import h5py
import numpy as np

from .utils import docval, getargs, popargs, docval_macro, get_data_shape


def append_data(data, arg):
    if isinstance(data, (list, DataIO)):
        data.append(arg)
        return data
    elif isinstance(data, np.ndarray):
        return np.append(data,  np.expand_dims(arg, axis=0), axis=0)
    elif isinstance(data, h5py.Dataset):
        shape = list(data.shape)
        shape[0] += 1
        data.resize(shape)
        data[-1] = arg
        return data
    else:
        msg = "Data cannot append to object of type '%s'" % type(data)
        raise ValueError(msg)


def extend_data(data, arg):
    if isinstance(data, (list, DataIO)):
        data.extend(arg)
        return data
    elif isinstance(data, np.ndarray):
        return np.vstack((data, arg))
    elif isinstance(data, h5py.Dataset):
        shape = list(data.shape)
        shape[0] += len(arg)
        data.resize(shape)
        data[-len(arg):] = arg
        return data
    else:
        msg = "Data cannot extend object of type '%s'" % type(data)
        raise ValueError(msg)


@docval_macro('array_data')
class AbstractDataChunkIterator(metaclass=ABCMeta):
    """
    Abstract iterator class used to iterate over DataChunks.

    Derived classes must ensure that all abstract methods and abstract properties are implemented, in
    particular, dtype, maxshape, __iter__, ___next__, recommended_chunk_shape, and recommended_data_shape.

    Iterating over AbstractContainer objects is not yet supported.
    """

    @abstractmethod
    def __iter__(self):
        """Return the iterator object"""
        raise NotImplementedError("__iter__ not implemented for derived class")

    @abstractmethod
    def __next__(self):
        r"""
        Return the next data chunk or raise a StopIteration exception if all chunks have been retrieved.

        HINT: numpy.s\_ provides a convenient way to generate index tuples using standard array slicing. This
        is often useful to define the DataChunk.selection of the current chunk

        :returns: DataChunk object with the data and selection of the current chunk
        :rtype: DataChunk
        """
        raise NotImplementedError("__next__ not implemented for derived class")

    @abstractmethod
    def recommended_chunk_shape(self):
        """
        Recommend the chunk shape for the data array.

        :return: NumPy-style shape tuple describing the recommended shape for the chunks of the target
                 array or None. This may or may not be the same as the shape of the chunks returned in the
                 iteration process.
        """
        raise NotImplementedError("recommended_chunk_shape not implemented for derived class")

    @abstractmethod
    def recommended_data_shape(self):
        """
        Recommend the initial shape for the data array.

        This is useful in particular to avoid repeated resized of the target array when reading from
        this data iterator. This should typically be either the final size of the array or the known
        minimal shape of the array.

        :return: NumPy-style shape tuple indicating the recommended initial shape for the target array.
                 This may or may not be the final full shape of the array, i.e., the array is allowed
                 to grow. This should not be None.
        """
        raise NotImplementedError("recommended_data_shape not implemented for derived class")

    @property
    @abstractmethod
    def dtype(self):
        """
        Define the data type of the array

        :return: NumPy style dtype or otherwise compliant dtype string
        """
        raise NotImplementedError("dtype not implemented for derived class")

    @property
    @abstractmethod
    def maxshape(self):
        """
        Property describing the maximum shape of the data array that is being iterated over

        :return: NumPy-style shape tuple indicating the maxiumum dimensions up to which the dataset may be
                 resized. Axes with None are unlimited.
        """
        raise NotImplementedError("maxshape not implemented for derived class")


class DataChunkIterator(AbstractDataChunkIterator):
    """
    Custom iterator class used to iterate over chunks of data.

    This default implementation of AbstractDataChunkIterator accepts any iterable and assumes that we iterate over
    a single dimension of the data array (default: the first dimension). DataChunkIterator supports buffered read,
    i.e., multiple values from the input iterator can be combined to a single chunk. This is
    useful for buffered I/O operations, e.g., to improve performance by accumulating data
    in memory and writing larger blocks at once.
    """

    __docval_init = (
        {'name': 'data', 'type': None, 'doc': 'The data object used for iteration', 'default': None},
        {'name': 'maxshape', 'type': tuple,
         'doc': 'The maximum shape of the full data array. Use None to indicate unlimited dimensions',
         'default': None},
        {'name': 'dtype', 'type': np.dtype, 'doc': 'The Numpy data type for the array', 'default': None},
        {'name': 'buffer_size', 'type': int, 'doc': 'Number of values to be buffered in a chunk', 'default': 1},
        {'name': 'iter_axis', 'type': int, 'doc': 'The dimension to iterate over', 'default': 0}
    )

    @docval(*__docval_init)
    def __init__(self, **kwargs):
        """Initialize the DataChunkIterator.
        If 'data' is an iterator and 'dtype' is not specified, then next is called on the iterator in order to determine
        the dtype of the data.
        """
        # Get the user parameters
        self.data, self.__maxshape, self.__dtype, self.buffer_size, self.iter_axis = getargs('data',
                                                                                             'maxshape',
                                                                                             'dtype',
                                                                                             'buffer_size',
                                                                                             'iter_axis',
                                                                                             kwargs)
        self.chunk_index = 0
        # Create an iterator for the data if possible
        if isinstance(self.data, Iterable):
            if self.iter_axis != 0 and isinstance(self.data, (list, tuple)):
                warn('Iterating over an axis other than the first dimension of list or tuple data '
                     'involves converting the data object to a numpy ndarray, which may incur a computational '
                     'cost.')
                self.data = np.asarray(self.data)
            if isinstance(self.data, np.ndarray):
                # iterate over the given axis by adding a new view on data (iter only works on the first dim)
                self.__data_iter = iter(np.moveaxis(self.data, self.iter_axis, 0))
            else:
                self.__data_iter = iter(self.data)
        else:
            self.__data_iter = None
        self.__next_chunk = DataChunk(None, None)
        self.__next_chunk_start = 0
        self.__first_chunk_shape = None
        # Determine the shape of the data if possible
        if self.__maxshape is None:
            # If the self.data object identifies its shape, then use it
            if hasattr(self.data, "shape"):
                self.__maxshape = self.data.shape
                # Avoid the special case of scalar values by making them into a 1D numpy array
                if len(self.__maxshape) == 0:
                    self.data = np.asarray([self.data, ])
                    self.__maxshape = self.data.shape
                    self.__data_iter = iter(self.data)
            # Try to get an accurate idea of __maxshape for other Python data structures if possible.
            # Don't just call get_data_shape for a generator as that would potentially trigger loading of all the data
            elif isinstance(self.data, list) or isinstance(self.data, tuple):
                self.__maxshape = get_data_shape(self.data, strict_no_data_load=True)

        # If we have a data iterator and do not know the dtype, then read the first chunk
        if self.__data_iter is not None and self.__dtype is None:
            self._read_next_chunk()

        # Determine the type of the data if possible
        if self.__next_chunk.data is not None:
            self.__dtype = self.__next_chunk.data.dtype
            self.__first_chunk_shape = get_data_shape(self.__next_chunk.data)

        # This should be done as a last resort only
        if self.__first_chunk_shape is None and self.__maxshape is not None:
            self.__first_chunk_shape = tuple(1 if i is None else i for i in self.__maxshape)

        if self.__dtype is None:
            raise Exception('Data type could not be determined. Please specify dtype in DataChunkIterator init.')

    @classmethod
    @docval(*__docval_init)
    def from_iterable(cls, **kwargs):
        return cls(**kwargs)

    def __iter__(self):
        """Return the iterator object"""
        return self

    def _read_next_chunk(self):
        """Read a single chunk from self.__data_iter and store the results in self.__next_chunk

        :returns: self.__next_chunk, i.e., the DataChunk object describing the next chunk
        """
        from h5py import Dataset as H5Dataset
        if isinstance(self.data, H5Dataset):
            start_index = self.chunk_index * self.buffer_size
            stop_index = start_index + self.buffer_size
            iter_data_bounds = self.data.shape[self.iter_axis]
            if start_index >= iter_data_bounds:
                self.__next_chunk = DataChunk(None, None)
            else:
                if stop_index > iter_data_bounds:
                    stop_index = iter_data_bounds

                selection = [slice(None)] * len(self.maxshape)
                selection[self.iter_axis] = slice(start_index, stop_index)
                selection = tuple(selection)
                self.__next_chunk.data = self.data[selection]
                self.__next_chunk.selection = selection
        elif self.__data_iter is not None:
            # the pieces in the buffer - first dimension consists of individual calls to next
            iter_pieces = []
            # offset of where data begins - shift the selection of where to place this chunk by this much
            curr_chunk_offset = 0
            read_next_empty = False
            while len(iter_pieces) < self.buffer_size:
                try:
                    dat = next(self.__data_iter)
                    if dat is None and len(iter_pieces) == 0:
                        # Skip forward in our chunk until we find data
                        curr_chunk_offset += 1
                    elif dat is None and len(iter_pieces) > 0:
                        # Stop iteration if we hit empty data while constructing our block
                        # Buffer may not be full.
                        read_next_empty = True
                        break
                    else:
                        # Add pieces of data to our buffer
                        iter_pieces.append(np.asarray(dat))
                except StopIteration:
                    break

            if len(iter_pieces) == 0:
                self.__next_chunk = DataChunk(None, None)  # signal end of iteration
            else:
                # concatenate all the pieces into the chunk along the iteration axis
                piece_shape = list(get_data_shape(iter_pieces[0]))
                piece_shape.insert(self.iter_axis, 1)  # insert the missing axis
                next_chunk_shape = piece_shape.copy()
                next_chunk_shape[self.iter_axis] *= len(iter_pieces)
                next_chunk_size = next_chunk_shape[self.iter_axis]

                # use the piece dtype because the actual dtype may not have been determined yet
                # NOTE: this could be problematic if a generator returns e.g. floats first and ints later
                self.__next_chunk.data = np.empty(next_chunk_shape, dtype=iter_pieces[0].dtype)
                self.__next_chunk.data = np.stack(iter_pieces, axis=self.iter_axis)

                selection = [slice(None)] * len(self.maxshape)
                selection[self.iter_axis] = slice(self.__next_chunk_start + curr_chunk_offset,
                                                  self.__next_chunk_start + curr_chunk_offset + next_chunk_size)
                self.__next_chunk.selection = tuple(selection)

                # next chunk should start at self.__next_chunk.selection[self.iter_axis].stop
                # but if this chunk stopped because of reading empty data, then this should be adjusted by 1
                self.__next_chunk_start = self.__next_chunk.selection[self.iter_axis].stop
                if read_next_empty:
                    self.__next_chunk_start += 1
        else:
            self.__next_chunk = DataChunk(None, None)

        self.chunk_index += 1
        return self.__next_chunk

    def __next__(self):
        r"""Return the next data chunk or raise a StopIteration exception if all chunks have been retrieved.

        HINT: numpy.s\_ provides a convenient way to generate index tuples using standard array slicing. This
        is often useful to define the DataChunk.selection of the current chunk

        :returns: DataChunk object with the data and selection of the current chunk
        :rtype: DataChunk

        """
        # If we have not already read the next chunk, then read it now
        if self.__next_chunk.data is None:
            self._read_next_chunk()
        # If we do not have any next chunk
        if self.__next_chunk.data is None:
            raise StopIteration
        # If this is the first time we see a chunk then remember the size of the first chunk
        if self.__first_chunk_shape is None:
            self.__first_chunk_shape = self.__next_chunk.data.shape
        # Keep the next chunk we need to return
        curr_chunk = DataChunk(self.__next_chunk.data,
                               self.__next_chunk.selection)
        # Remove the data for the next chunk from our list since we are returning it here.
        # This is to allow the GarbageCollector to remmove the data when it goes out of scope and avoid
        # having 2 full chunks in memory if not necessary
        self.__next_chunk.data = None
        # Return the current next chunk
        return curr_chunk

    next = __next__

    @docval(returns='Tuple with the recommended chunk shape or None if no particular shape is recommended.')
    def recommended_chunk_shape(self):
        """Recommend a chunk shape.

        To optimize iterative write the chunk should be aligned with the common shape of chunks returned by __next__
        or if those chunks are too large, then a well-aligned subset of those chunks. This may also be
        any other value in case one wants to recommend chunk shapes to optimize read rather
        than write. The default implementation returns None, indicating no preferential chunking option."""
        return None

    @docval(returns='Recommended initial shape for the full data. This should be the shape of the full dataset' +
                    'if known beforehand or alternatively the minimum shape of the dataset. Return None if no ' +
                    'recommendation is available')
    def recommended_data_shape(self):
        """Recommend an initial shape of the data. This is useful when progressively writing data and
        we want to recommend an initial size for the dataset"""
        if self.maxshape is not None:
            if np.all([i is not None for i in self.maxshape]):
                return self.maxshape
        return self.__first_chunk_shape

    @property
    def maxshape(self):
        """
        Get a shape tuple describing the maximum shape of the array described by this DataChunkIterator. If an iterator
        is provided and no data has been read yet, then the first chunk will be read (i.e., next will be called on the
        iterator) in order to determine the maxshape.

        :return: Shape tuple. None is used for dimenwions where the maximum shape is not known or unlimited.
        """
        if self.__maxshape is None:
            # If no data has been read from the iterator yet, read the first chunk and use it to determine the maxshape
            if self.__data_iter is not None and self.__next_chunk.data is None:
                self._read_next_chunk()

            # Determine maxshape from self.__next_chunk
            if self.__next_chunk.data is None:
                return None
            data_shape = get_data_shape(self.__next_chunk.data)
            self.__maxshape = list(data_shape)
            try:
                # Size of self.__next_chunk.data along self.iter_axis is not accurate for maxshape because it is just a
                # chunk. So try to set maxshape along the dimension self.iter_axis based on the shape of self.data if
                # possible. Otherwise, use None to represent an unlimited size
                if hasattr(self.data, '__len__') and self.iter_axis == 0:
                    # special case of 1-D array
                    self.__maxshape[0] = len(self.data)
                else:
                    self.__maxshape[self.iter_axis] = self.data.shape[self.iter_axis]
            except AttributeError:  # from self.data.shape
                self.__maxshape[self.iter_axis] = None
            self.__maxshape = tuple(self.__maxshape)

        return self.__maxshape

    @property
    def dtype(self):
        """
        Get the value data type

        :return: np.dtype object describing the datatype
        """
        return self.__dtype


class DataChunk:
    """
    Class used to describe a data chunk. Used in DataChunkIterator.
    """

    @docval({'name': 'data', 'type': np.ndarray,
             'doc': 'Numpy array with the data value(s) of the chunk', 'default': None},
            {'name': 'selection', 'type': None,
             'doc': 'Numpy index tuple describing the location of the chunk', 'default': None})
    def __init__(self, **kwargs):
        self.data, self.selection = getargs('data', 'selection', kwargs)

    def __len__(self):
        """Get the number of values in the data chunk"""
        if self.data is not None:
            return len(self.data)
        else:
            return 0

    def __getattr__(self, attr):
        """Delegate retrival of attributes to the data in self.data"""
        return getattr(self.data, attr)

    def __copy__(self):
        newobj = DataChunk(data=self.data,
                           selection=self.selection)
        return newobj

    def __deepcopy__(self, memo):
        result = DataChunk(data=copy.deepcopy(self.data),
                           selection=copy.deepcopy(self.selection))
        memo[id(self)] = result
        return result

    def astype(self, dtype):
        """Get a new DataChunk with the self.data converted to the given type"""
        return DataChunk(data=self.data.astype(dtype),
                         selection=self.selection)

    @property
    def dtype(self):
        """
        Data type of the values in the chunk

        :returns: np.dtype of the values in the DataChunk
        """
        return self.data.dtype


def assertEqualShape(data1,
                     data2,
                     axes1=None,
                     axes2=None,
                     name1=None,
                     name2=None,
                     ignore_undetermined=True):
    """
    Ensure that the shape of data1 and data2 match along the given dimensions

    :param data1: The first input array
    :type data1: List, Tuple, np.ndarray, DataChunkIterator etc.
    :param data2: The second input array
    :type data2: List, Tuple, np.ndarray, DataChunkIterator etc.
    :param name1: Optional string with the name of data1
    :param name2: Optional string with the name of data2
    :param axes1: The dimensions of data1 that should be matched to the dimensions of data2. Set to None to
                  compare all axes in order.
    :type axes1: int, Tuple of ints, List of ints, or None
    :param axes2: The dimensions of data2 that should be matched to the dimensions of data1. Must have
                  the same length as axes1. Set to None to compare all axes in order.
    :type axes1: int, Tuple of ints, List of ints, or None
    :param ignore_undetermined: Boolean indicating whether non-matching unlimited dimensions should be ignored,
               i.e., if two dimension don't match because we can't determine the shape of either one, then
               should we ignore that case or treat it as no match

    :return: Bool indicating whether the check passed and a string with a message about the matching process
    """
    # Create the base return object
    response = ShapeValidatorResult()
    # Determine the shape of the datasets
    response.shape1 = get_data_shape(data1)
    response.shape2 = get_data_shape(data2)
    # Determine the number of dimensions of the datasets
    num_dims_1 = len(response.shape1) if response.shape1 is not None else None
    num_dims_2 = len(response.shape2) if response.shape2 is not None else None
    # Determine the string names of the datasets
    n1 = name1 if name1 is not None else ("data1 at " + str(hex(id(data1))))
    n2 = name2 if name2 is not None else ("data2 at " + str(hex(id(data2))))
    # Determine the axes we should compare
    response.axes1 = list(range(num_dims_1)) if axes1 is None else ([axes1] if isinstance(axes1, int) else axes1)
    response.axes2 = list(range(num_dims_2)) if axes2 is None else ([axes2] if isinstance(axes2, int) else axes2)
    # Validate the array shape
    # 1) Check the number of dimensions of the arrays
    if (response.axes1 is None and response.axes2 is None) and num_dims_1 != num_dims_2:
        response.result = False
        response.error = 'NUM_DIMS_ERROR'
        response.message = response.SHAPE_ERROR[response.error]
        response.message += " %s is %sD and %s is %sD" % (n1, num_dims_1, n2, num_dims_2)
    # 2) Check that we have the same number of dimensions to compare on both arrays
    elif len(response.axes1) != len(response.axes2):
        response.result = False
        response.error = 'NUM_AXES_ERROR'
        response.message = response.SHAPE_ERROR[response.error]
        response.message += " Cannot compare axes %s with %s" % (str(response.axes1), str(response.axes2))
    # 3) Check that the datasets have sufficient numner of dimensions
    elif np.max(response.axes1) >= num_dims_1 or np.max(response.axes2) >= num_dims_2:
        response.result = False
        response.error = 'AXIS_OUT_OF_BOUNDS'
        response.message = response.SHAPE_ERROR[response.error]
        if np.max(response.axes1) >= num_dims_1:
            response.message += "Insufficient number of dimensions for %s -- Expected %i found %i" % \
                                (n1, np.max(response.axes1) + 1, num_dims_1)
        elif np.max(response.axes2) >= num_dims_2:
            response.message += "Insufficient number of dimensions for %s -- Expected %i found %i" % \
                                (n2, np.max(response.axes2) + 1, num_dims_2)
    # 4) Compare the length of the dimensions we should validate
    else:
        unmatched = []
        ignored = []
        for ax in zip(response.axes1, response.axes2):
            if response.shape1[ax[0]] != response.shape2[ax[1]]:
                if ignore_undetermined and (response.shape1[ax[0]] is None or response.shape2[ax[1]] is None):
                    ignored.append(ax)
                else:
                    unmatched.append(ax)
        response.unmatched = unmatched
        response.ignored = ignored

        # Check if everything checked out
        if len(response.unmatched) == 0:
            response.result = True
            response.error = None
            response.message = response.SHAPE_ERROR[response.error]
            if len(response.ignored) > 0:
                response.message += " Ignored undetermined axes %s" % str(response.ignored)
        else:
            response.result = False
            response.error = 'AXIS_LEN_ERROR'
            response.message = response.SHAPE_ERROR[response.error]
            response.message += "Axes %s with size %s of %s did not match dimensions %s with sizes %s of %s." % \
                                (str([un[0] for un in response.unmatched]),
                                 str([response.shape1[un[0]] for un in response.unmatched]),
                                 n1,
                                 str([un[1] for un in response.unmatched]),
                                 str([response.shape2[un[1]] for un in response.unmatched]),
                                 n2)
            if len(response.ignored) > 0:
                response.message += " Ignored undetermined axes %s" % str(response.ignored)
    return response


class ShapeValidatorResult:
    """Class for storing results from validating the shape of multi-dimensional arrays.

    This class is used to store results generated by ShapeValidator

    :ivar result: Boolean indicating whether results matched or not
    :type result: bool
    :ivar message: Message indicating the result of the matching procedure
    :type messaage: str, None
    """
    SHAPE_ERROR = {None: 'All required axes matched',
                   'NUM_DIMS_ERROR': 'Unequal number of dimensions.',
                   'NUM_AXES_ERROR': "Unequal number of axes for comparison.",
                   'AXIS_OUT_OF_BOUNDS': "Axis index for comparison out of bounds.",
                   'AXIS_LEN_ERROR': "Unequal length of axes."}
    """
    Dict where the Keys are the type of errors that may have occurred during shape comparison and the
    values are strings with default error messages for the type.
    """

    @docval({'name': 'result', 'type': bool, 'doc': 'Result of the shape validation', 'default': False},
            {'name': 'message', 'type': str,
             'doc': 'Message describing the result of the shape validation', 'default': None},
            {'name': 'ignored', 'type': tuple,
             'doc': 'Axes that have been ignored in the validaton process', 'default': tuple(), 'shape': (None,)},
            {'name': 'unmatched', 'type': tuple,
             'doc': 'List of axes that did not match during shape validation', 'default': tuple(), 'shape': (None,)},
            {'name': 'error', 'type': str, 'doc': 'Error that may have occurred. One of ERROR_TYPE', 'default': None},
            {'name': 'shape1', 'type': tuple,
             'doc': 'Shape of the first array for comparison', 'default': tuple(), 'shape': (None,)},
            {'name': 'shape2', 'type': tuple,
             'doc': 'Shape of the second array for comparison', 'default': tuple(), 'shape': (None,)},
            {'name': 'axes1', 'type': tuple,
             'doc': 'Axes for the first array that should match', 'default': tuple(), 'shape': (None,)},
            {'name': 'axes2', 'type': tuple,
             'doc': 'Axes for the second array that should match', 'default': tuple(), 'shape': (None,)},
            )
    def __init__(self, **kwargs):
        self.result, self.message, self.ignored, self.unmatched, \
            self.error, self.shape1, self.shape2, self.axes1, self.axes2 = getargs(
                'result', 'message', 'ignored', 'unmatched', 'error', 'shape1', 'shape2', 'axes1', 'axes2', kwargs)

    def __setattr__(self, key, value):
        """
        Overwrite to ensure that, e.g., error_message is not set to an illegal value.
        """
        if key == 'error':
            if value not in self.SHAPE_ERROR.keys():
                raise ValueError("Illegal error type. Error must be one of ShapeValidatorResult.SHAPE_ERROR: %s"
                                 % str(self.SHAPE_ERROR))
            else:
                super().__setattr__(key, value)
        elif key in ['shape1', 'shape2', 'axes1', 'axes2', 'ignored', 'unmatched']:  # Make sure we sore tuples
            super().__setattr__(key, tuple(value))
        else:
            super().__setattr__(key, value)

    def __getattr__(self, item):
        """
        Overwrite to allow dynamic retrival of the default message
        """
        if item == 'default_message':
            return self.SHAPE_ERROR[self.error]
        return self.__getattribute__(item)


@docval_macro('data')
class DataIO:
    """
    Base class for wrapping data arrays for I/O. Derived classes of DataIO are typically
    used to pass dataset-specific I/O parameters to the particular HDMFIO backend.
    """

    @docval({'name': 'data', 'type': 'array_data', 'doc': 'the data to be written', 'default': None})
    def __init__(self, **kwargs):
        data = popargs('data', kwargs)
        self.__data = data

    def get_io_params(self):
        """
        Returns a dict with the I/O parameters specified in this DataIO.
        """
        return dict()

    @property
    def data(self):
        """Get the wrapped data object"""
        return self.__data

    @data.setter
    def data(self, val):
        """Set the wrapped data object"""
        if self.__data is not None:
            raise ValueError("cannot overwrite 'data' on DataIO")
        self.__data = val

    def __copy__(self):
        """
        Define a custom copy method for shallow copy..

        This is needed due to delegation of __getattr__ to the data to
        ensure proper copy.

        :return: Shallow copy of self, ie., a new instance of DataIO wrapping the same self.data object
        """
        newobj = DataIO(data=self.data)
        return newobj

    def append(self, arg):
        self.__data = append_data(self.__data, arg)

    def extend(self, arg):
        self.__data = extend_data(self.__data, arg)

    def __deepcopy__(self, memo):
        """
        Define a custom copy method for deep copy.

        This is needed due to delegation of __getattr__ to the data to
        ensure proper copy.

        :param memo:
        :return: Deep copy of self, i.e., a new instance of DataIO wrapping a deepcopy of the
        self.data object.
        """
        result = DataIO(data=copy.deepcopy(self.__data))
        memo[id(self)] = result
        return result

    def __len__(self):
        """Number of values in self.data"""
        if not self.valid:
            raise InvalidDataIOError("Cannot get length of data. Data is not valid.")
        return len(self.data)

    def __bool__(self):
        if self.valid:
            if isinstance(self.data, AbstractDataChunkIterator):
                return True
            return len(self) > 0
        return False

    def __getattr__(self, attr):
        """Delegate attribute lookup to data object"""
        if attr == '__array_struct__' and not self.valid:
            # np.array() checks __array__ or __array_struct__ attribute dep. on numpy version
            raise InvalidDataIOError("Cannot convert data to array. Data is not valid.")
        if not self.valid:
            raise InvalidDataIOError("Cannot get attribute '%s' of data. Data is not valid." % attr)
        return getattr(self.data, attr)

    def __getitem__(self, item):
        """Delegate slicing to the data object"""
        if not self.valid:
            raise InvalidDataIOError("Cannot get item from data. Data is not valid.")
        return self.data[item]

    def __array__(self):
        """
        Support conversion of DataIO.data to a numpy array. This function is
        provided to improve transparent interoperability of DataIO with numpy.

        :return: An array instance of self.data
        """
        if not self.valid:
            raise InvalidDataIOError("Cannot convert data to array. Data is not valid.")
        if hasattr(self.data, '__array__'):
            return self.data.__array__()
        elif isinstance(self.data, DataChunkIterator):
            raise NotImplementedError("Conversion of DataChunkIterator to array not supported")
        else:
            # NOTE this may result in a copy of the array
            return np.asarray(self.data)

    def __next__(self):
        """Delegate iteration interface to data object"""
        if not self.valid:
            raise InvalidDataIOError("Cannot iterate on data. Data is not valid.")
        return self.data.__next__()

    def __iter__(self):
        """Delegate iteration interface to the data object"""
        if not self.valid:
            raise InvalidDataIOError("Cannot iterate on data. Data is not valid.")
        return self.data.__iter__()

    @property
    def valid(self):
        """bool indicating if the data object is valid"""
        return self.data is not None


class InvalidDataIOError(Exception):
    pass
