from zarr.hierarchy import Group
import zarr
import numcodecs
import numpy as np
from collections import Iterable, deque
import json
import logging

from ...data_utils import DataIO
from ...utils import docval, getargs, call_docval_func  # , popargs

from ...spec import SpecWriter, SpecReader


class ZarrIODataChunkIteratorQueue(deque):
    """
    Helper class used by ZarrIO to manage the write for DataChunkIterators

    Each queue element must be a tupple of two elements:
    1) the dataset to write to and 2) the AbstractDataChunkIterator with the data
    """
    def __init__(self):
        self.logger = logging.getLogger('%s.%s' % (self.__class__.__module__, self.__class__.__qualname__))
        super().__init__()

    def __write_chunk(cls, dset, data):
        """
        Internal helper function used to read a chunk from the given DataChunkIterator
        and write it to the given Dataset

        :param dset: The Dataset to write to
        :param data: The DataChunkIterator to read from
        :return: True of a chunk was written, False otherwise
        :rtype: bool
        """
        # Read the next data block
        try:
            chunk_i = next(data)
        except StopIteration:
            return False
        # Determine the minimum array size required to store the chunk
        min_bounds = chunk_i.get_min_bounds()
        resize_required = False
        min_shape = list(dset.shape)
        if len(min_bounds) > len(dset.shape):
            raise ValueError("Shape of data chunk does not match the shape of the dataset")
        else:
            for si, sv in enumerate(dset.shape):
                if si < len(min_bounds):
                    if sv < min_bounds[si]:
                        min_shape[si] = min_bounds[si]
                        resize_required = True
                else:
                    break

        # Expand the dataset if needed
        if resize_required:
            dset.resize(min_shape)
        # Write the data
        dset[chunk_i.selection] = chunk_i.data
        # Chunk written and we need to continue
        return True

    def exhaust_queue(self):
        """
        Read and write from any queued DataChunkIterators in a round-robin fashion
        """
        # Iterate through our queue and write data chunks in a round-robin fashion until all iterators are exhausted
        self.logger.debug("Exhausting DataChunkIterator from queue (length %d)" % len(self))
        while len(self) > 0:
            dset, data = self.popleft()
            if self.__write_chunk(dset, data):
                self.append(dataset=dset, data=data)
        self.logger.debug("Exhausted DataChunkIterator from queue (length %d)" % len(self))

    def append(self, dataset, data):
        """
        Append a value to the queue

        :param dataset: The dataset where the DataChunkIterator is written to
        :type dataset: Zarr array
        :param data: DataChunkIterator with the data to be written
        :type data: AbstractDataChunkIterator
        """
        super().append((dataset, data))


class ZarrSpecWriter(SpecWriter):
    """
    Class used to write format specs to Zarr
    """

    @docval({'name': 'group', 'type': Group, 'doc': 'the Zarr file to write specs to'})
    def __init__(self, **kwargs):
        self.__group = getargs('group', kwargs)

    @staticmethod
    def stringify(spec):
        """
        Converts a spec into a JSON string to write to a dataset
        """
        return json.dumps(spec, separators=(',', ':'))

    def __write(self, d, name):
        data = self.stringify(d)
        dset = self.__group.require_dataset(name,
                                            shape=(1, ),
                                            dtype=object,
                                            object_codec=numcodecs.JSON(),
                                            compressor=None)
        dset.attrs['zarr_dtype'] = 'scalar'
        dset[0] = data
        return dset

    def write_spec(self, spec, path):
        """Write a spec to the given path"""
        return self.__write(spec, path)

    def write_namespace(self, namespace, path):
        """Write a namespace to the given path"""
        return self.__write({'namespaces': [namespace]}, path)


class ZarrSpecReader(SpecReader):
    """
    Class to read format specs from Zarr
    """

    @docval({'name': 'group', 'type': Group, 'doc': 'the Zarr file to read specs from'},
            {'name': 'source', 'type': str, 'doc': 'the path spec files are relative to', 'default': '.'})
    def __init__(self, **kwargs):
        self.__group, source = getargs('group', 'source', kwargs)
        super_kwargs = {'source': source}
        call_docval_func(super(ZarrSpecReader, self).__init__, super_kwargs)

    def __read(self, path):
        s = self.__group[path][0]
        d = json.loads(s)
        return d

    def read_spec(self, spec_path):
        """Read a spec from the given path"""
        return self.__read(spec_path)

    def read_namespace(self, ns_path):
        """Read a namespace from the given path"""
        ret = self.__read(ns_path)
        ret = ret['namespaces']
        return ret


class ZarrDataIO(DataIO):
    """
    Wrap data arrays for write via ZarrIO to customize I/O behavior, such as compression and chunking
    for data arrays.
    """
    @docval({'name': 'data',
             'type': (np.ndarray, list, tuple, zarr.Array, Iterable),
             'doc': 'the data to be written. NOTE: If an zarr.Array is used, all other settings but link_data' +
                    ' will be ignored as the dataset will either be linked to or copied as is in ZarrIO.'},
            {'name': 'chunks',
             'type': (list, tuple),
             'doc': 'Chunk shape',
             'default': None},
            {'name': 'fillvalue',
             'type': None,
             'doc': 'Value to be returned when reading uninitialized parts of the dataset',
             'default': None},
            {'name': 'compressor',
             'type': numcodecs.abc.Codec,
             'doc': 'Zarr compressor filter to be used',
             'default': None},
            {'name': 'filters',
             'type': (list, tuple),
             'doc': 'One or more Zarr-supported codecs used to transform data prior to compression.',
             'default': None},
            {'name': 'link_data',
             'type': bool,
             'doc': 'If data is an zarr.Array should it be linked to or copied. NOTE: This parameter is only ' +
                    'allowed if data is an zarr.Array',
             'default': False}
            )
    def __init__(self, **kwargs):
        # TODO Need to add error checks and warnings to ZarrDataIO to check for parameter collisions and add tests
        data, chunks, fill_value, compressor, filters, self.__link_data = getargs(
            'data', 'chunks', 'fillvalue', 'compressor', 'filters', 'link_data', kwargs)
        call_docval_func(super(ZarrDataIO, self).__init__, kwargs)
        if not isinstance(data, zarr.Array) and self.__link_data:
            self.__link_data = False
        self.__iosettings = dict()
        if chunks is not None:
            self.__iosettings['chunks'] = chunks
        if fill_value is not None:
            self.__iosettings['fill_value'] = fill_value
        if compressor is not None:
            self.__iosettings['compressor'] = compressor
        if filters is not None:
            self.__iosettings['filters'] = filters

    @property
    def link_data(self):
        return self.__link_data

    @property
    def io_settings(self):
        return self.__iosettings


class ZarrReference(dict):

    @docval({'name': 'source',
             'type': str,
             'doc': 'Source of referenced object',
             'default': None},
            {'name': 'path',
             'type': str,
             'doc': 'Path of referenced object',
             'default': None}
            )
    def __init__(self, **kwargs):
        dest_source, dest_path = getargs('source', 'path', kwargs)
        super(ZarrReference, self).__init__()
        super(ZarrReference, self).__setitem__('source', dest_source)
        super(ZarrReference, self).__setitem__('path', dest_path)

    @property
    def source(self):
        return super(ZarrReference, self).__getitem__('source')

    @property
    def path(self):
        return super(ZarrReference, self).__getitem__('path')

    @source.setter
    def source(self, s):
        super(ZarrReference, self).__setitem__('source', s)

    @path.setter
    def path(self, p):
        super(ZarrReference, self).__setitem__('path', p)
