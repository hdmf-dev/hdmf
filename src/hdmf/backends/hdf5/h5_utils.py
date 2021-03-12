import json
import os
import warnings
from abc import ABCMeta, abstractmethod
from collections.abc import Iterable
from copy import copy

import numpy as np
from h5py import Group, Dataset, RegionReference, Reference, special_dtype
from h5py import filters as h5py_filters

from ...array import Array
from ...data_utils import DataIO, AbstractDataChunkIterator
from ...query import HDMFDataset, ReferenceResolver, ContainerResolver, BuilderResolver
from ...region import RegionSlicer
from ...spec import SpecWriter, SpecReader
from ...utils import docval, getargs, popargs, call_docval_func, get_docval


class H5Dataset(HDMFDataset):
    @docval({'name': 'dataset', 'type': (Dataset, Array), 'doc': 'the HDF5 file lazily evaluate'},
            {'name': 'io', 'type': 'HDF5IO', 'doc': 'the IO object that was used to read the underlying dataset'})
    def __init__(self, **kwargs):
        self.__io = popargs('io', kwargs)
        call_docval_func(super().__init__, kwargs)

    @property
    def io(self):
        return self.__io

    @property
    def regionref(self):
        return self.dataset.regionref

    @property
    def ref(self):
        return self.dataset.ref

    @property
    def shape(self):
        return self.dataset.shape


class DatasetOfReferences(H5Dataset, ReferenceResolver, metaclass=ABCMeta):
    """
    An extension of the base ReferenceResolver class to add more abstract methods for
    subclasses that will read HDF5 references
    """

    @abstractmethod
    def get_object(self, h5obj):
        """
        A class that maps an HDF5 object to a Builder or Container
        """
        pass

    def invert(self):
        """
        Return an object that defers reference resolution
        but in the opposite direction.
        """
        if not hasattr(self, '__inverted'):
            cls = self.get_inverse_class()
            docval = get_docval(cls.__init__)
            kwargs = dict()
            for arg in docval:
                kwargs[arg['name']] = getattr(self, arg['name'])
            self.__inverted = cls(**kwargs)
        return self.__inverted

    def _get_ref(self, ref):
        return self.get_object(self.dataset.file[ref])

    def __iter__(self):
        for ref in super().__iter__():
            yield self._get_ref(ref)

    def __next__(self):
        return self._get_ref(super().__next__())


class BuilderResolverMixin(BuilderResolver):
    """
    A mixin for adding to HDF5 reference-resolving types
    the get_object method that returns Builders
    """

    def get_object(self, h5obj):
        """
        A class that maps an HDF5 object to a Builder
        """
        return self.io.get_builder(h5obj)


class ContainerResolverMixin(ContainerResolver):
    """
    A mixin for adding to HDF5 reference-resolving types
    the get_object method that returns Containers
    """

    def get_object(self, h5obj):
        """
        A class that maps an HDF5 object to a Container
        """
        return self.io.get_container(h5obj)


class AbstractH5TableDataset(DatasetOfReferences):

    @docval({'name': 'dataset', 'type': (Dataset, Array), 'doc': 'the HDF5 file lazily evaluate'},
            {'name': 'io', 'type': 'HDF5IO', 'doc': 'the IO object that was used to read the underlying dataset'},
            {'name': 'types', 'type': (list, tuple),
             'doc': 'the IO object that was used to read the underlying dataset'})
    def __init__(self, **kwargs):
        types = popargs('types', kwargs)
        call_docval_func(super().__init__, kwargs)
        self.__refgetters = dict()
        for i, t in enumerate(types):
            if t is RegionReference:
                self.__refgetters[i] = self.__get_regref
            elif t is Reference:
                self.__refgetters[i] = self._get_ref
            elif t is str:
                # we need this for when we read compound data types
                # that have unicode sub-dtypes since h5py does not
                # store UTF-8 in compound dtypes
                self.__refgetters[i] = self._get_utf
        self.__types = types
        tmp = list()
        for i in range(len(self.dataset.dtype)):
            sub = self.dataset.dtype[i]
            if sub.metadata:
                if 'vlen' in sub.metadata:
                    t = sub.metadata['vlen']
                    if t is str:
                        tmp.append('utf')
                    elif t is bytes:
                        tmp.append('ascii')
                elif 'ref' in sub.metadata:
                    t = sub.metadata['ref']
                    if t is Reference:
                        tmp.append('object')
                    elif t is RegionReference:
                        tmp.append('region')
            else:
                tmp.append(sub.type.__name__)
        self.__dtype = tmp

    @property
    def types(self):
        return self.__types

    @property
    def dtype(self):
        return self.__dtype

    def __getitem__(self, arg):
        rows = copy(super().__getitem__(arg))
        if np.issubdtype(type(arg), np.integer):
            self.__swap_refs(rows)
        else:
            for row in rows:
                self.__swap_refs(row)
        return rows

    def __swap_refs(self, row):
        for i in self.__refgetters:
            getref = self.__refgetters[i]
            row[i] = getref(row[i])

    def _get_utf(self, string):
        """
        Decode a dataset element to unicode
        """
        return string.decode('utf-8') if isinstance(string, bytes) else string

    def __get_regref(self, ref):
        obj = self._get_ref(ref)
        return obj[ref]

    def resolve(self, manager):
        return self[0:len(self)]

    def __iter__(self):
        for i in range(len(self)):
            yield self[i]


class AbstractH5ReferenceDataset(DatasetOfReferences):

    def __getitem__(self, arg):
        ref = super().__getitem__(arg)
        if isinstance(ref, np.ndarray):
            return [self._get_ref(x) for x in ref]
        else:
            return self._get_ref(ref)

    @property
    def dtype(self):
        return 'object'


class AbstractH5RegionDataset(AbstractH5ReferenceDataset):

    def __getitem__(self, arg):
        obj = super().__getitem__(arg)
        ref = self.dataset[arg]
        return obj[ref]

    @property
    def dtype(self):
        return 'region'


class ContainerH5TableDataset(ContainerResolverMixin, AbstractH5TableDataset):
    """
    A reference-resolving dataset for resolving references inside tables
    (i.e. compound dtypes) that returns resolved references as Containers
    """

    @classmethod
    def get_inverse_class(cls):
        return BuilderH5TableDataset


class BuilderH5TableDataset(BuilderResolverMixin, AbstractH5TableDataset):
    """
    A reference-resolving dataset for resolving references inside tables
    (i.e. compound dtypes) that returns resolved references as Builders
    """

    @classmethod
    def get_inverse_class(cls):
        return ContainerH5TableDataset


class ContainerH5ReferenceDataset(ContainerResolverMixin, AbstractH5ReferenceDataset):
    """
    A reference-resolving dataset for resolving object references that returns
    resolved references as Containers
    """

    @classmethod
    def get_inverse_class(cls):
        return BuilderH5ReferenceDataset


class BuilderH5ReferenceDataset(BuilderResolverMixin, AbstractH5ReferenceDataset):
    """
    A reference-resolving dataset for resolving object references that returns
    resolved references as Builders
    """

    @classmethod
    def get_inverse_class(cls):
        return ContainerH5ReferenceDataset


class ContainerH5RegionDataset(ContainerResolverMixin, AbstractH5RegionDataset):
    """
    A reference-resolving dataset for resolving region references that returns
    resolved references as Containers
    """

    @classmethod
    def get_inverse_class(cls):
        return BuilderH5RegionDataset


class BuilderH5RegionDataset(BuilderResolverMixin, AbstractH5RegionDataset):
    """
    A reference-resolving dataset for resolving region references that returns
    resolved references as Builders
    """

    @classmethod
    def get_inverse_class(cls):
        return ContainerH5RegionDataset


class H5SpecWriter(SpecWriter):

    __str_type = special_dtype(vlen=str)

    @docval({'name': 'group', 'type': Group, 'doc': 'the HDF5 file to write specs to'})
    def __init__(self, **kwargs):
        self.__group = getargs('group', kwargs)

    @staticmethod
    def stringify(spec):
        '''
        Converts a spec into a JSON string to write to a dataset
        '''
        return json.dumps(spec, separators=(',', ':'))

    def __write(self, d, name):
        data = self.stringify(d)
        # create spec group if it does not exist. otherwise, do not overwrite existing spec
        dset = self.__group.create_dataset(name, shape=tuple(), data=data, dtype=self.__str_type)
        return dset

    def write_spec(self, spec, path):
        return self.__write(spec, path)

    def write_namespace(self, namespace, path):
        return self.__write({'namespaces': [namespace]}, path)


class H5SpecReader(SpecReader):
    """Class that reads cached JSON-formatted namespace and spec data from an HDF5 group."""

    @docval({'name': 'group', 'type': Group, 'doc': 'the HDF5 group to read specs from'})
    def __init__(self, **kwargs):
        self.__group = getargs('group', kwargs)
        super_kwargs = {'source': "%s:%s" % (os.path.abspath(self.__group.file.name), self.__group.name)}
        call_docval_func(super().__init__, super_kwargs)
        self.__cache = None

    def __read(self, path):
        s = self.__group[path][()]
        if isinstance(s, np.ndarray) and s.shape == (1,):  # unpack scalar spec dataset
            s = s[0]

        if isinstance(s, bytes):
            s = s.decode('UTF-8')

        d = json.loads(s)
        return d

    def read_spec(self, spec_path):
        return self.__read(spec_path)

    def read_namespace(self, ns_path):
        if self.__cache is None:
            self.__cache = self.__read(ns_path)
        ret = self.__cache['namespaces']
        return ret


class H5RegionSlicer(RegionSlicer):

    @docval({'name': 'dataset', 'type': (Dataset, H5Dataset), 'doc': 'the HDF5 dataset to slice'},
            {'name': 'region', 'type': RegionReference, 'doc': 'the region reference to use to slice'})
    def __init__(self, **kwargs):
        self.__dataset = getargs('dataset', kwargs)
        self.__regref = getargs('region', kwargs)
        self.__len = self.__dataset.regionref.selection(self.__regref)[0]
        self.__region = None

    def __read_region(self):
        if self.__region is None:
            self.__region = self.__dataset[self.__regref]

    def __getitem__(self, idx):
        self.__read_region()
        return self.__region[idx]

    def __len__(self):
        return self.__len


class H5DataIO(DataIO):
    """
    Wrap data arrays for write via HDF5IO to customize I/O behavior, such as compression and chunking
    for data arrays.
    """

    @docval({'name': 'data',
             'type': (np.ndarray, list, tuple, Dataset, Iterable),
             'doc': 'the data to be written. NOTE: If an h5py.Dataset is used, all other settings but link_data' +
                    ' will be ignored as the dataset will either be linked to or copied as is in H5DataIO.',
             'default': None},
            {'name': 'maxshape',
             'type': tuple,
             'doc': 'Dataset will be resizable up to this shape (Tuple). Automatically enables chunking.' +
                    'Use None for the axes you want to be unlimited.',
             'default': None},
            {'name': 'chunks',
             'type': (bool, tuple),
             'doc': 'Chunk shape or True to enable auto-chunking',
             'default': None},
            {'name': 'compression',
             'type': (str, bool, int),
             'doc': 'Compression strategy. If a bool is given, then gzip compression will be used by default.' +
                    'http://docs.h5py.org/en/latest/high/dataset.html#dataset-compression',
             'default': None},
            {'name': 'compression_opts',
             'type': (int, tuple),
             'doc': 'Parameter for compression filter',
             'default': None},
            {'name': 'fillvalue',
             'type': None,
             'doc': 'Value to be returned when reading uninitialized parts of the dataset',
             'default': None},
            {'name': 'shuffle',
             'type': bool,
             'doc': 'Enable shuffle I/O filter. http://docs.h5py.org/en/latest/high/dataset.html#dataset-shuffle',
             'default': None},
            {'name': 'fletcher32',
             'type': bool,
             'doc': 'Enable fletcher32 checksum. http://docs.h5py.org/en/latest/high/dataset.html#dataset-fletcher32',
             'default': None},
            {'name': 'link_data',
             'type': bool,
             'doc': 'If data is an h5py.Dataset should it be linked to or copied. NOTE: This parameter is only ' +
                    'allowed if data is an h5py.Dataset',
             'default': False},
            {'name': 'allow_plugin_filters',
             'type': bool,
             'doc': 'Enable passing dynamically loaded filters as compression parameter',
             'default': False}
            )
    def __init__(self, **kwargs):
        # Get the list of I/O options that user has passed in
        ioarg_names = [name for name in kwargs.keys() if name not in ['data', 'link_data', 'allow_plugin_filters']]
        # Remove the ioargs from kwargs
        ioarg_values = [popargs(argname, kwargs) for argname in ioarg_names]
        # Consume link_data parameter
        self.__link_data = popargs('link_data', kwargs)
        # Consume allow_plugin_filters parameter
        self.__allow_plugin_filters = popargs('allow_plugin_filters', kwargs)
        # Check for possible collision with other parameters
        if not isinstance(getargs('data', kwargs), Dataset) and self.__link_data:
            self.__link_data = False
            warnings.warn('link_data parameter in H5DataIO will be ignored')
        # Call the super constructor and consume the data parameter
        call_docval_func(super().__init__, kwargs)
        # Construct the dict with the io args, ignoring all options that were set to None
        self.__iosettings = {k: v for k, v in zip(ioarg_names, ioarg_values) if v is not None}
        # Set io_properties for DataChunkIterators
        if isinstance(self.data, AbstractDataChunkIterator):
            # Define the chunking options if the user has not set them explicitly.
            if 'chunks' not in self.__iosettings and self.data.recommended_chunk_shape() is not None:
                self.__iosettings['chunks'] = self.data.recommended_chunk_shape()
            # Define the maxshape of the data if not provided by the user
            if 'maxshape' not in self.__iosettings:
                self.__iosettings['maxshape'] = self.data.maxshape
        # Make default settings when compression set to bool (True/False)
        if isinstance(self.__iosettings.get('compression', None), bool):
            if self.__iosettings['compression']:
                self.__iosettings['compression'] = 'gzip'
            else:
                self.__iosettings.pop('compression', None)
                if 'compression_opts' in self.__iosettings:
                    warnings.warn('Compression disabled by compression=False setting. ' +
                                  'compression_opts parameter will, therefore, be ignored.')
                    self.__iosettings.pop('compression_opts', None)
        # Validate the compression options used
        self._check_compression_options()
        # Confirm that the compressor is supported by h5py
        if not self.filter_available(self.__iosettings.get('compression', None),
                                     self.__allow_plugin_filters):
            msg = "%s compression may not be supported by this version of h5py." % str(self.__iosettings['compression'])
            if not self.__allow_plugin_filters:
                msg += "Set `allow_plugin_filters=True` to enable the use of dynamically-loaded plugin filters."
            raise ValueError(msg)
        # Check possible parameter collisions
        if isinstance(self.data, Dataset):
            for k in self.__iosettings.keys():
                warnings.warn("%s in H5DataIO will be ignored with H5DataIO.data being an HDF5 dataset" % k)

    def get_io_params(self):
        """
        Returns a dict with the I/O parameters specifiedin in this DataIO.
        """
        ret = dict(self.__iosettings)
        ret['link_data'] = self.__link_data
        return ret

    def _check_compression_options(self):
        """
        Internal helper function used to check if compression options are compliant
        with the compression filter used.

        :raises ValueError: If incompatible options are detected
        """
        if 'compression' in self.__iosettings:
            if 'compression_opts' in self.__iosettings:
                if self.__iosettings['compression'] == 'gzip':
                    if self.__iosettings['compression_opts'] not in range(10):
                        raise ValueError("GZIP compression_opts setting must be an integer from 0-9, "
                                         "not " + str(self.__iosettings['compression_opts']))
                elif self.__iosettings['compression'] == 'lzf':
                    if self.__iosettings['compression_opts'] is not None:
                        raise ValueError("LZF compression filter accepts no compression_opts")
                elif self.__iosettings['compression'] == 'szip':
                    szip_opts_error = False
                    # Check that we have a tuple
                    szip_opts_error |= not isinstance(self.__iosettings['compression_opts'], tuple)
                    # Check that we have a tuple of the right length and correct settings
                    if not szip_opts_error:
                        try:
                            szmethod, szpix = self.__iosettings['compression_opts']
                            szip_opts_error |= (szmethod not in ('ec', 'nn'))
                            szip_opts_error |= (not (0 < szpix <= 32 and szpix % 2 == 0))
                        except ValueError:  # ValueError is raised if tuple does not have the right length to unpack
                            szip_opts_error = True
                    if szip_opts_error:
                        raise ValueError("SZIP compression filter compression_opts"
                                         " must be a 2-tuple ('ec'|'nn', even integer 0-32).")
            # Warn if compressor other than gzip is being used
            if self.__iosettings['compression'] not in ['gzip', h5py_filters.h5z.FILTER_DEFLATE]:
                warnings.warn(str(self.__iosettings['compression']) + " compression may not be available "
                              "on all installations of HDF5. Use of gzip is recommended to ensure portability of "
                              "the generated HDF5 files.")

    @staticmethod
    def filter_available(filter, allow_plugin_filters):
        """
        Check if a given I/O filter is available

        :param filter: String with the name of the filter, e.g., gzip, szip etc.
                       int with the registered filter ID, e.g. 307
        :type filter: String, int
        :param allow_plugin_filters: bool indicating whether the given filter can be dynamically loaded
        :return: bool indicating wether the given filter is available
        """
        if filter is not None:
            if filter in h5py_filters.encode:
                return True
            elif allow_plugin_filters is True:
                if type(filter) == int:
                    if h5py_filters.h5z.filter_avail(filter):
                        filter_info = h5py_filters.h5z.get_filter_info(filter)
                        if filter_info == (h5py_filters.h5z.FILTER_CONFIG_DECODE_ENABLED +
                                           h5py_filters.h5z.FILTER_CONFIG_ENCODE_ENABLED):
                            return True
            return False
        else:
            return True

    @property
    def link_data(self):
        return self.__link_data

    @property
    def io_settings(self):
        return self.__iosettings

    @property
    def valid(self):
        if isinstance(self.data, Dataset) and not self.data.id.valid:
            return False
        return super().valid
