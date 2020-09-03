"""Module with the Zarr-based I/O-backend for HDMF"""
# Python imports
import os
import itertools
from copy import deepcopy
import warnings
import numpy as np
import tempfile
import logging

# Zarr imports
import zarr
from zarr.hierarchy import Group
from zarr.core import Array
import numcodecs

# HDMF imports
from .zarr_utils import ZarrDataIO, ZarrReference, ZarrSpecWriter, ZarrSpecReader, ZarrIODataChunkIteratorQueue
from ..io import HDMFIO
from ..errors import UnsupportedOperation
from ...utils import docval, getargs, popargs, call_docval_func, get_docval
from ...build import Builder, GroupBuilder, DatasetBuilder, LinkBuilder, BuildManager,\
                     RegionBuilder, ReferenceBuilder, TypeMap  # , ObjectMapper
from ...utils import get_data_shape
from ...data_utils import AbstractDataChunkIterator
from ...spec import RefSpec, DtypeSpec, NamespaceCatalog

from ..utils import NamespaceToBuilderHelper, WriteStatusTracker
from ...query import HDMFDataset
from ...container import Container

# Module variables
ROOT_NAME = 'root'
SPEC_LOC_ATTR = '.specloc'


# TODO We should resolve reference stored in datasets to the containers
# TODO We should add support for RegionReferences
# TODO HDF5IO uses export_source argument on export. Need to check with Ryan if we need it here as well.


class ZarrIO(HDMFIO):

    @docval({'name': 'path', 'type': str, 'doc': 'the path to the Zarr file'},
            {'name': 'manager', 'type': BuildManager, 'doc': 'the BuildManager to use for I/O', 'default': None},
            {'name': 'mode', 'type': str,
             'doc': 'the mode to open the Zarr file with, one of ("w", "r", "r+", "a", "w-")'},
            {'name': 'synchronizer', 'type': (zarr.ProcessSynchronizer, zarr.ThreadSynchronizer, bool),
             'doc': 'Zarr synchronizer to use for parallel I/O. If set to True a ProcessSynchronizer is used.',
             'default': None},
            {'name': 'chunking', 'type': bool, 'doc': "Enable chunking of datasets by default", 'default': True})
    def __init__(self, **kwargs):
        self.logger = logging.getLogger('%s.%s' % (self.__class__.__module__, self.__class__.__qualname__))
        path, manager, mode, synchronizer, chunking = popargs('path', 'manager', 'mode',
                                                              'synchronizer', 'chunking', kwargs)
        if manager is None:
            manager = BuildManager(TypeMap(NamespaceCatalog()))
        if isinstance(synchronizer, bool):
            sync_path = tempfile.mkdtemp()
            self.__synchronizer = zarr.ProcessSynchronizer(sync_path)
        else:
            self.__synchronizer = synchronizer
        self.__mode = mode
        self.__path = path
        self.__file = None
        self.__built = dict()
        self._written_builders = WriteStatusTracker()  # track which builders were written (or read) by this IO object
        self.__dci_queue = ZarrIODataChunkIteratorQueue()  # a queue of DataChunkIterators that need to be exhausted
        self.__chunking = chunking
        super().__init__(manager, source=path)
        warn_msg = '\033[91m' + 'The ZarrIO backend is experimental. It is under active ' + \
                   'development and backward compatibility is not guaranteed for the backend.' + '\033[0m'
        warnings.warn(warn_msg)

    @property
    def chunking(self):
        return self.__chunking

    @property
    def synchronizer(self):
        return self.__synchronizer

    def open(self):
        """Open the Zarr file"""
        if self.__file is None:
            self.__file = zarr.open(store=self.__path,
                                    mode=self.__mode,
                                    synchronizer=self.__synchronizer)

    def close(self):
        """Close the Zarr file"""
        self.__file = None
        return

    @classmethod
    @docval({'name': 'namespace_catalog',
             'type': (NamespaceCatalog, TypeMap),
             'doc': 'the NamespaceCatalog or TypeMap to load namespaces into'},
            {'name': 'path', 'type': str, 'doc': 'the path to the Zarr file'},
            {'name': 'namespaces', 'type': list, 'doc': 'the namespaces to load', 'default': None})
    def load_namespaces(cls, namespace_catalog, path, namespaces=None):
        '''
        Load cached namespaces from a file.
        '''
        f = zarr.open(path, 'r')
        if SPEC_LOC_ATTR not in f.attrs:
            msg = "No cached namespaces found in %s" % path
            warnings.warn(msg)
        else:
            spec_group = f[f.attrs[SPEC_LOC_ATTR]]
            if namespaces is None:
                namespaces = list(spec_group.keys())
            for ns in namespaces:
                ns_group = spec_group[ns]
                latest_version = list(ns_group.keys())[-1]
                ns_group = ns_group[latest_version]
                reader = ZarrSpecReader(ns_group)
                namespace_catalog.load_namespaces('namespace', reader=reader)

    @docval({'name': 'container', 'type': Container, 'doc': 'the Container object to write'},
            {'name': 'cache_spec', 'type': bool, 'doc': 'cache specification to file', 'default': True},
            {'name': 'link_data', 'type': bool,
             'doc': 'If not specified otherwise link (True) or copy (False) Datasets', 'default': True},
            {'name': 'exhaust_dci', 'type': bool,
             'doc': 'exhaust DataChunkIterators one at a time. If False, add ' +
                    'them to the internal queue self.__dci_queue and exhaust them concurrently at the end',
             'default': True},)
    def write(self, **kwargs):
        """Overwrite the write method to add support for caching the specification"""
        cache_spec = popargs('cache_spec', kwargs)
        call_docval_func(super(ZarrIO, self).write, kwargs)
        if cache_spec:
            self.__cache_spec()

    def __cache_spec(self):
        """Interanl function used to cache the spec in the current file"""
        ref = self.__file.attrs.get(SPEC_LOC_ATTR)
        spec_group = None
        if ref is not None:
            spec_group = self.__file[ref]
        else:
            path = 'specifications'  # do something to figure out where the specifications should go
            spec_group = self.__file.require_group(path)
            self.__file.attrs[SPEC_LOC_ATTR] = path
        ns_catalog = self.manager.namespace_catalog
        for ns_name in ns_catalog.namespaces:
            ns_builder = NamespaceToBuilderHelper.convert_namespace(ns_catalog, ns_name)
            namespace = ns_catalog.get_namespace(ns_name)
            if namespace.version is None:
                group_name = '%s/unversioned' % ns_name
            else:
                group_name = '%s/%s' % (ns_name, namespace.version)
            ns_group = spec_group.require_group(group_name)
            writer = ZarrSpecWriter(ns_group)
            ns_builder.export('namespace', writer=writer)

    @docval(*get_docval(HDMFIO.export),
            {'name': 'cache_spec', 'type': bool, 'doc': 'whether to cache the specification to file', 'default': True})
    def export(self, **kwargs):
        """Export data read from a file from any backend to Zarr.

        See :py:meth:`hdmf.backends.io.HDMFIO.export` for more details.
        """
        if self.__mode != 'w':
            raise UnsupportedOperation("Cannot export to file %s in mode '%s'. Please use mode 'w'."
                                       % (self.source, self.__mode))

        src_io = getargs('src_io', kwargs)
        write_args, cache_spec = popargs('write_args', 'cache_spec', kwargs)

        if not isinstance(src_io, ZarrIO) and write_args.get('link_data', True):
            raise UnsupportedOperation("Cannot export from non-Zarr backend %s to Zarr with write argument "
                                       "link_data=True." % src_io.__class__.__name__)

        # write_args['export_source'] = src_io.source  # pass export_source=src_io.source to write_builder
        ckwargs = kwargs.copy()
        ckwargs['write_args'] = write_args
        call_docval_func(super().export, ckwargs)
        if cache_spec:
            self.__cache_spec()

    def get_written(self, builder, check_on_disk=False):
        """Return True if this builder has been written to (or read from) disk by this IO object, False otherwise.

        :param builder: Builder object to get the written flag for
        :type builder: Builder
        :param check_on_disk: Check that the builder has been physically written to disk not just flagged as written
                              by this I/O backend
        :type check_on_disk: bool

        :return: True if the builder is found in self._written_builders using the builder ID, False otherwise
        """
        return self._written_builders.get_written(builder)

    def get_builder_exists_on_disk(self, builder, parent):
        """Convenience function to check whether a given builder exists on disk"""
        builder_path = os.path.join(self.__path, os.path.join(parent.name, builder.name).lstrip('/'))
        exists_on_disk = os.path.exists(builder_path)
        return exists_on_disk

    @docval({'name': 'builder', 'type': GroupBuilder, 'doc': 'the GroupBuilder object representing the NWBFile'},
            {'name': 'link_data', 'type': bool,
             'doc': 'If not specified otherwise link (True) or copy (False) Zarr Datasets', 'default': True},
            {'name': 'exhaust_dci', 'type': bool,
             'doc': 'exhaust DataChunkIterators one at a time. If False, add ' +
                    'them to the internal queue self.__dci_queue and exhaust them concurrently at the end',
             'default': True})
    def write_builder(self, **kwargs):
        """Write a builder to disk"""
        f_builder, link_data, exhaust_dci = getargs('builder', 'link_data', 'exhaust_dci', kwargs)
        for name, gbldr in f_builder.groups.items():
            self.write_group(parent=self.__file,
                             builder=gbldr,
                             link_data=link_data,
                             exhaust_dci=exhaust_dci)
        for name, dbldr in f_builder.datasets.items():
            self.write_dataset(parent=self.__file,
                               builder=dbldr,
                               link_data=link_data,
                               exhaust_dci=exhaust_dci)
        self.set_attributes(self.__file, f_builder.attributes)
        self.__dci_queue.exhaust_queue()  # Write all DataChunkIterators that have been queued
        self._written_builders.set_written(f_builder)
        self.logger.debug("Done writing %s '%s' to path '%s'" %
                          (f_builder.__class__.__qualname__, f_builder.name, self.source))

    @docval({'name': 'parent', 'type': Group, 'doc': 'the parent Zarr object'},
            {'name': 'builder', 'type': GroupBuilder, 'doc': 'the GroupBuilder to write'},
            {'name': 'link_data', 'type': bool,
             'doc': 'If not specified otherwise link (True) or copy (False) Zarr Datasets', 'default': True},
            {'name': 'exhaust_dci', 'type': bool,
             'doc': 'exhaust DataChunkIterators one at a time. If False, add ' +
                    'them to the internal queue self.__dci_queue and exhaust them concurrently at the end',
             'default': True},
            returns='the Group that was created', rtype='Group')
    def write_group(self, **kwargs):
        """Write a GroupBuider to file"""
        parent, builder, link_data, exhaust_dci = getargs('parent', 'builder', 'link_data', 'exhaust_dci', kwargs)
        if self.get_written(builder):
            group = parent[builder.name]
        else:
            group = parent.require_group(builder.name)

        subgroups = builder.groups
        if subgroups:
            for subgroup_name, sub_builder in subgroups.items():
                self.write_group(parent=group,
                                 builder=sub_builder,
                                 link_data=link_data,
                                 exhaust_dci=exhaust_dci)

        datasets = builder.datasets
        if datasets:
            for dset_name, sub_builder in datasets.items():
                self.write_dataset(parent=group,
                                   builder=sub_builder,
                                   link_data=link_data,
                                   exhaust_dci=exhaust_dci)

        # write all links (haven implemented)
        links = builder.links
        if links:
            for link_name, sub_builder in links.items():
                self.write_link(group, sub_builder)

        attributes = builder.attributes
        self.set_attributes(group, attributes)
        self._written_builders.set_written(builder)  # record that the builder has been written
        return group

    @docval({'name': 'obj', 'type': (Group, Array), 'doc': 'the Zarr object to add attributes to'},
            {'name': 'attributes',
             'type': dict,
             'doc': 'a dict containing the attributes on the Group or Dataset, indexed by attribute name'})
    def set_attributes(self, **kwargs):
        obj, attributes = getargs('obj', 'attributes', kwargs)
        for key, value in attributes.items():
            # Case 1: list, set, tuple type attributes
            if isinstance(value, (set, list, tuple)) or (isinstance(value, np.ndarray) and np.ndim(value) != 0):
                tmp = tuple(value)
                # Attempt write of the attribute
                try:
                    obj.attrs[key] = tmp
                # Numpy scalars abd bytes are not JSON serializable. Try to convert to a serializable type instead
                except TypeError as e:
                    write_ok = False
                    try:
                        tmp = tuple([i.item()
                                     if (isinstance(i, np.generic) and not isinstance(i, np.bytes_))
                                     else i.decode("utf-8")
                                     if isinstance(i, (bytes, np.bytes_))
                                     else i
                                     for i in value])
                        obj.attrs[key] = tmp
                        write_ok = True
                    except:  # noqa: E722
                        pass
                    if not write_ok:
                        raise TypeError(str(e) + " type=" + str(type(value)) + "  data=" + str(value))
            # Case 2: References
            elif isinstance(value, (Container, Builder, ReferenceBuilder)):
                if isinstance(value, RegionBuilder):
                    type_str = 'region'
                    refs = self.__get_ref(value.builder)
                elif isinstance(value, ReferenceBuilder) or isinstance(value, Container) or isinstance(value, Builder):
                    type_str = 'object'
                    if isinstance(value, Builder):
                        refs = self.__get_ref(value)
                    else:
                        refs = self.__get_ref(value.builder)
                tmp = {'zarr_dtype': type_str, 'value': refs}
                obj.attrs[key] = tmp
            # Case 3: Scalar attributes
            else:
                # Attempt to write the attribute
                try:
                    obj.attrs[key] = value
                # Numpy scalars and bytes are not JSON serializable. Try to convert to a serializable type instead
                except TypeError as e:
                    write_ok = False
                    try:
                        val = value.item if isinstance(value, np.ndarray) else value
                        val = value.item() \
                            if (isinstance(value, np.generic) and not isinstance(value, np.bytes_)) \
                            else val.decode("utf-8") \
                            if isinstance(value, (bytes, np.bytes_)) \
                            else val
                        obj.attrs[key] = val
                        write_ok = True
                    except:  # noqa: E722
                        pass
                    if not write_ok:
                        raise TypeError(str(e) + "key=" + key + " type=" + str(type(value)) + "  data=" + str(value))

    def __get_path(self, builder):
        """Get the path to the builder.

        If builder.location is set then it is used as the path, otherwise the function
        determines the path by constructing it iteratively from the parents of the
        builder.
        """
        if builder.location is not None:
            path = os.path.normpath(os.path.join(builder.location, builder.name)).replace("\\", "/")
        else:
            curr = builder
            names = list()
            while curr is not None and curr.name != ROOT_NAME:
                names.append(curr.name)
                curr = curr.parent
            delim = "/"
            path = "%s%s" % (delim, delim.join(reversed(names)))
        return path

    @staticmethod
    def get_zarr_paths(zarr_object):
        """
        For a Zarr object find 1) the path to the main zarr file it is in and 2) the path to the object within the file

        :param zarr_object: Object for which we are looking up the path
        :type zarr_object: Zarr Group or Array
        :return: Tuple of two string with: 1) path of the Zarr file and 2) full path within the zarr file to the object
        """
        # In Zarr the path is a combination of the path of the store and the path of the object. So we first need to
        # merge those two paths, then remove the path of the file, add the missing leading "/" and then compute the
        # directory name to get the path of the parent
        fullpath = os.path.normpath(os.path.join(zarr_object.store.path, zarr_object.path)).replace("\\", "/")
        # To determine the filepath we now iterate over the path and check if the .zgroup object exists at
        # a level, indicating that we are still within the Zarr file. The first level we hit where the parent
        # directory does not have a .zgroup means we have found the main file
        filepath = fullpath
        while os.path.exists(os.path.join(os.path.dirname(filepath), ".zgroup")):
            filepath = os.path.dirname(filepath)
        # From the fullpath and filepath we can now compute the objectpath within the zarr file as the relative
        # path from the filepath to the object
        objectpath = "/" + os.path.relpath(fullpath, filepath)
        # return the result
        return filepath, objectpath

    @staticmethod
    def get_zarr_parent_path(zarr_object):
        """
        Get the location of the parent of a zarr_object within the file

        :param zarr_object: Object for which we are looking up the path
        :type zarr_object: Zarr Group or Array
        :return: String with the path
        """
        filepath, objectpath = ZarrIO.get_zarr_paths(zarr_object)
        parentpath = os.path.dirname(objectpath)
        return parentpath

    def __is_ref(self, dtype):
        if isinstance(dtype, DtypeSpec):
            return self.__is_ref(dtype.dtype)
        elif isinstance(dtype, RefSpec):
            return True
        elif isinstance(dtype, np.dtype):
            return False
        else:
            return dtype == DatasetBuilder.OBJECT_REF_TYPE or dtype == DatasetBuilder.REGION_REF_TYPE

    def __get_ref(self, ref_object):
        """
        Create a ZarrReference object that points to the given container

        :param ref_object: the object to be referenced
        :type ref_object: Builder, Container, ReferenceBuilder
        :returns: ZarrReference object

        """
        if isinstance(ref_object, RegionBuilder):  # or region is not None: TODO: Add to support regions
            raise NotImplementedError("Region references are currently not supported by ZarrIO")
        if isinstance(ref_object, Builder):
            if isinstance(ref_object, LinkBuilder):
                builder = ref_object.target_builder
            else:
                builder = ref_object
        elif isinstance(ref_object, ReferenceBuilder):
            builder = ref_object.builder
        else:
            builder = self.manager.build(ref_object)
        path = self.__get_path(builder)
        # TODO Add to get region for region references.
        #      Also add  {'name': 'region', 'type': (slice, list, tuple),
        #      'doc': 'the region reference indexing object',  'default': None},
        # if isinstance(ref_object, RegionBuilder):
        #    region = ref_object.region

        # by checking os.isdir makes sure we have a valid link path to a dir for Zarr. For conversion
        # between backends a user should always use export which takes care of creating a clean set of builders.
        source = builder.source if os.path.isdir(builder.source) else self.__path
        return ZarrReference(source, path)

    def __add_link__(self, parent, target_source, target_path, link_name):
        """
        Add a link to the file

        :param parent: The parent Zarr group containing the link
        :type parent: zarr.hierarchy.Group
        :param target_source: Source path within the Zarr file to the linked object
        :type target_source: str
        :param target_path: Path to the Zarr file containing the linked object
        :param link_name: Name of the link
        :type link_name: str
        """
        if 'zarr_link' not in parent.attrs:
            parent.attrs['zarr_link'] = []
        zarr_link = list(parent.attrs['zarr_link'])
        zarr_link.append({'source': target_source, 'path': target_path, 'name': link_name})
        parent.attrs['zarr_link'] = zarr_link

    @docval({'name': 'parent', 'type': Group, 'doc': 'the parent Zarr object'},
            {'name': 'builder', 'type': LinkBuilder, 'doc': 'the LinkBuilder to write'})
    def write_link(self, **kwargs):
        parent, builder = getargs('parent', 'builder', kwargs)
        if self.get_written(builder):
            self.logger.debug("Skipping LinkBuilder '%s' already written to parent group '%s'"
                              % (builder.name, parent.name))
            return
        self.logger.debug("Writing LinkBuilder '%s' to parent group '%s'" % (builder.name, parent.name))
        name = builder.name
        target_builder = builder.builder
        zarr_ref = self.__get_ref(target_builder)
        # if the target and source are both the same, then we need to ALWAYS use ourselves as a source
        # When exporting from one source to another, the LinkBuilders.source are not updated, i.e,. the
        # builder.source and target_builder.source are not being updated and point to the old file, but
        # for internal links (a.k.a, SoftLinks) they will be the same and our target will be part of
        # our new file, so we can savely replace the source
        if builder.source == target_builder.source:
            zarr_ref.source = self.__path
        self.__add_link__(parent, zarr_ref.source, zarr_ref.path, name)
        self._written_builders.set_written(builder)  # record that the builder has been written

    @classmethod
    def __setup_chunked_dataset__(cls, parent, name, data, options=None):
        """
        Setup a dataset for writing to one-chunk-at-a-time based on the given DataChunkIterator. This
        is a helper function for write_dataset()

        :param parent: The parent object to which the dataset should be added
        :type parent: Zarr Group or File
        :param name: The name of the dataset
        :type name: str
        :param data: The data to be written.
        :type data: AbstractDataChunkIterator
        :param options: Dict with options for creating a dataset. available options are 'dtype' and 'io_settings'
        :type options: dict

        """
        io_settings = {}
        if options is not None:
            if 'io_settings' in options:
                io_settings = options.get('io_settings')
        # Define the chunking options if the user has not set them explicitly. We need chunking for the iterative write.
        if 'chunks' not in io_settings:
            recommended_chunks = data.recommended_chunk_shape()
            io_settings['chunks'] = True if recommended_chunks is None else recommended_chunks
        # Define the shape of the data if not provided by the user
        if 'shape' not in io_settings:
            io_settings['shape'] = data.recommended_data_shape()
        if 'dtype' not in io_settings:
            if (options is not None) and ('dtype' in options):
                io_settings['dtype'] = options['dtype']
            else:
                io_settings['dtype'] = data.dtype
            if isinstance(io_settings['dtype'], str):
                # map to real dtype if we were given a string
                io_settings['dtype'] = cls.__dtypes.get(io_settings['dtype'])
        try:
            dset = parent.create_dataset(name, **io_settings)
        except Exception as exc:
            raise Exception("Could not create dataset %s in %s" % (name, parent.name)) from exc
        return dset

    @docval({'name': 'parent', 'type': Group, 'doc': 'the parent Zarr object'},  # noqa: C901
            {'name': 'builder', 'type': DatasetBuilder, 'doc': 'the DatasetBuilder to write'},
            {'name': 'link_data', 'type': bool,
             'doc': 'If not specified otherwise link (True) or copy (False) Zarr Datasets', 'default': True},
            {'name': 'exhaust_dci', 'type': bool,
             'doc': 'exhaust DataChunkIterators one at a time. If False, add ' +
                    'them to the internal queue self.__dci_queue and exhaust them concurrently at the end',
             'default': True},
            {'name': 'force_data', 'type': None,
             'doc': 'Used internally to force the data being used when we have to load the data', 'default': None},
            returns='the Zarr array that was created', rtype=Array)
    def write_dataset(self, **kwargs):  # noqa: C901
        parent, builder, link_data, exhaust_dci = getargs('parent', 'builder', 'link_data', 'exhaust_dci', kwargs)
        force_data = getargs('force_data', kwargs)
        if self.get_written(builder):
            return None
        name = builder.name
        data = builder.data if force_data is None else force_data
        options = dict()
        if isinstance(data, ZarrDataIO):
            options['io_settings'] = data.io_settings
            link_data = data.link_data
            data = data.data
        else:
            options['io_settings'] = {}
        # Enable/Disable chunking for all datasets if not set. Ignore in case of a DataChunkIterator
        # as those datasets will always be chunked and need to set their own chunking
        if 'chunks' not in options['io_settings'] and not isinstance(data, AbstractDataChunkIterator):
            options['io_settings']['chunks'] = self.chunking

        attributes = builder.attributes
        options['dtype'] = builder.dtype

        linked = False

        # Write a regular Zarr array
        dset = None
        if isinstance(data, Array):
            # copy the dataset
            if link_data:
                self.__add_link__(parent, data.store.path, data.name, name)
                linked = True
                dset = None
            else:
                zarr.copy(data, parent, name=name)
                dset = parent[name]
        # When converting data between backends we may see an HDMFDataset, e.g., a H55ReferenceDataset, with references
        elif isinstance(data, HDMFDataset):
            # If we have a dataset of containers we need to make the references to the containers
            if len(data) > 0 and isinstance(data[0], Container):
                ref_data = [self.__get_ref(data[i]) for i in range(len(data))]
                shape = (len(data), )
                type_str = 'object'
                dset = parent.require_dataset(name,
                                              shape=shape,
                                              dtype=object,
                                              object_codec=numcodecs.JSON(),
                                              **options['io_settings'])
                dset.attrs['zarr_dtype'] = type_str
                dset[:] = ref_data
                self._written_builders.set_written(builder)  # record that the builder has been written
            # If we have a regular dataset, then load the data and write the builder after load
            else:
                # TODO This code path is also exercised when data is a
                # hdmf.backends.hdf5.h5_utils.BuilderH5ReferenceDataset (aka.  ReferenceResolver)
                # check that this is indeed the right thing to do here

                # We can/should not update the data in the builder itself so we load the data here and instead
                # force write_dataset when we call it recursively to use the data we loaded, rather than the
                # dataset that is set on the builder
                dset = self.write_dataset(parent=parent,
                                          builder=builder,
                                          link_data=link_data,
                                          force_data=data[:])
                self._written_builders.set_written(builder)  # record that the builder has been written
        # Write a compound dataset
        elif isinstance(options['dtype'], list):

            refs = list()
            type_str = list()
            for i, dts in enumerate(options['dtype']):
                if self.__is_ref(dts['dtype']):
                    refs.append(i)
                    ref_tmp = self.__get_ref(data[0][i])
                    if isinstance(ref_tmp, ZarrReference):
                        dts_str = 'object'
                    else:
                        dts_str = 'region'
                    type_str.append({'name': dts['name'], 'dtype': dts_str})
                else:
                    i = list([dts, ])
                    t = self.__resolve_dtype_helper__(i)
                    type_str.append(self.__serial_dtype__(t)[0])

            if len(refs) > 0:
                dset = parent.require_dataset(name,
                                              shape=(len(data), ),
                                              dtype=object,
                                              object_codec=numcodecs.JSON(),
                                              **options['io_settings'])
                self._written_builders.set_written(builder)  # record that the builder has been written
                dset.attrs['zarr_dtype'] = type_str
                for j, item in enumerate(data):
                    new_item = list(item)
                    for i in refs:
                        new_item[i] = self.__get_ref(item[i])
                    dset[j] = new_item
            else:
                # write a compound datatype
                dset = self.__list_fill__(parent, name, data, options)
        # Write a dataset of references
        elif self.__is_ref(options['dtype']):
            if isinstance(data, RegionBuilder):
                shape = (1,)
                type_str = 'region'
                refs = self.__get_ref(data.builder, data.region)
            elif isinstance(data, ReferenceBuilder):
                shape = (1,)
                type_str = 'object'
                refs = self.__get_ref(data.builder)
            elif options['dtype'] == 'region':
                shape = (len(data), )
                type_str = 'region'
                refs = [self.__get_ref(item.builder, item.region) for item in data]
            else:
                shape = (len(data), )
                type_str = 'object'
                refs = [self.__get_ref(item) for item in data]

            dset = parent.require_dataset(name,
                                          shape=shape,
                                          dtype=object,
                                          object_codec=numcodecs.JSON(),
                                          **options['io_settings'])
            self._written_builders.set_written(builder)  # record that the builder has been written
            dset.attrs['zarr_dtype'] = type_str
            if hasattr(refs, '__len__'):
                dset[:] = refs
            else:
                dset[0] = refs
        # write a 'regular' dataset without DatasetIO info
        else:
            if isinstance(data, (str, bytes)):
                dset = self.__scalar_fill__(parent, name, data, options)
            # Iterative write of a data chunk iterator
            elif isinstance(data, AbstractDataChunkIterator):
                dset = self.__setup_chunked_dataset__(parent, name, data, options)
                self.__dci_queue.append(dataset=dset, data=data)
            elif hasattr(data, '__len__'):
                dset = self.__list_fill__(parent, name, data, options)
            else:
                dset = self.__scalar_fill__(parent, name, data, options)
        if not linked:
            self.set_attributes(dset, attributes)
        # record that the builder has been written
        self._written_builders.set_written(builder)
        # Exhaust the DataChunkIterator if the dataset was given this way. Note this is a no-op
        # if the self.__dci_queue is empty
        if exhaust_dci:
            self.__dci_queue.exhaust_queue()
        return dset

    __dtypes = {
        "float": np.float32,
        "float32": np.float32,
        "double": np.float64,
        "float64": np.float64,
        "long": np.int64,
        "int64": np.int64,
        "uint64": np.uint64,
        "int": np.int32,
        "int32": np.int32,
        "int16": np.int16,
        "int8": np.int8,
        "bool": np.bool_,
        "bool_": np.bool_,
        "text": bytes,
        "utf": str,
        "utf8": str,
        "utf-8": str,
        "ascii": bytes,
        "str": bytes,
        "isodatetime": bytes,
        "string_": bytes,
        "uint32": np.uint32,
        "uint16": np.uint16,
        "uint8": np.uint8,
        "ref": ZarrReference,
        "reference": ZarrReference,
        "object": ZarrReference,
        "region": ZarrReference,
    }

    @classmethod
    def __serial_dtype__(cls, dtype):
        if isinstance(dtype, type):
            return dtype.__name__
        elif isinstance(dtype, np.dtype):
            if dtype.names is None:
                return dtype.type.__name__
            else:
                ret = list()
                for n in dtype.names:
                    item = dict()
                    item['name'] = n
                    item['dtype'] = cls.__serial_dtype__(dtype[n])
                    ret.append(item)
                return ret
        # TODO Does not work when Reference in compound datatype
        elif dtype == ZarrReference:
            return 'object'

    @classmethod
    def __resolve_dtype__(cls, dtype, data):
        dtype = cls.__resolve_dtype_helper__(dtype)
        if dtype is None:
            dtype = cls.get_type(data)
        return dtype

    @classmethod
    def __resolve_dtype_helper__(cls, dtype):
        if dtype is None:
            return None
        elif isinstance(dtype, (type, np.dtype)):
            return dtype
        elif isinstance(dtype, str):
            return cls.__dtypes.get(dtype)
        elif isinstance(dtype, dict):
            return cls.__dtypes.get(dtype['reftype'])
        else:
            return np.dtype([(x['name'], cls.__resolve_dtype_helper__(x['dtype'])) for x in dtype])

    @classmethod
    def get_type(cls, data):
        if isinstance(data, str):
            return str
        elif not hasattr(data, '__len__'):
            return type(data)
        else:
            if len(data) == 0:
                raise ValueError('cannot determine type for empty data')
            return cls.get_type(data[0])

    __reserve_attribute = ('zarr_dtype', 'zarr_link')

    @classmethod  # noqa: C901
    def __list_fill__(cls, parent, name, data, options=None):
        dtype = None
        io_settings = dict()
        if options is not None:
            dtype = options.get('dtype')
            io_settings = options.get('io_settings')
            if io_settings is None:
                io_settings = dict()
        # Determine the dtype
        if not isinstance(dtype, type):
            try:
                dtype = cls.__resolve_dtype__(dtype, data)
            except Exception as exc:
                msg = 'cannot add %s to %s - could not determine type' % (name, parent.name)  # noqa: F821
                raise Exception(msg) from exc

        # Set the type_str
        type_str = cls.__serial_dtype__(dtype)

        # Determine the shape and update the dtype if necessary when dtype==object
        if 'shape' in io_settings:  # Use the shape set by the user
            data_shape = io_settings.pop('shape')
        # If we have a numeric numpy array then use its shape
        elif isinstance(dtype, np.dtype) and np.issubdtype(dtype, np.number) or dtype == np.bool_:
            data_shape = get_data_shape(data)
        # Deal with object dtype
        elif isinstance(dtype, np.dtype):
            data = data[:]  # load the data in case we come from HDF5 or another on-disk data source we don't know
            data_shape = (len(data), )
            # if we have a compound data type
            if dtype.names:
                data_shape = get_data_shape(data)
                # If strings are part of our compound type then we need to use Object type instead
                # otherwise we try to keep the native compound datatype that numpy is using
                for substype in dtype.fields.items():
                    if np.issubdtype(substype[1][0], np.flexible) or np.issubdtype(substype[1][0], np.object_):
                        dtype = object
                        io_settings['object_codec'] = numcodecs.pickles.Pickle()
                        break
            # sometimes bytes and strings can hide as object in numpy array so lets try
            # to write those as strings and bytes rather than as objects
            elif len(data) > 0 and isinstance(data, np.ndarray):
                if isinstance(data.item(0), bytes):
                    dtype = bytes
                    data_shape = get_data_shape(data)
                elif isinstance(data.item(0), str):
                    dtype = str
                    data_shape = get_data_shape(data)
            # Set encoding for objects
            else:
                dtype = object
                io_settings['object_codec'] = numcodecs.pickles.Pickle()
        # Determine the shape from the data if all other cases have not been hit
        else:
            data_shape = get_data_shape(data)

        # Create the dataset
        dset = parent.require_dataset(name, shape=data_shape, dtype=dtype, **io_settings)
        dset.attrs['zarr_dtype'] = type_str

        # Write the data to file
        if dtype == object:
            for c in np.ndindex(data_shape):
                o = data
                for i in c:
                    o = o[i]
                # bytes are not JSON serializable
                dset[c] = o if not isinstance(o, (bytes, np.bytes_)) else o.decode("utf-8")
            return dset
        # standard write
        else:
            try:
                dset[:] = data  # If data is an h5py.Dataset then this will copy the data
            # For compound data types containing strings Zarr sometimes does not like wirting multiple values
            # try to write them one-at-a-time instead then
            except ValueError:
                for i in range(len(data)):
                    dset[i] = data[i]
        return dset

    @classmethod
    def __scalar_fill__(cls, parent, name, data, options=None):
        dtype = None
        io_settings = dict()
        if options is not None:
            dtype = options.get('dtype')
            io_settings = options.get('io_settings')
            if io_settings is None:
                io_settings = dict()
        if not isinstance(dtype, type):
            try:
                dtype = cls.__resolve_dtype__(dtype, data)
            except Exception as exc:
                msg = 'cannot add %s to %s - could not determine type' % (name, parent.name)
                raise Exception(msg) from exc
        if dtype == object:
            io_settings['object_codec'] = numcodecs.JSON()

        dset = parent.require_dataset(name, shape=(1, ), dtype=dtype, **io_settings)
        dset[:] = data
        type_str = 'scalar'
        dset.attrs['zarr_dtype'] = type_str
        return dset

    @docval(returns='a GroupBuilder representing the NWB Dataset', rtype='GroupBuilder')
    def read_builder(self):
        f_builder = self.__read_group(self.__file, ROOT_NAME)
        return f_builder

    def __set_built(self, zarr_obj, builder):
        fpath = zarr_obj.store.path
        path = zarr_obj.path
        path = os.path.join(fpath, path)
        self.__built.setdefault(path, builder)

    def __get_built(self, zarr_obj):
        """
        Look up a builder for the given zarr object

        :param zarr_obj: The Zarr object to be built
        :type zarr_obj: Zarr Group or Dataset

        :return: Builder in the self.__built cache or None
        """
        fpath = zarr_obj.store.path
        path = zarr_obj.path
        path = os.path.join(fpath, path)
        return self.__built.get(path, None)

    def __read_group(self, zarr_obj, name=None):
        ret = self.__get_built(zarr_obj)
        if ret is not None:
            return ret

        if name is None:
            name = str(os.path.basename(zarr_obj.name))

        # Create the GroupBuilder
        attributes = self.__read_attrs(zarr_obj)
        ret = GroupBuilder(name=name, source=self.__path, attributes=attributes)
        ret.location = self.get_zarr_parent_path(zarr_obj)

        # read sub groups
        for sub_name, sub_group in zarr_obj.groups():
            sub_builder = self.__read_group(sub_group, sub_name)
            ret.set_group(sub_builder)

        # read sub datasets
        for sub_name, sub_array in zarr_obj.arrays():
            sub_builder = self.__read_dataset(sub_array, sub_name)
            ret.set_dataset(sub_builder)

        # read the links
        self.__read_links(zarr_obj=zarr_obj, parent=ret)

        self._written_builders.set_written(ret)  # record that the builder has been written
        self.__set_built(zarr_obj, ret)
        return ret

    def __read_links(self, zarr_obj, parent):
        """
        Read the links associated with a zarr group
        :param zarr_obj: The Zarr group we should read links from
        :type zarr_obj: zarr.hiearchy.Group
        :param parent: GroupBuilder with which the links need to be associated
        :type parent: GroupBuilder
        """
        # read links
        if 'zarr_link' in zarr_obj.attrs:
            links = zarr_obj.attrs['zarr_link']
            for link in links:
                link_name = link['name']
                if link['source'] is None:
                    l_path = str(link['path'])
                elif link['path'] is None:
                    l_path = str(link['source'])
                else:
                    l_path = os.path.join(link['source'], link['path'].lstrip("/"))
                if not os.path.exists(l_path):
                    raise ValueError("Found bad link %s in %s to %s" % (link_name, self.__path, l_path))

                target_name = str(os.path.basename(l_path))
                target_zarr_obj = zarr.open(l_path, mode='r')
                # NOTE: __read_group and __read_dataset return the cached builders if the target has already been built
                if isinstance(target_zarr_obj, Group):
                    builder = self.__read_group(target_zarr_obj, target_name)
                else:
                    builder = self.__read_dataset(target_zarr_obj, target_name)
                link_builder = LinkBuilder(builder=builder, name=link_name, source=self.__path)
                link_builder.location = os.path.join(parent.location, parent.name)
                self._written_builders.set_written(link_builder)  # record that the builder has been written
                parent.set_link(link_builder)

    def __read_dataset(self, zarr_obj, name):
        ret = self.__get_built(zarr_obj)
        if ret is not None:
            return ret

        if 'zarr_dtype' not in zarr_obj.attrs:
            raise ValueError("Dataset missing zarr_dtype: " + str(name) + "   " + str(zarr_obj))

        kwargs = {"attributes": self.__read_attrs(zarr_obj),
                  "dtype": zarr_obj.attrs['zarr_dtype'],
                  "maxshape": zarr_obj.shape,
                  "chunks": not (zarr_obj.shape == zarr_obj.chunks),
                  "source": self.__path}
        dtype = kwargs['dtype']

        # data = deepcopy(zarr_obj[:])
        data = zarr_obj
        # kwargs['data'] = zarr_obj[:]
        # Read scalar dataset
        if dtype == 'scalar':
            data = zarr_obj[0]

        obj_refs = False
        reg_refs = False
        has_reference = False
        if isinstance(dtype, list):
            # compound data type
            obj_refs = list()
            reg_refs = list()
            for i, dts in enumerate(dtype):
                if dts['dtype'] == DatasetBuilder.OBJECT_REF_TYPE:
                    obj_refs.append(i)
                    has_reference = True
                elif dts['dtype'] == DatasetBuilder.REGION_REF_TYPE:
                    reg_refs.append(i)
                    has_reference = True

        elif self.__is_ref(dtype):
            # reference array
            has_reference = True
            if dtype == DatasetBuilder.OBJECT_REF_TYPE:
                obj_refs = True
            elif dtype == DatasetBuilder.REGION_REF_TYPE:
                reg_refs = True

        if has_reference:
            try:
                data = deepcopy(data[:])
                self.__parse_ref(kwargs['maxshape'], obj_refs, reg_refs, data)
            except ValueError as e:
                raise ValueError(str(e) + "  zarr-name=" + str(zarr_obj.name) + " name=" + str(name))

        kwargs['data'] = data
        if name is None:
            name = str(os.path.basename(zarr_obj.name))
        ret = DatasetBuilder(name, **kwargs)
        ret.location = self.get_zarr_parent_path(zarr_obj)
        self._written_builders.set_written(ret)  # record that the builder has been written
        self.__set_built(zarr_obj, ret)
        return ret

    def __parse_ref(self, shape, obj_refs, reg_refs, data):
        corr = []
        obj_pos = []
        reg_pos = []
        for s in shape:
            corr.append(range(s))
        corr = tuple(corr)
        for c in itertools.product(*corr):
            if isinstance(obj_refs, list):
                for i in obj_refs:
                    t = list(c)
                    t.append(i)
                    obj_pos.append(t)
            elif obj_refs:
                obj_pos.append(list(c))
            if isinstance(reg_refs, list):
                for i in reg_refs:
                    t = list(c)
                    t.append(i)
                    reg_pos.append(t)
            elif reg_refs:
                reg_pos.append(list(c))

        for p in obj_pos:
            o = data
            for i in p:
                o = o[i]
            source = o['source']
            path = o['path']
            if source is not None and source != "":
                path = os.path.join(source, path.lstrip("/"))

            if not os.path.exists(path):
                raise ValueError("Found bad link in dataset to %s" % (path))

            target_name = os.path.basename(path)
            target_zarr_obj = zarr.open(path, mode='r')

            o = data
            for i in range(0, len(p)-1):
                o = data[p[i]]
            if isinstance(target_zarr_obj, zarr.hierarchy.Group):
                o[p[-1]] = self.__read_group(target_zarr_obj, target_name)
            else:
                o[p[-1]] = self.__read_dataset(target_zarr_obj, target_name)

    def __read_attrs(self, zarr_obj):
        ret = dict()
        for k in zarr_obj.attrs.keys():
            if k not in self.__reserve_attribute:
                v = zarr_obj.attrs[k]
                if isinstance(v, dict) and 'zarr_dtype' in v:
                    # TODO Is this the correct way to resolve references?
                    if v['zarr_dtype'] == 'object':
                        source = v['value']['source']
                        path = v['value']['path']
                        if source is not None and source != "":
                            path = os.path.join(source, path.lstrip("/"))

                        if not os.path.exists(path):
                            raise ValueError("Found bad link in attribute to %s" % (path))

                        target_name = str(os.path.basename(path))
                        target_zarr_obj = zarr.open(str(path), mode='r')
                        if isinstance(target_zarr_obj, zarr.hierarchy.Group):
                            ret[k] = self.__read_group(target_zarr_obj, target_name)
                        else:
                            ret[k] = self.__read_dataset(target_zarr_obj, target_name)
                    # TODO Need to implement region references for attributes
                    elif v['zarr_dtype'] == 'region':
                        raise NotImplementedError("Read of region references from attributes not implemented in ZarrIO")
                    else:
                        raise NotImplementedError("Unsupported zarr_dtype for attribute " + str(v))
                else:
                    ret[k] = v
        return ret
