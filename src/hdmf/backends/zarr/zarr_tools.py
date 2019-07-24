import numpy as np
from zarr.hierarchy import Group
from zarr.core import Array
import numcodecs
import os
import itertools
from copy import deepcopy
import warnings

import zarr
import tempfile
from six import raise_from, string_types, binary_type, text_type
from .zarr_utils import ZarrDataIO, ZarrReference, ZarrSpecWriter, ZarrSpecReader
from ..io import HDMFIO
from ...utils import docval, getargs, popargs, call_docval_func
from ...build import Builder, GroupBuilder, DatasetBuilder, LinkBuilder, BuildManager,\
                     RegionBuilder, ReferenceBuilder, TypeMap  # , ObjectMapper
from ...container import Container
from ...data_utils import get_shape  # , AbstractDataChunkIterator,
from ...spec import RefSpec, DtypeSpec, NamespaceCatalog

from ..hdf5.h5tools import NamespaceIOHelper
from ...query import HDMFDataset
from ...container import Container

ROOT_NAME = 'root'
SPEC_LOC_ATTR = '.specloc'

# TODO We should 1) update the objectids when copying data between the backends, 2) reset builder.source before write (or create new set of builders), 3) reset builder.written (or create new set of builders), 4) resolve reference stored in datasets to the containers

class ZarrIO(HDMFIO):

    @docval({'name': 'path', 'type': str, 'doc': 'the path to the Zarr file'},
            {'name': 'manager', 'type': BuildManager, 'doc': 'the BuildManager to use for I/O', 'default': None},
            {'name': 'mode', 'type': str,
             'doc': 'the mode to open the Zarr file with, one of ("w", "r", "r+", "a", "w-")'},
            {'name': 'comm', 'type': 'Intracomm',
             'doc': 'the MPI communicator to use for parallel I/O', 'default': None})
    def __init__(self, **kwargs):
        path, manager, mode, comm = popargs('path', 'manager', 'mode', 'comm', kwargs)
        if manager is None:
            manager = BuildManager(TypeMap(NamespaceCatalog()))
        self.__comm = comm
        self.__mode = mode
        self.__path = path
        self.__file = None
        self.__built = dict()
        super(ZarrIO, self).__init__(manager, source=path)

    @property
    def comm(self):
        return self.__comm

    def open(self):
        """Open the Zarr file"""
        if self.__file is None:
            if self.__comm:
                sync_path = tempfile.mkdtemp()
                synchronizer = zarr.ProcessSynchronizer(sync_path)
                kwargs = {'synchronizer': synchronizer}
            else:
                kwargs = {}
            self.__file = zarr.open(self.__path, self.__mode, **kwargs)

    def close(self):
        """Close the Zarr file"""
        self.__file = None
        return

    @classmethod
    @docval({'name': 'namespace_catalog',
             'type': (NamespaceCatalog, TypeMap),
             'doc': 'the NamespaceCatalog or TypeMap to load namespaces into'},
            {'name': 'path', 'type': str, 'doc': 'the path to the HDF5 file'},
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
            {'name': 'cache_spec', 'type': bool, 'doc': 'cache specification to file', 'default': False},
            {'name': 'link_data', 'type': bool,
             'doc': 'If not specified otherwise link (True) or copy (False) HDF5 Datasets', 'default': True})
    def write(self, **kwargs):
        """Overwrite the write method to add support for caching the specification"""
        cache_spec = popargs('cache_spec', kwargs)
        call_docval_func(super(ZarrIO, self).write, kwargs)
        if cache_spec:
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
                ns_builder = NamespaceIOHelper.convert_namespace(ns_catalog, ns_name)
                namespace = ns_catalog.get_namespace(ns_name)
                if namespace.version is None:
                    group_name = '%s/unversioned' % ns_name
                else:
                    group_name = '%s/%s' % (ns_name, namespace.version)
                ns_group = spec_group.require_group(group_name)
                writer = ZarrSpecWriter(ns_group)
                ns_builder.export('namespace', writer=writer)

    @docval({'name': 'builder', 'type': GroupBuilder, 'doc': 'the GroupBuilder object representing the NWBFile'},
            {'name': 'link_data', 'type': bool,
             'doc': 'If not specified otherwise link (True) or copy (False) Zarr Datasets', 'default': True})
    def write_builder(self, **kwargs):
        """Write a builder to disk"""
        f_builder, link_data = getargs('builder', 'link_data', kwargs)
        for name, gbldr in f_builder.groups.items():
            self.write_group(self.__file, gbldr)
        for name, dbldr in f_builder.datasets.items():
            self.write_dataset(self.__file, dbldr, link_data)
        self.set_attributes(self.__file, f_builder.attributes)

    def __builder_written_to_zarr(self, builder, parent):
        """Check whether a given builder has been written to disk"""
        # TODO Ideally we should only have to check builder.written. However, when copying between backends builder.written will be set to True because it was written on the other backend. This should be fixed in the Builders or BuildManager prior to write to be useful across backends.
        builder_path = os.path.join(self.__path, os.path.join(parent.name, builder.name).lstrip('/'))
        exists_on_disk =  os.path.exists(builder_path)
        return builder.written and exists_on_disk  # If the file was previously written to an HDF5 file then we need to create the group

    @docval({'name': 'parent', 'type': Group, 'doc': 'the parent Zarr object'},
            {'name': 'builder', 'type': GroupBuilder, 'doc': 'the GroupBuilder to write'},
            returns='the Group that was created', rtype='Group')
    def write_group(self, **kwargs):
        """Write a GroupBuider to file"""
        parent, builder = getargs('parent', 'builder', kwargs)
        if self.__builder_written_to_zarr(builder, parent):
            group = parent[builder.name]
        else:
            group = parent.require_group(builder.name)

        subgroups = builder.groups
        if subgroups:
            for subgroup_name, sub_builder in subgroups.items():
                self.write_group(group, sub_builder)

        datasets = builder.datasets
        if datasets:
            for dset_name, sub_builder in datasets.items():
                self.write_dataset(group, sub_builder)

        # write all links (haven implemented)
        links = builder.links
        if links:
            for link_name, sub_builder in links.items():
                self.write_link(group, sub_builder)

        attributes = builder.attributes
        self.set_attributes(group, attributes)
        builder.written = True
        return group

    @docval({'name': 'obj', 'type': (Group, Array), 'doc': 'the Zarr object to add attributes to'},
            {'name': 'attributes',
             'type': dict,
             'doc': 'a dict containing the attributes on the Group or Dataset, indexed by attribute name'})
    def set_attributes(self, **kwargs):
        obj, attributes = getargs('obj', 'attributes', kwargs)
        for key, value in attributes.items():
            # Case 1: list, set, tuple type attributes
            if isinstance(value, (set, list, tuple)) or (isinstance(value, np.ndarray) and len(value) > 1):
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
                    except:  # noqa: E272
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
                        val = value.item() \
                            if (isinstance(value, np.generic) and not isinstance(value, np.bytes_)) \
                            else value.decode("utf-8") \
                            if isinstance(value, (bytes, np.bytes_)) \
                            else value
                        obj.attrs[key] = val
                        write_ok = True
                    except:  # noqa: E272
                        pass
                    if not write_ok:
                        raise TypeError(str(e) + "key=" + key + " type=" + str(type(value)) + "  data=" + str(value))

    def __get_path(self, builder):
        curr = builder
        names = list()
        while curr is not None and curr.name != ROOT_NAME:
            names.append(curr.name)
            curr = curr.parent
        delim = "/"
        path = "%s%s" % (delim, delim.join(reversed(names)))
        return path

    def __is_ref(self, dtype):
        if isinstance(dtype, DtypeSpec):
            return self.__is_ref(dtype.dtype)
        elif isinstance(dtype, RefSpec):
            return True
        elif isinstance(dtype, np.dtype):
            return False
        else:
            return dtype == DatasetBuilder.OBJECT_REF_TYPE or dtype == DatasetBuilder.REGION_REF_TYPE

    # TODO Haven't implemented RegionReference
    @docval({'name': 'container', 'type': (Builder, Container, ReferenceBuilder), 'doc': 'the object to reference'},
            {'name': 'region', 'type': (slice, list, tuple), 'doc': 'the region reference indexing object',
             'default': None},
            returns='the reference', rtype=ZarrReference)
    def __get_ref(self, **kwargs):
        """Create a ZarrReference object that points to the given container"""
        container, region = getargs('container', 'region', kwargs)
        if isinstance(container, RegionBuilder) or region is not None:
            raise NotImplementedError("Region references are currently not supported by ZarrIO")
        if isinstance(container, Builder):
            if isinstance(container, LinkBuilder):
                builder = container.target_builder
            else:
                builder = container
        elif isinstance(container, ReferenceBuilder):
            builder = container.builder
        else:
            builder = self.manager.build(container)
        path = self.__get_path(builder)
        # TODO Add to get region for region references
        # if isinstance(container, RegionBuilder):
        #    region = container.region

        # TODO When converting from HDF5 to Zarr the builder.source will already be set to HDF5 file. However, we can't link to that from Zarr. I.e., we need to force to change the path to Zarr.
        # TODO The issue of the bad builder.source should be fixed in the builders or BuildManager prior to write to be useful across backends
        if os.path.isdir(builder.source):
            source = builder.source
        else:
            source = self.__path

        return ZarrReference(source, path)

    def __add_link__(self, parent, target_source, target_path, link_name):
        if 'zarr_link' not in parent.attrs:
            parent.attrs['zarr_link'] = []
        zarr_link = list(parent.attrs['zarr_link'])
        zarr_link.append({'source': target_source, 'path': target_path, 'name': link_name})
        parent.attrs['zarr_link'] = zarr_link

    @docval({'name': 'parent', 'type': Group, 'doc': 'the parent HDF5 object'},
            {'name': 'builder', 'type': LinkBuilder, 'doc': 'the LinkBuilder to write'})
    def write_link(self, **kwargs):
        parent, builder = getargs('parent', 'builder', kwargs)
        if self.__builder_written_to_zarr(builder, parent):
            return
        name = builder.name
        target_builder = builder.builder
        zarr_ref = self.__get_ref(target_builder)
        self.__add_link__(parent, zarr_ref.source, zarr_ref.path, name)
        builder.written = True

    @docval({'name': 'parent', 'type': Group, 'doc': 'the parent Zarr object'},  # noqa
            {'name': 'builder', 'type': DatasetBuilder, 'doc': 'the DatasetBuilder to write'},
            {'name': 'link_data', 'type': bool,
             'doc': 'If not specified otherwise link (True) or copy (False) Zarr Datasets', 'default': True},
            returns='the Zarr that was created', rtype=Array)
    def write_dataset(self, **kwargs):
        parent, builder, link_data = getargs('parent', 'builder', 'link_data', kwargs)
        if self.__builder_written_to_zarr(builder, parent):
            return None
        name = builder.name
        data = builder.data
        options = dict()
        if isinstance(data, ZarrDataIO):
            options['io_settings'] = data.io_settings
            link_data = data.link_data
            data = data.data
        else:
            options['io_settings'] = {}

        attributes = builder.attributes
        options['dtype'] = builder.dtype

        linked = False

        # Write a regular Zarr array
        if isinstance(data, Array):
            # copy the dataset
            if link_data:
                self.__add_link__(parent, data.store.path, data.name, name)
                linked = True
            else:
                zarr.copy(data, parent, name=name)
                dset = parent[name]
        # When converting data between backends we may encounter and HDMFDataset, e.g., a H55ReferenceDataset with references
        # TODO this conversion from HDMFDataset should happen for the builder outside of the I/O backend prior to write in order to be usable across backends
        elif isinstance(data, HDMFDataset):
            # If we have a dataset of containers we need to make the references to the containers
            if len(data) > 0 and isinstance(data[0], Container):
                ref_data = [self.__get_ref(data[i]) for i in range(len(data))]
                shape = (len(data), )
                type_str = 'object'
                dset = parent.require_dataset(name,
                                              shape=shape,
                                              dtype=object,
                                              compressor=None,
                                              object_codec=numcodecs.JSON(),
                                              **options['io_settings'])
                dset.attrs['zarr_dtype'] = type_str
                dset[:] = ref_data
                builder.written = True
            # If we have a regular dataset, then load the data and write the builder after load
            else:
                cp_builder = deepcopy(builder)
                cp_builder.data = data[:]
                self.write_dataset(parent, cp_builder, link_data)
                builder.written = True
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
                                              compressor=None,
                                              object_codec=numcodecs.JSON(),
                                              **options['io_settings'])
                builder.written = True
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
                                          compressor=None,
                                          object_codec=numcodecs.JSON(),
                                          **options['io_settings'])
            builder.written = True
            dset.attrs['zarr_dtype'] = type_str
            if hasattr(refs, '__len__'):
                dset[:] = refs
            else:
                dset[0] = refs
        # write a 'regular' dataset without DatasetIO info
        else:
            if isinstance(data, (text_type, binary_type)):
                dset = self.__scalar_fill__(parent, name, data, options)
            elif hasattr(data, '__len__'):
                dset = self.__list_fill__(parent, name, data, options)
            else:
                dset = self.__scalar_fill__(parent, name, data, options)
        if not linked:
            self.set_attributes(dset, attributes)
        builder.written = True
        return

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
        "text": binary_type,
        "utf": text_type,
        "utf8": text_type,
        "utf-8": text_type,
        "ascii": binary_type,
        "str": binary_type,
        "isodatetime": binary_type,
        "string_": binary_type,
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
        if isinstance(data, (text_type, string_types)):
            return str
        elif not hasattr(data, '__len__'):
            return type(data)
        else:
            if len(data) == 0:
                raise ValueError('cannot determine type for empty data')
            return cls.get_type(data[0])

    __reserve_attribute = ('zarr_dtype', 'zarr_link')

    @classmethod
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
                raise_from(Exception(msg), exc)

        # Set the type_str
        type_str = cls.__serial_dtype__(dtype)

        # Determine the shape and update the dtype if necessary when dtype==object
        if 'shape' in io_settings:  # Use the shape set by the user
            data_shape = io_settings.pop('shape')
        # If we have a numeric numpy array then use its shape
        elif isinstance(dtype, np.dtype) and np.issubdtype(dtype, np.number) or dtype == np.bool_:
            data_shape = get_shape(data)
        # Deal with object dtype
        elif isinstance(dtype, np.dtype):
            data = data[:]  # load the data in case we come from HDF5
            data_shape = (len(data), )
            dtype = object
             # sometimes bytes and strings can hide as object in numpy array so lets try to write those as strings and bytes rathern than as objects
            if len(data) > 0 and isinstance(data, np.ndarray):
                if isinstance(data.item(0), bytes):
                    dtype=bytes
                    data_shape = get_shape(data)
                elif isinstance(data.item(0), str):
                    dtype=str
                    data_shape = get_shape(data)
            # Set encoding for objects
            if dtype == object:
                io_settings['object_codec'] = numcodecs.JSON()
        # Determine the shape from the data if all other cases have not been hit
        else:
            data_shape = get_shape(data)

        # Create the dataset
        dset = parent.require_dataset(name, shape=data_shape, dtype=dtype, compressor=None, **io_settings)
        dset.attrs['zarr_dtype'] = type_str

        # Write the data to file
        if dtype == object:
            for c in np.ndindex(data_shape):
                o = data
                for i in c:
                    o = o[i]
                dset[c] = o  if not isinstance(o, (bytes, np.bytes_)) else o.decode("utf-8") # bytes are not JSON serializable
            return dset
        # standard write
        else:
            dset[:] = data  # If data is an h5py.Dataset then this will copy the data

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
                msg = 'cannot add %s to %s - could not determine type' % (name, parent.name)  # noqa: F821
                raise_from(Exception(msg), exc)
        if dtype == object:
            io_settings['object_codec'] = numcodecs.JSON()

        dset = parent.require_dataset(name, shape=(1, ), dtype=dtype, compressor=None, **io_settings)
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
        fpath = zarr_obj.store.path
        path = zarr_obj.path
        path = os.path.join(fpath, path)
        return self.__built.get(path)

    def __read_group(self, zarr_obj, name=None):
        ret = self.__get_built(zarr_obj)
        if ret is not None:
            return ret

        kwargs = {
            "attributes": self.__read_attrs(zarr_obj),
            "groups": dict(),
            "datasets": dict(),
            "links": dict()
        }

        if name is None:
            name = str(os.path.basename(zarr_obj.name))

        # read sub groups
        for sub_name, sub_group in zarr_obj.groups():
            sub_builder = self.__read_group(sub_group, sub_name)
            kwargs['groups'][sub_builder.name] = sub_builder

        # read sub datasets
        for sub_name, sub_array in zarr_obj.arrays():
            sub_builder = self.__read_dataset(sub_array, sub_name)
            kwargs['datasets'][sub_builder.name] = sub_builder

        # read links
        if 'zarr_link' in zarr_obj.attrs:
            links = zarr_obj.attrs['zarr_link']
            for link in links:
                l_name = link['name']
                if link['source'] is None:
                    l_path = str(link['path'])
                elif link['path'] is None:
                    l_path = str(link['source'])
                else:
                    l_path = os.path.join(link['source'], link['path'].lstrip("/"))

                if not os.path.exists(l_path):
                    raise ValueError("Found bad link %s to %s" % (l_name, l_path))

                target_name = str(os.path.basename(l_path))
                target_zarr_obj = zarr.open(l_path, mode='r')
                if isinstance(target_zarr_obj, Group):
                    builder = self.__read_group(target_zarr_obj, target_name)
                else:
                    builder = self.__read_dataset(target_zarr_obj, target_name)
                link_builder = LinkBuilder(builder, l_name, source=self.__path)
                link_builder.written = True
                kwargs['links'][target_name] = link_builder

        kwargs['source'] = self.__path
        ret = GroupBuilder(name, **kwargs)
        ret.written = True
        self.__set_built(zarr_obj, ret)
        return ret

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
            data = deepcopy(data[:])
            self.__parse_ref(kwargs['maxshape'], obj_refs, reg_refs, data)

        kwargs['data'] = data
        if name is None:
            name = str(os.path.basename(zarr_obj.name))
        ret = DatasetBuilder(name, **kwargs)
        ret.written = True
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
                            path =  os.path.join(source, path.lstrip("/"))

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

