import numpy as np
import os.path
from zarr.hierarchy import Group
from zarr.core import Array
import numcodecs
import os
import itertools
from copy import deepcopy

import zarr
import tempfile
from six import raise_from, text_type, string_types, binary_type, text_type
from .zarr_utils import ZarrDataIO
from .zarr_utils import ZarrReference
from ..io import HDMFIO
from ...utils import docval, getargs, popargs, call_docval_func
from ...build import Builder, GroupBuilder, DatasetBuilder, LinkBuilder, BuildManager,\
                     RegionBuilder, ReferenceBuilder, TypeMap, ObjectMapper
from ...container import Container
from ...data_utils import AbstractDataChunkIterator, get_shape
from ...spec import RefSpec, DtypeSpec, NamespaceCatalog


ROOT_NAME = 'root'

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
        if self.__file is None:
            open_flag = self.__mode
            if self.__comm:
                sync_path = tempfile.mkdtemp()
                synchronizer = zarr.ProcessSynchronizer(sync_path)
                kwargs = {'synchronizer': synchronizer}
            else:
                kwargs = {}
            self.__file = zarr.open(self.__path, self.__mode, **kwargs)

    def close(self):
        self.__file = None
        return

    @docval({'name': 'builder', 'type': GroupBuilder, 'doc': 'the GroupBuilder object representing the NWBFile'},
            {'name': 'link_data', 'type': bool,
             'doc': 'If not specified otherwise link (True) or copy (False) Zarr Datasets', 'default': True})
    def write_builder(self, **kwargs):
        f_builder, link_data = getargs('builder', 'link_data', kwargs)
        for name, gbldr in f_builder.groups.items():
            self.write_group(self.__file, gbldr)
        for name, dbldr in f_builder.datasets.items():
            self.write_dataset(self.__file, dbldr, link_data)
        self.set_attributes(self.__file, f_builder.attributes)

    @docval({'name': 'parent', 'type': Group, 'doc': 'the parent Zarr object'},
            {'name': 'builder', 'type': GroupBuilder, 'doc': 'the GroupBuilder to write'},
            returns='the Group that was created', rtype='Group')
    def write_group(self, **kwargs):
        parent, builder = getargs('parent', 'builder', kwargs)
        if builder.written:
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
            if isinstance(value, (set, list, tuple)):
                tmp = tuple(value)
                obj.attrs[key] = tmp
            else:
                obj.attrs[key] = value

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
        else:
            return dtype == DatasetBuilder.OBJECT_REF_TYPE or dtype == DatasetBuilder.REGION_REF_TYPE


    #### Haven't implemented RegionReference
    @docval({'name': 'container', 'type': (Builder, Container, ReferenceBuilder), 'doc': 'the object to reference'},
            {'name': 'region', 'type': (slice, list, tuple), 'doc': 'the region reference indexing object',
             'default': None},
            returns='the reference', rtype=ZarrReference)
    def __get_ref(self, **kwargs):
        container, region = getargs('container', 'region', kwargs)
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
        if isinstance(container, RegionBuilder):
            region = container.region
        source = builder.source
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
        if builder.written:
            return
        name = builder.name
        target_builder = builder.builder
        path = self.__get_path(target_builder)
        self.__add_link__(parent, target_builder.source, path, name)
        builder.written = True

    @docval({'name': 'parent', 'type': Group, 'doc': 'the parent Zarr object'},  # noqa
            {'name': 'builder', 'type': DatasetBuilder, 'doc': 'the DatasetBuilder to write'},
            {'name': 'link_data', 'type': bool,
             'doc': 'If not specified otherwise link (True) or copy (False) Zarr Datasets', 'default': True},
            returns='the Zarr that was created', rtype=Array)
    def write_dataset(self, **kwargs):
        parent, builder, link_data = getargs('parent', 'builder', 'link_data', kwargs)
        if builder.written:
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

        if isinstance(data, Array):
            ## copy the dataset
            if link_data:
                self.__add_link__(parent, data.store.path, data.name, name)
                linked = True
            else:
                zarr.copy(data, parent, name = name)
                dset = parent[name]
        elif isinstance(options['dtype'], list):

            refs = list()
            type_str = list()
            for i ,dts in enumerate(options['dtype']):
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
                dset = parent.require_dataset(name, shape = (len(data), ), dtype = object
                                            , compressor = None
                                            , object_codec = numcodecs.JSON()
                                            , **options['io_settings'])
                builder.written = True
                dset.attrs['zarr_dtype'] = type_str
                for j, item in enumerate(data):
                    new_item = list(item)
                    for i in refs:
                        new_item[i] = self.__get_ref(item[i])
                    dset[j] = new_item
            else:
                ### write a compound datatype
                dset = self.__list_fill__(parent, name, data, options)

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
                typer_str = 'region'
                refs = list()
                for item in data:
                    refs.append(self.__get_ref(item.builder, item.region))
            else:
                shape = (len(data), )
                type_str = 'object'
                refs = list()
                for item in data:
                    refs.append(self.__get_ref(item))

            dset = parent.require_dataset(name, shape = shape, dtype = object
                                            , compressor = None
                                            , object_codec = numcodecs.JSON()
                                            , **options['io_settings'])
            builder.written = True
            dset.attrs['zarr_dtype'] = type_str
            if hasattr(refs, '__len__'):
                for i in range(0, len(refs)):
                    dset[i] = refs[i]
            else:
                dset[0] = refs

        ## write a 'regular' dataset without DatasetIO info
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
        ## Dont work when Reference in compound datatype
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
        elif isinstance(dtype, type):
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
        if not isinstance(dtype, type):
            try:
                dtype = cls.__resolve_dtype__(dtype, data)
            except Exception as exc:
                msg = 'cannot add %s to %s - could not determine type' % (name, parent.name)  # noqa: F821
                raise_from(Exception(msg), exc)

        type_str = cls.__serial_dtype__(dtype)


        if 'shape' in io_settings:
            data_shape = io_settings.pop('shape')
        elif isinstance(dtype, np.dtype):
            data_shape = (len(data), )
        else:
            data_shape = get_shape(data)

        if isinstance(dtype, np.dtype):
            dtype = object
            io_settings['object_codec'] = numcodecs.JSON()
            #chunks = io_settings['chunks']

        dset = parent.require_dataset(name, shape = data_shape, dtype = dtype, compressor = None, **io_settings)
        dset.attrs['zarr_dtype'] = type_str
        if dtype == object:
            corr = []
            for s in data_shape:
                corr.append(range(s))
            corr = tuple(corr)
            for c in itertools.product(*corr):
                o = data
                for i in c:
                    o = o[i]
                dset[c] = o
            return dset

        dset[:] = data
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
        dset = parent.require_dataset(name, shape=(1, ), dtype = dtype, compressor = None, **io_settings)
        dset[:] = data
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
        if ret != None:
            return ret

        kwargs = {
            "attributes": self.__read_attrs(zarr_obj),
            "groups": dict(),
            "datasets": dict(),
            "links": dict()
        }

        if name is None:
            name = str(os.path.basename(zarr_obj.name))

        ##read sub groups
        for sub_name, sub_group in zarr_obj.groups():
            sub_builder = self.__read_group(sub_group, sub_name)
            kwargs['groups'][sub_builder.name] = sub_builder

        ##read sub datasets
        for sub_name, sub_array in zarr_obj.arrays():
            sub_builder = self.__read_dataset(sub_array, sub_name)
            kwargs['datasets'][sub_builder.name] = sub_builder

        ##read links
        if 'zarr_link' in zarr_obj.attrs:
            links = zarr_obj.attrs['zarr_link']
            for link in links:
                l_name = link['name']
                if link['source'] is None:
                    l_path = str(link['path'])
                elif link['path'] is None:
                    l_path = str(link['source'])
                else:
                    l_path = str(link['source'] + "/" + link['path'])
                target_name = str(os.path.basename(l_path))
                target_zarr_obj = zarr.open(l_path, mode = 'r')
                if isinstance(target_zarr_obj, Group):
                    builder = self.__read_group(target_zarr_obj, target_name)
                else:
                    builder = self.__read_dataset(target_zarr_obj, target_name)
                link_builder = LinkBuilder(builder, l_name, source = self.__path)
                link_builder.written = True
                kwargs['links'][target_name] = link_builder

        kwargs['source'] = self.__path
        ret = GroupBuilder(name, **kwargs)
        ret.written = True
        self.__set_built(zarr_obj, ret)
        return ret

    def __read_dataset(self, zarr_obj, name):
        ret = self.__get_built(zarr_obj)
        if ret != None:
            return ret

        kwargs = {
            "attributes": self.__read_attrs(zarr_obj),
            "dtype": zarr_obj.attrs['zarr_dtype'],
            "maxshape": zarr_obj.shape,
            "chunks": not (zarr_obj.shape == zarr_obj.chunks)
        }

        kwargs['source'] = self.__path
        #data = deepcopy(zarr_obj[:])
        data = zarr_obj
        #kwargs['data'] = zarr_obj[:]

        dtype = kwargs['dtype']
        obj_refs = False
        reg_refs = False
        has_reference = False
        if isinstance(dtype, list):
            ##compound data type
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
            ### reference array
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
                path = source + path
            target_name = str(os.path.basename(path))
            target_zarr_obj = zarr.open(str(path), mode = 'r')

            o = data
            for i in range(0, len(p)-1):
                o = data[p[i]]
            o[p[-1]] = self.__read_dataset(target_zarr_obj, target_name)



    def __read_attrs(self, zarr_obj):
        ret = dict()
        for k in zarr_obj.attrs.keys():
            if k not in self.__reserve_attribute:
                v = zarr_obj.attrs[k]
                ret[k] = v
        return ret
