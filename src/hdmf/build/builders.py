import numpy as np
from h5py import RegionReference
import copy as _copy
import itertools as _itertools
import posixpath as _posixpath
from abc import ABCMeta
import warnings
from collections.abc import Iterable
from collections import namedtuple
from datetime import datetime

from ..utils import docval, getargs, popargs, fmt_docval_args, get_docval


class Builder(dict, metaclass=ABCMeta):
    ''' Abstract class used to represent an object within a hierarchy. '''

    @docval({'name': 'name', 'type': str, 'doc': 'the name of the Builder'},
            {'name': 'parent', 'type': 'Builder', 'doc': 'the parent Builder of this Builder', 'default': None},
            {'name': 'source', 'type': str,
             'doc': 'the source of the data in this Builder, e.g., file name', 'default': None})
    def __init__(self, **kwargs):
        name, parent, source = getargs('name', 'parent', 'source', kwargs)
        super().__init__()
        self.__name = name
        self.__parent = parent
        if source is not None:
            self.__source = source
        elif parent is not None:
            self.__source = parent.source
        else:
            self.__source = None
        self.__written = False

    @property
    def path(self):
        ''' The path of this Builder '''
        s = list()
        c = self
        while c is not None:
            s.append(c.name)
            c = c.parent
        return "/".join(s[::-1])

    @property
    def written(self):
        ''' The source of this Builder '''
        return self.__written

    @written.setter
    def written(self, s):
        if self.__written and not s:
            raise AttributeError('Cannot change written to not written')
        self.__written = s

    @property
    def name(self):
        ''' The name of this Builder '''
        return self.__name

    @property
    def source(self):
        ''' The source of this Builder '''
        return self.__source

    @source.setter
    def source(self, s):
        if self.__source is not None:
            raise AttributeError('Cannot reset source once it is specified')
        self.__source = s

    @property
    def parent(self):
        ''' The parent Builder of this Builder '''
        return self.__parent

    @parent.setter
    def parent(self, p):
        if self.__parent is not None:
            raise AttributeError('Cannot reset parent once it is specified')
        self.__parent = p
        if self.__source is None:
            self.source = p.source

    def __repr__(self):
        dict_repr = super().__repr__()
        ret = "%s %s %s" % (self.name, self.__class__.__name__, dict_repr)
        return ret


class BaseBuilder(Builder):
    ''' A builder that contains a location and a dictionary of attributes '''

    __attribute = 'attributes'  # key for attributes dictionary in this dictionary

    @docval({'name': 'name', 'type': str, 'doc': 'the name of the Builder'},
            {'name': 'attributes', 'type': dict, 'doc': 'a dictionary of attributes to create in this Builder',
             'default': dict()},
            {'name': 'parent', 'type': 'GroupBuilder', 'doc': 'the parent Builder of this Builder', 'default': None},
            *get_docval(Builder.__init__, 'source'))
    def __init__(self, **kwargs):
        name, attributes, parent, source = getargs('name', 'attributes', 'parent', 'source', kwargs)
        super().__init__(name, parent, source)
        super().__setitem__(BaseBuilder.__attribute, dict())
        for name, val in attributes.items():
            self.set_attribute(name, val)
        self.__location = None

    @property
    def location(self):
        ''' The location of this Builder in its source '''
        return self.__location

    @location.setter
    def location(self, val):
        self.__location = val

    @property
    def attributes(self):
        ''' The attributes stored in this Builder object '''
        return super().__getitem__(BaseBuilder.__attribute)

    @docval({'name': 'name', 'type': str, 'doc': 'the name of the attribute'},
            {'name': 'value', 'type': None, 'doc': 'the attribute value'})
    def set_attribute(self, **kwargs):
        ''' Set an attribute for this Builder '''
        name, value = getargs('name', 'value', kwargs)
        self.attributes[name] = value

    @docval({'name': 'builder', 'type': 'BaseBuilder', 'doc': 'the BaseBuilder to merge attributes from'})
    def deep_update(self, **kwargs):
        ''' Merge attributes from the given BaseBuilder into this Builder '''
        builder = kwargs['builder']
        for name, value in builder.attributes.items():
            self.set_attribute(name, value)


class GroupBuilder(BaseBuilder):
    '''
    This class is a dictionary that holds the contents of a Container rearranged based on the ObjectMapper for that
    Container into subgroups, datasets, attributes, and links. On write, the backend code writes the contents of these
    builders to the backend. On read, the backend code creates builders to hold data that has been read from the
    backend.
    '''

    # keys for child dictionaries/lists in this dictionary
    __link = 'links'
    __group = 'groups'
    __dataset = 'datasets'
    __attribute = 'attributes'  # matches BaseBuilder.__attribute

    @docval({'name': 'name', 'type': str, 'doc': 'the name of the group'},
            {'name': 'groups', 'type': (dict, list), 'doc': 'a dictionary/list of subgroups to create in this group',
             'default': list()},
            {'name': 'datasets', 'type': (dict, list), 'doc': 'a dictionary/list of datasets to create in this group',
             'default': list()},
            {'name': 'attributes', 'type': dict, 'doc': 'a dictionary of attributes to create in this group',
             'default': dict()},
            {'name': 'links', 'type': (dict, list), 'doc': 'a dictionary/list of links to create in this group',
             'default': list()},
            *get_docval(BaseBuilder.__init__, 'parent', 'source'))
    def __init__(self, **kwargs):
        name, groups, datasets, links, attributes, parent, source = getargs(
            'name', 'groups', 'datasets', 'links', 'attributes', 'parent', 'source', kwargs)
        # convert groups, datasets, links to lists based on dict values if dict is given (i.e., ignore dict keys)
        groups = self.__to_list(groups)
        datasets = self.__to_list(datasets)
        links = self.__to_list(links)
        self.obj_type = dict()
        super().__init__(name, attributes, parent, source)  # superclass handles attributes
        super().__setitem__(GroupBuilder.__group, dict())
        super().__setitem__(GroupBuilder.__dataset, dict())
        super().__setitem__(GroupBuilder.__link, dict())
        for group in groups:
            self.set_group(group)
        for dataset in datasets:
            if dataset is not None:
                self.set_dataset(dataset)
        for link in links:
            self.set_link(link)

    def __to_list(self, d):
        if isinstance(d, dict):
            return list(d.values())
        return d

    @property
    def source(self):
        ''' The source of this Builder '''
        return super().source

    @source.setter
    def source(self, s):
        ''' A recursive setter to set all subgroups/datasets/links source when this source is set '''
        super(GroupBuilder, self.__class__).source.fset(self, s)  # call parent setter
        for g in self.groups.values():
            if g.source is None:
                g.source = s
        for d in self.datasets.values():
            if d.source is None:
                d.source = s
        for l in self.links.values():
            if l.source is None:
                l.source = s

    @property
    def groups(self):
        ''' The subgroups contained in this GroupBuilder '''
        return super().__getitem__(GroupBuilder.__group)

    @property
    def datasets(self):
        ''' The datasets contained in this GroupBuilder '''
        return super().__getitem__(GroupBuilder.__dataset)

    @property
    def links(self):
        ''' The links contained in this GroupBuilder '''
        return super().__getitem__(GroupBuilder.__link)

    @docval(*get_docval(BaseBuilder.set_attribute))
    def set_attribute(self, **kwargs):
        ''' Set an attribute for this group '''
        name, value = getargs('name', 'value', kwargs)
        super().set_attribute(name, value)
        self.obj_type[name] = GroupBuilder.__attribute  # track that this name is associated with an attribute

    @docval({'name': 'builder', 'type': 'Builder', 'doc': 'the Builder to add to this GroupBuilder'})
    def set_builder(self, **kwargs):
        ''' Add an existing Builder to this GroupBuilder '''
        builder = kwargs['builder']
        if isinstance(builder, LinkBuilder):
            self.__set_builder(builder, GroupBuilder.__link)
        elif isinstance(builder, GroupBuilder):
            self.__set_builder(builder, GroupBuilder.__group)
        elif isinstance(builder, DatasetBuilder):
            self.__set_builder(builder, GroupBuilder.__dataset)
        else:
            raise ValueError('Got unexpected builder type: %s' % type(builder))

    def __set_builder(self, builder, obj_type):
        ''' Store the given child builder in the groups/datasets/attributes/links dict under its name as the key '''
        name = builder.name
        if name in self.obj_type:
            if self.obj_type[name] != obj_type:
                if name == 'comments':
                    # LEGACY: Support legacy files where "comments" exists as both an attribute and as dataset
                    #         in some groups.
                    #         To allow read to get past this special case, this will skip the issue.
                    warnings.warn("'%s' already exists as %s; skipping..." % (name, self.obj_type[name]))
                else:
                    raise KeyError("'%s' already exists as %s in %s, cannot set as %s" %
                                   (name, self.obj_type[name], self.name, obj_type))
        super().__getitem__(obj_type)[name] = builder
        self.obj_type[name] = obj_type
        if builder.parent is None:
            builder.parent = self

    # these are the same docval args as for DatasetBuilder, except this omits parent and source and does not allow a
    # RegionBuilder or datatime for data
    @docval({'name': 'name', 'type': str, 'doc': 'the name of this dataset'},
            {'name': 'data', 'type': ('array_data', 'scalar_data', 'data', 'DatasetBuilder', Iterable),
             'doc': 'a dictionary of datasets to create in this dataset', 'default': None},
            {'name': 'dtype', 'type': (type, np.dtype, str, list),
             'doc': 'the datatype of this dataset', 'default': None},
            {'name': 'attributes', 'type': dict,
             'doc': 'a dictionary of attributes to create in this dataset', 'default': dict()},
            {'name': 'maxshape', 'type': (int, tuple),
             'doc': 'the shape of this dataset. Use None for scalars', 'default': None},
            {'name': 'chunks', 'type': bool, 'doc': 'whether or not to chunk this dataset', 'default': False},
            {'name': 'dims', 'type': (list, tuple), 'doc': 'a list of dimensions of this dataset', 'default': None},
            {'name': 'coords', 'type': dict, 'doc': 'a dictionary of coordinates of this dataset',
             'default': None},
            returns='the DatasetBuilder object for the dataset', rtype='DatasetBuilder')
    def add_dataset(self, **kwargs):
        ''' Create a dataset and add it to this group, setting parent and source '''
        kwargs['parent'] = self
        pargs, pkwargs = fmt_docval_args(DatasetBuilder.__init__, kwargs)
        builder = DatasetBuilder(*pargs, **pkwargs)
        self.set_dataset(builder)
        return builder

    @docval({'name': 'builder', 'type': 'DatasetBuilder', 'doc': 'the DatasetBuilder that represents this dataset'})
    def set_dataset(self, **kwargs):
        ''' Add a dataset to this group '''
        builder = getargs('builder', kwargs)
        self.__set_builder(builder, GroupBuilder.__dataset)

    # these are the same docval args as for GroupBuilder, except this omits parent and source and does not allow
    # lists for groups, datasets, and links and has different default values accordingly
    # TODO: groups, datasets, and links should really be lists with default list() because the keys are all ignored.
    @docval({'name': 'name', 'type': str, 'doc': 'the name of this subgroup'},
            {'name': 'groups', 'type': dict,
             'doc': 'a dictionary of subgroups to create in this subgroup', 'default': dict()},
            {'name': 'datasets', 'type': dict,
             'doc': 'a dictionary of datasets to create in this subgroup', 'default': dict()},
            {'name': 'attributes', 'type': dict,
             'doc': 'a dictionary of attributes to create in this subgroup', 'default': dict()},
            {'name': 'links', 'type': dict,
             'doc': 'a dictionary of links to create in this subgroup', 'default': dict()},
            returns='the GroupBuilder object for the subgroup', rtype='GroupBuilder')
    def add_group(self, **kwargs):
        ''' Add a subgroup with the given data to this group '''
        name = kwargs.pop('name')
        builder = GroupBuilder(name, parent=self, **kwargs)
        self.set_group(builder)
        return builder

    @docval({'name': 'builder', 'type': 'GroupBuilder', 'doc': 'the GroupBuilder that represents this subgroup'})
    def set_group(self, **kwargs):
        ''' Add a subgroup to this group '''
        builder = getargs('builder', kwargs)
        self.__set_builder(builder, GroupBuilder.__group)

    # these are the same docval args as for LinkBuilder, except this omits parent and source
    @docval({'name': 'target', 'type': ('GroupBuilder', 'DatasetBuilder'), 'doc': 'the target Builder'},
            {'name': 'name', 'type': str, 'doc': 'the name of this link', 'default': None},
            returns='the builder object for the soft link', rtype='LinkBuilder')
    def add_link(self, **kwargs):
        ''' Create a soft link and add it to this group '''
        name, target = getargs('name', 'target', kwargs)
        builder = LinkBuilder(target, name, parent=self)
        self.set_link(builder)
        return builder

    @docval({'name': 'builder', 'type': 'LinkBuilder', 'doc': 'the LinkBuilder that represents this link'})
    def set_link(self, **kwargs):
        ''' Add a link to this group '''
        builder = getargs('builder', kwargs)
        self.__set_builder(builder, GroupBuilder.__link)

    # TODO: write unittests for this method
    @docval({'name': 'builder', 'type': 'GroupBuilder', 'doc': 'the GroupBuilder to merge into this GroupBuilder'})
    def deep_update(self, **kwargs):
        ''' Recursively merge subgroups, datasets, and links rrom the given builder into this group '''
        builder = kwargs['builder']
        super().deep_update(builder)
        # merge subgroups
        for name, subgroup in builder.groups.items():
            if name in self.groups:
                self.groups[name].deep_update(subgroup)
            else:
                self.set_group(subgroup)
        # merge datasets
        for name, dataset in builder.datasets.items():
            if name in self.datasets:
                self.datasets[name].deep_update(dataset)
            else:
                self.set_dataset(dataset)
        # merge links
        for name, link in builder.links.items():
            self.set_link(link)

    def is_empty(self):
        '''
        Returns True if there are no datasets, attributes, links or subgroups that contain datasets, attributes or
        links. False otherwise.
        '''
        if (len(super().__getitem__(GroupBuilder.__dataset)) or
                len(super().__getitem__(GroupBuilder.__attribute)) or
                len(super().__getitem__(GroupBuilder.__link))):
            return False
        elif len(super().__getitem__(GroupBuilder.__group)):
            return all(g.is_empty() for g in super().__getitem__(GroupBuilder.__group).values())
        else:
            return True

    def __getitem__(self, key):
        ''' Like dict.__getitem__, but looks in groups, datasets, attributes, and links sub-dictionaries. '''
        try:
            key_ar = _posixpath.normpath(key).split('/')
            return self.__get_rec(key_ar)
        except KeyError:
            raise KeyError(key)

    def get(self, key, default=None):
        ''' Like dict.get, but looks in groups, datasets, attributes, and links sub-dictionaries. '''
        try:
            key_ar = _posixpath.normpath(key).split('/')
            return self.__get_rec(key_ar)
        except KeyError:
            return default

    def __get_rec(self, key_ar):
        # recursive helper for __getitem__
        if len(key_ar) == 1:
            return super().__getitem__(self.obj_type[key_ar[0]])[key_ar[0]]
        else:
            if key_ar[0] in super().__getitem__(GroupBuilder.__group):
                return super().__getitem__(GroupBuilder.__group)[key_ar[0]].__get_rec(key_ar[1:])
        raise KeyError(key_ar[0])

    def __setitem__(self, args, val):
        raise NotImplementedError('__setitem__')

    def __contains__(self, item):
        return self.obj_type.__contains__(item)

    def items(self):
        '''
        Like dict.items, but iterates over key-value pairs in groups, datasets, attributes, and links sub-dictionaries.
        '''
        return _itertools.chain(super().__getitem__(GroupBuilder.__group).items(),
                                super().__getitem__(GroupBuilder.__dataset).items(),
                                super().__getitem__(GroupBuilder.__attribute).items(),
                                super().__getitem__(GroupBuilder.__link).items())

    def keys(self):
        ''' Like dict.keys, but iterates over keys in groups, datasets, attributes, and links sub-dictionaries. '''
        return _itertools.chain(super().__getitem__(GroupBuilder.__group).keys(),
                                super().__getitem__(GroupBuilder.__dataset).keys(),
                                super().__getitem__(GroupBuilder.__attribute).keys(),
                                super().__getitem__(GroupBuilder.__link).keys())

    def values(self):
        ''' Like dict.values, but iterates over values in groups, datasets, attributes, and links sub-dictionaries. '''
        return _itertools.chain(super().__getitem__(GroupBuilder.__group).values(),
                                super().__getitem__(GroupBuilder.__dataset).values(),
                                super().__getitem__(GroupBuilder.__attribute).values(),
                                super().__getitem__(GroupBuilder.__link).values())


class DatasetBuilder(BaseBuilder):
    '''
    This class is a dictionary that holds a particular dataset of a Container (e.g., scalar, array, string, etc.) as
    well as any fields relevant to the data, such as data type, maxshape, whether to chunk the data, its dimensions,
    and its coordinates.
    '''

    OBJECT_REF_TYPE = 'object'
    REGION_REF_TYPE = 'region'

    @docval({'name': 'name', 'type': str, 'doc': 'the name of the dataset'},
            {'name': 'data',
             'type': ('array_data', 'scalar_data', 'data', 'DatasetBuilder', 'RegionBuilder', Iterable, datetime),
             'doc': 'the data in this dataset', 'default': None},
            {'name': 'dtype', 'type': (type, np.dtype, str, list),
             'doc': 'the datatype of this dataset', 'default': None},
            {'name': 'attributes', 'type': dict,
             'doc': 'a dictionary of attributes to create in this dataset', 'default': dict()},
            {'name': 'maxshape', 'type': (int, tuple),
             'doc': 'the shape of this dataset. Use None for scalars', 'default': None},
            {'name': 'chunks', 'type': bool, 'doc': 'whether or not to chunk this dataset', 'default': False},
            {'name': 'dims', 'type': (list, tuple), 'doc': 'a list of dimensions of this dataset', 'default': None},
            {'name': 'coords', 'type': dict, 'doc': 'a dictionary of coordinates of this dataset',
             'default': None},
            *get_docval(BaseBuilder.__init__, 'parent', 'source'))
    def __init__(self, **kwargs):
        ''' Create a Builder object for a dataset '''
        name, data, dtype, attributes, maxshape, chunks, parent, source, dims, coords = getargs(
            'name', 'data', 'dtype', 'attributes', 'maxshape', 'chunks', 'parent', 'source', 'dims', 'coords', kwargs)
        super().__init__(name, attributes, parent, source)
        self['attributes'] = _copy.copy(attributes)  # TODO: is this necessary? it is set (but not copied) earlier
        self['data'] = data
        self['dims'] = dims
        self['coords'] = coords
        self.__chunks = chunks
        self.__maxshape = maxshape

        # if data is a group/dataset/link builder and dtype is not provided, set dtype to represent an object reference
        if isinstance(data, BaseBuilder) and dtype is None:
            dtype = self.OBJECT_REF_TYPE
        self.__dtype = dtype

    @property
    def data(self):
        ''' The data stored in the dataset represented by this builder '''
        return self['data']

    @data.setter
    def data(self, val):
        if self['data'] is not None:
            raise AttributeError('Cannot reset data once it is specified')
        self['data'] = val

    @property
    def dims(self):
        ''' The dimensions of the dataset represented by this builder '''
        return self['dims']

    @dims.setter
    def dims(self, val):
        if self['dims'] is not None:
            raise AttributeError('Cannot reset dims once it is specified')
        self['dims'] = val

    @property
    def coords(self):
        ''' The coordinates of the dataset represented by this builder '''
        return self['coords']

    @coords.setter
    def coords(self, val):
        if self['coords'] is not None:
            raise AttributeError('Cannot reset coords once it is specified')
        self['coords'] = val

    @property
    def chunks(self):
        ''' Whether or not this dataset is chunked '''
        return self.__chunks

    @property
    def maxshape(self):
        ''' The max shape of this object '''
        return self.__maxshape

    @property
    def dtype(self):
        ''' The data type of this object '''
        return self.__dtype

    @dtype.setter
    def dtype(self, val):
        ''' The data type of this object '''
        if self.__dtype is not None:
            raise AttributeError('Cannot reset dtype once it is specified')
        self.__dtype = val

    @docval({'name': 'dataset', 'type': 'DatasetBuilder',
             'doc': 'the DatasetBuilder to merge into this DatasetBuilder'})
    def deep_update(self, **kwargs):
        ''' Merge data and attributes from given DatasetBuilder into this DatasetBuilder '''
        dataset = getargs('dataset', kwargs)
        super().deep_update(dataset)
        if dataset.data:
            self['data'] = dataset.data  # TODO: figure out if we want to add a check for overwrite


class LinkBuilder(Builder):

    @docval({'name': 'builder', 'type': (DatasetBuilder, GroupBuilder), 'doc': 'the target of this link'},
            {'name': 'name', 'type': str, 'doc': 'the name of the dataset', 'default': None},
            {'name': 'parent', 'type': GroupBuilder, 'doc': 'the parent builder of this Builder', 'default': None},
            {'name': 'source', 'type': str, 'doc': 'the source of the data in this builder', 'default': None})
    def __init__(self, **kwargs):
        name, builder, parent, source = getargs('name', 'builder', 'parent', 'source', kwargs)
        if name is None:
            name = builder.name
        super().__init__(name, parent, source)
        self['builder'] = builder

    @property
    def builder(self):
        ''' The target builder object '''
        return self['builder']


class ReferenceBuilder(dict):
    # TODO why is this a dictionary?

    @docval({'name': 'builder', 'type': (DatasetBuilder, GroupBuilder), 'doc': 'the Dataset this reference applies to'})
    def __init__(self, **kwargs):
        builder = getargs('builder', kwargs)
        self['builder'] = builder

    @property
    def builder(self):
        ''' The target builder object '''
        return self['builder']


class RegionBuilder(ReferenceBuilder):

    @docval({'name': 'region', 'type': (slice, tuple, list, RegionReference),
             'doc': 'the region i.e. slice or indices into the target Dataset'},
            {'name': 'builder', 'type': DatasetBuilder, 'doc': 'the Dataset this region applies to'})
    def __init__(self, **kwargs):
        region, builder = popargs('region', 'builder', kwargs)
        super().__init__(builder)
        self['region'] = region

    @property
    def region(self):
        ''' The target builder object '''
        return self['region']


class CoordBuilder(namedtuple('CoordBuilder', 'name axes coord_dataset_name coord_axes coord_type')):
    '''
    An immutable object that represents a coordinate with fields name, axes, coord_dataset, coord_axes, coord_type.

    NOTE: 'axes' = 'dims_index'
    '''

    @docval({'name': 'name', 'type': str, 'doc': 'The name of this coordinate'},
            {'name': 'axes', 'type': (int, list, tuple),
             'doc': 'The axes (0-indexed) of the dataset that this coordinate acts on'},
            {'name': 'coord_dataset_name', 'type': str, 'doc': 'The name of the dataset of this coordinate'},
            {'name': 'coord_axes', 'type': (int, list, tuple),
             'doc': 'The axes (0-indexed) of the dataset of this coordinate'},
            {'name': 'coord_type', 'type': str, 'doc': 'The type of this coordinate'})
    def __new__(cls, **kwargs):
        # initialize a new CoordBuilder with argument documentation and validation
        # to override initialization of a namedtuple, need to override __new__, not __init__
        return super().__new__(cls, **kwargs)
