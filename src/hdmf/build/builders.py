import copy as _copy
import itertools as _itertools
import posixpath as _posixpath
from abc import ABCMeta
from collections.abc import Iterable
from datetime import datetime

import numpy as np
from h5py import RegionReference

from ..utils import docval, getargs, get_docval


class Builder(dict, metaclass=ABCMeta):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of the group'},
            {'name': 'parent', 'type': 'Builder', 'doc': 'the parent builder of this Builder', 'default': None},
            {'name': 'source', 'type': str,
             'doc': 'the source of the data in this builder e.g. file name', 'default': None})
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

    @property
    def path(self):
        """The path of this builder."""
        s = list()
        c = self
        while c is not None:
            s.append(c.name)
            c = c.parent
        return "/".join(s[::-1])

    @property
    def name(self):
        """The name of this builder."""
        return self.__name

    @property
    def source(self):
        """The source of this builder."""
        return self.__source

    @source.setter
    def source(self, s):
        if self.__source is not None:
            raise AttributeError('Cannot overwrite source.')
        self.__source = s

    @property
    def parent(self):
        """The parent builder of this builder."""
        return self.__parent

    @parent.setter
    def parent(self, p):
        if self.__parent is not None:
            raise AttributeError('Cannot overwrite parent.')
        self.__parent = p
        if self.__source is None:
            self.source = p.source

    def __repr__(self):
        ret = "%s %s %s" % (self.path, self.__class__.__name__, super().__repr__())
        return ret


class BaseBuilder(Builder, metaclass=ABCMeta):
    __attribute = 'attributes'  # self dictionary key for attributes

    @docval({'name': 'name', 'type': str, 'doc': 'The name of the builder.'},
            {'name': 'attributes', 'type': dict, 'doc': 'A dictionary of attributes to create in this builder.',
             'default': dict()},
            {'name': 'parent', 'type': 'GroupBuilder', 'doc': 'The parent builder of this builder.', 'default': None},
            {'name': 'source', 'type': str,
             'doc': 'The source of the data represented in this builder', 'default': None})
    def __init__(self, **kwargs):
        name, attributes, parent, source = getargs('name', 'attributes', 'parent', 'source', kwargs)
        super().__init__(name, parent, source)
        super().__setitem__(BaseBuilder.__attribute, dict())
        for name, val in attributes.items():
            self.set_attribute(name, val)
        self.__location = None

    @property
    def location(self):
        """The location of this Builder in its source."""
        return self.__location

    @location.setter
    def location(self, val):
        self.__location = val

    @property
    def attributes(self):
        """The attributes stored in this Builder object."""
        return super().__getitem__(BaseBuilder.__attribute)

    @docval({'name': 'name', 'type': str, 'doc': 'The name of the attribute.'},
            {'name': 'value', 'type': None, 'doc': 'The attribute value.'})
    def set_attribute(self, **kwargs):
        """Set an attribute for this group."""
        name, value = getargs('name', 'value', kwargs)
        self.attributes[name] = value


class GroupBuilder(BaseBuilder):
    # sub-dictionary keys. subgroups go in super().__getitem__(GroupBuilder.__group)
    __group = 'groups'
    __dataset = 'datasets'
    __link = 'links'
    __attribute = 'attributes'

    @docval({'name': 'name', 'type': str, 'doc': 'The name of the group.'},
            {'name': 'groups', 'type': (dict, list),
             'doc': ('A dictionary or list of subgroups to add to this group. If a dict is provided, only the '
                     'values are used.'),
             'default': dict()},
            {'name': 'datasets', 'type': (dict, list),
             'doc': ('A dictionary or list of datasets to add to this group. If a dict is provided, only the '
                     'values are used.'),
             'default': dict()},
            {'name': 'attributes', 'type': dict, 'doc': 'A dictionary of attributes to create in this group.',
             'default': dict()},
            {'name': 'links', 'type': (dict, list),
             'doc': ('A dictionary or list of links to add to this group. If a dict is provided, only the '
                     'values are used.'),
             'default': dict()},
            {'name': 'parent', 'type': 'GroupBuilder', 'doc': 'The parent builder of this builder.', 'default': None},
            {'name': 'source', 'type': str,
             'doc': 'The source of the data represented in this builder.', 'default': None})
    def __init__(self, **kwargs):
        """Create a builder object for a group."""
        name, groups, datasets, links, attributes, parent, source = getargs(
            'name', 'groups', 'datasets', 'links', 'attributes', 'parent', 'source', kwargs)
        # NOTE: if groups, datasets, or links are dicts, their keys are unused
        groups = self.__to_list(groups)
        datasets = self.__to_list(datasets)
        links = self.__to_list(links)
        # dictionary mapping subgroup/dataset/attribute/link name to the key that maps to the
        # subgroup/dataset/attribute/link sub-dictionary that maps the name to the builder
        self.obj_type = dict()
        super().__init__(name, attributes, parent, source)
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
        """Recursively set all subgroups/datasets/links source when this source is set."""
        super(GroupBuilder, self.__class__).source.fset(self, s)
        for group in self.groups.values():
            if group.source is None:
                group.source = s
        for dset in self.datasets.values():
            if dset.source is None:
                dset.source = s
        for link in self.links.values():
            if link.source is None:
                link.source = s

    @property
    def groups(self):
        """The subgroups contained in this group."""
        return super().__getitem__(GroupBuilder.__group)

    @property
    def datasets(self):
        """The datasets contained in this group."""
        return super().__getitem__(GroupBuilder.__dataset)

    @property
    def links(self):
        """The links contained in this group."""
        return super().__getitem__(GroupBuilder.__link)

    @docval(*get_docval(BaseBuilder.set_attribute))
    def set_attribute(self, **kwargs):
        """Set an attribute for this group."""
        name, value = getargs('name', 'value', kwargs)
        self.__check_obj_type(name, GroupBuilder.__attribute)
        super().set_attribute(name, value)
        self.obj_type[name] = GroupBuilder.__attribute

    def __check_obj_type(self, name, obj_type):
        # check that the name is not associated with a different object type in this group
        if name in self.obj_type and self.obj_type[name] != obj_type:
            raise ValueError("'%s' already exists in %s.%s, cannot set in %s."
                             % (name, self.name, self.obj_type[name], obj_type))

    @docval({'name': 'builder', 'type': 'GroupBuilder', 'doc': 'The GroupBuilder to add to this group.'})
    def set_group(self, **kwargs):
        """Add a subgroup to this group."""
        builder = getargs('builder', kwargs)
        self.__set_builder(builder, GroupBuilder.__group)

    @docval({'name': 'builder', 'type': 'DatasetBuilder', 'doc': 'The DatasetBuilder to add to this group.'})
    def set_dataset(self, **kwargs):
        """Add a dataset to this group."""
        builder = getargs('builder', kwargs)
        self.__set_builder(builder, GroupBuilder.__dataset)

    @docval({'name': 'builder', 'type': 'LinkBuilder', 'doc': 'The LinkBuilder to add to this group.'})
    def set_link(self, **kwargs):
        """Add a link to this group."""
        builder = getargs('builder', kwargs)
        self.__set_builder(builder, GroupBuilder.__link)

    def __set_builder(self, builder, obj_type):
        name = builder.name
        self.__check_obj_type(name, obj_type)
        super().__getitem__(obj_type)[name] = builder
        self.obj_type[name] = obj_type
        if builder.parent is None:
            builder.parent = self

    def is_empty(self):
        """Returns true if there are no datasets, links, attributes, and non-empty subgroups. False otherwise."""
        if len(self.datasets) or len(self.links) or len(self.attributes):
            return False
        elif len(self.groups):
            return all(g.is_empty() for g in self.groups.values())
        else:
            return True

    def __getitem__(self, key):
        """Like dict.__getitem__, but looks in groups, datasets, attributes, and links sub-dictionaries.
        Key can be a posix path to a sub-builder.
        """
        try:
            key_ar = _posixpath.normpath(key).split('/')
            return self.__get_rec(key_ar)
        except KeyError:
            raise KeyError(key)

    def get(self, key, default=None):
        """Like dict.get, but looks in groups, datasets, attributes, and links sub-dictionaries.
        Key can be a posix path to a sub-builder.
        """
        try:
            key_ar = _posixpath.normpath(key).split('/')
            return self.__get_rec(key_ar)
        except KeyError:
            return default

    def __get_rec(self, key_ar):
        # recursive helper for __getitem__ and get
        if len(key_ar) == 1:
            # get the correct dictionary (groups, datasets, links, attributes) associated with the key
            # then look up the key within that dictionary to get the builder
            return super().__getitem__(self.obj_type[key_ar[0]])[key_ar[0]]
        else:
            if key_ar[0] in self.groups:
                return self.groups[key_ar[0]].__get_rec(key_ar[1:])
        raise KeyError(key_ar[0])

    def __setitem__(self, args, val):
        raise NotImplementedError('__setitem__')

    def __contains__(self, item):
        return self.obj_type.__contains__(item)

    def items(self):
        """Like dict.items, but iterates over items in groups, datasets, attributes, and links sub-dictionaries."""
        return _itertools.chain(self.groups.items(),
                                self.datasets.items(),
                                self.attributes.items(),
                                self.links.items())

    def keys(self):
        """Like dict.keys, but iterates over keys in groups, datasets, attributes, and links sub-dictionaries."""
        return _itertools.chain(self.groups.keys(),
                                self.datasets.keys(),
                                self.attributes.keys(),
                                self.links.keys())

    def values(self):
        """Like dict.values, but iterates over values in groups, datasets, attributes, and links sub-dictionaries."""
        return _itertools.chain(self.groups.values(),
                                self.datasets.values(),
                                self.attributes.values(),
                                self.links.values())


class DatasetBuilder(BaseBuilder):
    OBJECT_REF_TYPE = 'object'
    REGION_REF_TYPE = 'region'

    @docval({'name': 'name', 'type': str, 'doc': 'The name of the dataset.'},
            {'name': 'data',
             'type': ('array_data', 'scalar_data', 'data', 'DatasetBuilder', 'RegionBuilder', Iterable, datetime),
             'doc': 'The data in this dataset.', 'default': None},
            {'name': 'dtype', 'type': (type, np.dtype, str, list),
             'doc': 'The datatype of this dataset.', 'default': None},
            {'name': 'attributes', 'type': dict,
             'doc': 'A dictionary of attributes to create in this dataset.', 'default': dict()},
            {'name': 'maxshape', 'type': (int, tuple),
             'doc': 'The shape of this dataset. Use None for scalars.', 'default': None},
            {'name': 'chunks', 'type': bool, 'doc': 'Whether or not to chunk this dataset.', 'default': False},
            {'name': 'parent', 'type': GroupBuilder, 'doc': 'The parent builder of this builder.', 'default': None},
            {'name': 'source', 'type': str, 'doc': 'The source of the data in this builder.', 'default': None})
    def __init__(self, **kwargs):
        """ Create a Builder object for a dataset """
        name, data, dtype, attributes, maxshape, chunks, parent, source = getargs(
            'name', 'data', 'dtype', 'attributes', 'maxshape', 'chunks', 'parent', 'source', kwargs)
        super().__init__(name, attributes, parent, source)
        self['data'] = data
        self['attributes'] = _copy.copy(attributes)
        self.__chunks = chunks
        self.__maxshape = maxshape
        if isinstance(data, BaseBuilder):
            if dtype is None:
                dtype = self.OBJECT_REF_TYPE
        self.__dtype = dtype
        self.__name = name

    @property
    def data(self):
        """The data stored in the dataset represented by this builder."""
        return self['data']

    @data.setter
    def data(self, val):
        if self['data'] is not None:
            raise AttributeError("Cannot overwrite data.")
        self['data'] = val

    @property
    def chunks(self):
        """Whether or not this dataset is chunked."""
        return self.__chunks

    @property
    def maxshape(self):
        """The max shape of this dataset."""
        return self.__maxshape

    @property
    def dtype(self):
        """The data type of this dataset."""
        return self.__dtype

    @dtype.setter
    def dtype(self, val):
        if self.__dtype is not None:
            raise AttributeError("Cannot overwrite dtype.")
        self.__dtype = val


class LinkBuilder(Builder):

    @docval({'name': 'builder', 'type': (DatasetBuilder, GroupBuilder),
             'doc': 'The target group or dataset of this link.'},
            {'name': 'name', 'type': str, 'doc': 'The name of the link', 'default': None},
            {'name': 'parent', 'type': GroupBuilder, 'doc': 'The parent builder of this builder', 'default': None},
            {'name': 'source', 'type': str, 'doc': 'The source of the data in this builder', 'default': None})
    def __init__(self, **kwargs):
        """Create a builder object for a link."""
        name, builder, parent, source = getargs('name', 'builder', 'parent', 'source', kwargs)
        if name is None:
            name = builder.name
        super().__init__(name, parent, source)
        self['builder'] = builder

    @property
    def builder(self):
        """The target builder object."""
        return self['builder']


class ReferenceBuilder(dict):

    @docval({'name': 'builder', 'type': (DatasetBuilder, GroupBuilder),
             'doc': 'The group or dataset this reference applies to.'})
    def __init__(self, **kwargs):
        """Create a builder object for a reference."""
        builder = getargs('builder', kwargs)
        self['builder'] = builder

    @property
    def builder(self):
        """The target builder object."""
        return self['builder']


class RegionBuilder(ReferenceBuilder):

    @docval({'name': 'region', 'type': (slice, tuple, list, RegionReference),
             'doc': 'The region, i.e. slice or indices, into the target dataset.'},
            {'name': 'builder', 'type': DatasetBuilder, 'doc': 'The dataset this region reference applies to.'})
    def __init__(self, **kwargs):
        """Create a builder object for a region reference."""
        region, builder = getargs('region', 'builder', kwargs)
        super().__init__(builder)
        self['region'] = region

    @property
    def region(self):
        """The selected region of the target dataset."""
        return self['region']
