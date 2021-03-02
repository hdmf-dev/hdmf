from abc import ABCMeta, abstractmethod
from pathlib import Path

from ..build import BuildManager, GroupBuilder
from ..container import Container
from ..utils import docval, getargs, popargs


class HDMFIO(metaclass=ABCMeta):
    @docval({'name': 'manager', 'type': BuildManager,
             'doc': 'the BuildManager to use for I/O', 'default': None},
            {"name": "source", "type": (str, Path),
             "doc": "the source of container being built i.e. file path", 'default': None})
    def __init__(self, **kwargs):
        manager, source = getargs('manager', 'source', kwargs)
        if isinstance(source, Path):
            source = str(source)

        self.__manager = manager
        self.__built = dict()
        self.__source = source
        self.open()

    @property
    def manager(self):
        '''The BuildManager this instance is using'''
        return self.__manager

    @property
    def source(self):
        '''The source of the container being read/written i.e. file path'''
        return self.__source

    @docval(returns='the Container object that was read in', rtype=Container)
    def read(self, **kwargs):
        """Read a container from the IO source."""
        f_builder = self.read_builder()
        if all(len(v) == 0 for v in f_builder.values()):
            # TODO also check that the keys are appropriate. print a better error message
            raise UnsupportedOperation('Cannot build data. There are no values.')
        self.add_foreign(f_builder)
        container = self.__manager.construct(f_builder)
        return container

    @docval({'name': 'f_builder', 'type': Builder, 'doc': 'The builder to load foreign fields for'},
            {'name': 'cache', 'type': bool,
             'doc': 'cache resolved foreign field data for reuse', 'default': True},
            {'name': 'ignore_cache', 'type': bool,
             'doc': 'ignore cached foreign field data', 'default': False},
            returns='whether or not we read from the cache', rtype=bool)
    def add_foreign_fields(self, f_builder, cache=True, ignore_cache=False):
        """
        Add foreign fields to a builder
        """
        objects = self.get_object_map(f_builder)

        foreign_fields = None
        cached = True
        cache_used = True
        if not ignore_cache:
            # only load cached data if we are told to do so
            foreign_fields = self.load_cached_ff()
            cached = True

        if foreign_fields is None:
            # resolve foreign fields if there is nothing cached
            # or we have been told to ignore cached foreign fields
            cached = False
            foreign_fields = self.read_foreign_fields()
            cache_used = False

        # fill in the gaps the foreign fields create
        for ff in foreign_fields:
            foreign_builder = ff['builder']
            path = ff['path']
            parent_oid = ff['parent_object_id']
            parent = objects[parent_oid]
            if isinstance(foreign_builder, DatasetBuilder):
                parent.set_dataset(foreign_builder)
            elif isinstance(foreign_builder, GroupBuilder):
                parent.set_group(foreign_builder)
            else:
                # assume we are adding an attribute
                parent.set_attribute(path, foreign_builder)

        if not cached and cache:
            # only cached foreign field data if it has not
            # already been cached and we are told to do so
            self.cache_ff(foreign_fields)

        return cache_used

    def get_object_map(self, builder, index=None):
        """
        Build an index of object ID to builder object
        """
        if index is None:
            index = dict()

        # add subgroups
        for b in builder.groups:
            self.get_object_map(b, index=index)
        # add datasets
        for b in builder.datasets:
            self.get_object_map(b, index=index)

        # add this builder
        if isinstance(builder, GroupBuilder):
            id_key = self.__manager.namespace_catalog.group_spec_cls.id_key()
        elif isinstance(builder, DatasetBuilder):
            id_key = self.__manager.namespace_catalog.dataset_spec_cls.id_key()
        else:
            # do not index LinkBuilders right now
            # I'm not sure how/if these need to be handled
            return index

        # add this builder to index, only if it has an object ID
        oid = builder.attributes.get(id_key)
        if oid is not None:
            index[oid] = builder

        return index

    def cache_ff(self, foreign_fields):
        """ Cache resolved foreign fields """
        cache_loc = self._get_ff_cache_loc()
        with open(cache_loc, 'wb') as f:
            pickle.dump(foreign_fields, f, pickle.HIGHEST_PROTOCOL)

    def load_cached_ff(self):
        """ Load cached resolved foreign fields """
        cache_loc = self._get_ff_cache_loc()
        if not os.path.exists(cache_loc):
            return None
        with open(cache_loc, 'rb') as f:
            foreign_fields = pickle.load(f)
        return foreign_fields

    def _get_ff_cache_loc(self, ):
        """ Return a consistent location for this IO object to use for caching data"""
        # TODO: Fill this in
        pass

    @docval({'name': 'container', 'type': Container, 'doc': 'the Container object to write'},
            allow_extra=True)
    def write(self, **kwargs):
        """Write a container to the IO source."""
        container = popargs('container', kwargs)
        f_builder = self.__manager.build(container, source=self.__source, root=True)
        self.write_builder(f_builder, **kwargs)

    @docval({'name': 'src_io', 'type': 'HDMFIO', 'doc': 'the HDMFIO object for reading the data to export'},
            {'name': 'container', 'type': Container,
             'doc': ('the Container object to export. If None, then the entire contents of the HDMFIO object will be '
                     'exported'),
             'default': None},
            {'name': 'write_args', 'type': dict, 'doc': 'arguments to pass to :py:meth:`write_builder`',
             'default': dict()})
    def export(self, **kwargs):
        """Export from one backend to the backend represented by this class.

        If `container` is provided, then the build manager of `src_io` is used to build the container, and the resulting
        builder will be exported to the new backend. So if `container` is provided, `src_io` must have a non-None
        manager property. If `container` is None, then the contents of `src_io` will be read and exported to the new
        backend.

        The provided container must be the root of the hierarchy of the source used to read the container (i.e., you
        cannot read a file and export a part of that file.

        Arguments can be passed in for the `write_builder` method using `write_args`. Some arguments may not be
        supported during export.

        Example usage:

        .. code-block:: python

            old_io = HDF5IO('old.nwb', 'r')
            with HDF5IO('new_copy.nwb', 'w') as new_io:
                new_io.export(old_io)
        """
        src_io, container, write_args = getargs('src_io', 'container', 'write_args', kwargs)
        if container is not None:
            # check that manager exists, container was built from manager, and container is root of hierarchy
            if src_io.manager is None:
                raise ValueError('When a container is provided, src_io must have a non-None manager (BuildManager) '
                                 'property.')
            old_bldr = src_io.manager.get_builder(container)
            if old_bldr is None:
                raise ValueError('The provided container must have been read by the provided src_io.')
            if old_bldr.parent is not None:
                raise ValueError('The provided container must be the root of the hierarchy of the '
                                 'source used to read the container.')

            # build any modified containers
            src_io.manager.purge_outdated()
            bldr = src_io.manager.build(container, source=self.__source, root=True, export=True)
        else:
            bldr = src_io.read_builder()
        self.write_builder(builder=bldr, **write_args)

    @abstractmethod
    @docval(returns='a GroupBuilder representing the read data', rtype='GroupBuilder')
    def read_builder(self):
        ''' Read data and return the GroupBuilder representing it '''
        pass

    @abstractmethod
    @docval(returns='a list of dictionaries containing foreign field data', rtype=list)
    def read_foreign_fields(self):
        ''' Read and return foreign field data

        This method should return a list of dictionaries. Each dictionary should
        contain the following keys:

            value:                the value of the foreign field
            parent_object_id:     the object ID for the parent this foreign field belongs to
            path:                 the path of the foreign field relative to the parent object
        '''

        pass

    @abstractmethod
    @docval({'name': 'builder', 'type': GroupBuilder, 'doc': 'the GroupBuilder object representing the Container'},
            allow_extra=True)
    def write_builder(self, **kwargs):
        ''' Write a GroupBuilder representing an Container object '''
        pass

    @abstractmethod
    def open(self):
        ''' Open this HDMFIO object for writing of the builder '''
        pass

    @abstractmethod
    def close(self):
        ''' Close this HDMFIO object to further reading/writing'''
        pass

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


class UnsupportedOperation(ValueError):
    pass
