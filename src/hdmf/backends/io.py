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
        container = self.__manager.construct(f_builder)
        return container

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
