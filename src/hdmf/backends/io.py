from abc import ABCMeta, abstractmethod

from ..build import BuildManager, GroupBuilder
from ..utils import docval, getargs, popargs
from ..container import Container


class HDMFIO(metaclass=ABCMeta):
    @docval({'name': 'manager', 'type': BuildManager,
             'doc': 'the BuildManager to use for I/O', 'default': None},
            {"name": "source", "type": str,
             "doc": "the source of container being built i.e. file path", 'default': None})
    def __init__(self, **kwargs):
        self.__manager = getargs('manager', kwargs)
        self.__built = dict()
        self.__source = getargs('source', kwargs)
        self.open()

    @property
    def manager(self):
        '''The BuildManager this HDMFIO is using'''
        return self.__manager

    @property
    def source(self):
        '''The source of the container being read/written i.e. file path'''
        return self.__source

    @property
    def type_map(self):
        '''The TypeMap this HDMFIO is using (from the BuildManager). Returns None if there is no BuildManager'''
        if self.manager is None:
            return None
        else:
            return self.manager.type_map

    @docval(returns='the Container object that was read in', rtype=Container)
    def read(self, **kwargs):
        f_builder = self.read_builder()
        if all(len(v) == 0 for v in f_builder.values()):
            # TODO also check that the keys are appropriate. print a better error message
            raise UnsupportedOperation('Cannot build data. There are no values.')
        container = self.__manager.construct(f_builder)
        return container

    @docval({'name': 'container', 'type': Container, 'doc': 'the Container object to write'},
            {'name': 'exhaust_dci', 'type': bool,
             'doc': 'exhaust DataChunkIterators one at a time. If False, exhaust them concurrently', 'default': True},
            allow_extra=True)
    def write(self, **kwargs):
        container = popargs('container', kwargs)
        f_builder = self.__manager.build(container, source=self.__source)
        self.write_builder(f_builder, **kwargs)

    @classmethod
    @docval({'name': 'container', 'type': Container, 'doc': 'the Container object to export'},
            {'name': 'type_map', 'type': 'TypeMap', 'doc': 'the TypeMap to use to export'},
            {'name': 'source', 'type': str, 'doc': 'the source of container being built i.e. file path'},
            {'name': 'write_args', 'type': dict, 'doc': 'dictionary of arguments to use when writing to file',
             'default': dict()},
            allow_extra=True)
    def export(cls, **kwargs):
        ''' Export the given container using this IO object initialized with the given arguments '''
        container, type_map, source, read_args, write_args = popargs('container', 'type_map', 'source', 'read_args',
                                                                     'write_args', kwargs)
        temp_manager = BuildManager(type_map, export=True)
        write_io = cls(manager=temp_manager, source=source)
        write_io.write(container, **write_args)

    @classmethod
    @docval({'name': 'io', 'type': 'HDMFIO', 'doc': 'the HDMFIO object to read data from'},
            {'name': 'source', 'type': str, 'doc': 'the source of container being built i.e. file path'},
            {'name': 'read_args', 'type': dict, 'doc': 'dictionary of arguments to use when reading from read_io',
             'default': dict()},
            {'name': 'write_args', 'type': dict, 'doc': 'dictionary of arguments to use when writing to file',
             'default': dict()},
            allow_extra=True)
    def export_io(cls, **kwargs):
        ''' Export data from the given IO object using this IO object initialized with the given arguments '''
        io, source, read_args, write_args = popargs('io', 'source', 'read_args', 'write_args', kwargs)
        container = io.read(**read_args)
        cls.export(container=container, type_map=io.type_map, source=source, write_args=write_args)

    @abstractmethod
    @docval(returns='a GroupBuilder representing the read data', rtype='GroupBuilder')
    def read_builder(self):
        ''' Read data and return the GroupBuilder representing '''
        pass

    @abstractmethod
    @docval({'name': 'builder', 'type': GroupBuilder, 'doc': 'the GroupBuilder object representing the Container'},
            {'name': 'exhaust_dci', 'type': bool,
             'doc': 'exhaust DataChunkIterators one at a time. If False, exhaust them concurrently', 'default': True})
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
