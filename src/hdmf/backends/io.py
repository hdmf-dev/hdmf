from abc import ABCMeta, abstractmethod
import os
from pathlib import Path
from typing import Union, Optional

from ..build import BuildManager, GroupBuilder, TypeMap
from ..container import Container, HERDManager
from .errors import UnsupportedOperation
from ..utils import docval, getargs, popargs, get_basic_array_info, generate_array_html_repr
from ..spec import  NamespaceCatalog
from warnings import warn


class HDMFIO(metaclass=ABCMeta):

    @staticmethod
    @abstractmethod
    def can_read(path):
        """Determines whether a given path is readable by this HDMFIO class"""
        pass

    @docval({'name': 'manager', 'type': BuildManager,
             'doc': 'the BuildManager to use for I/O', 'default': None},
            {"name": "source", "type": (str, Path),
             "doc": "the source of container being built i.e. file path", 'default': None},
            {'name': 'herd_path', 'type': str,
             'doc': 'The path to read/write the HERD file', 'default': None},)
    def __init__(self, **kwargs):
        manager, source, herd_path = getargs('manager', 'source', 'herd_path', kwargs)
        if isinstance(source, Path):
            source = source.resolve()
        elif (isinstance(source, str) and
              not (source.lower().startswith("http://") or
                   source.lower().startswith("https://") or
                   source.lower().startswith("s3://"))):
            source = os.path.abspath(source)

        self.__manager = manager
        self.__built = dict()
        self.__source = source
        self.herd_path = herd_path
        self.herd = None
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
        container.read_io = self
        if self.herd_path is not None:
            from hdmf.common import HERD
            try:
                self.herd = HERD.from_zip(path=self.herd_path)
                if isinstance(container, HERDManager):
                    container.link_resources(herd=self.herd)
            except FileNotFoundError:
                msg = "File not found at {}. HERD not added.".format(self.herd_path)
                warn(msg)
            except ValueError:
                msg = "Check HERD separately for alterations. HERD not added."
                warn(msg)

        return container

    @docval({'name': 'container', 'type': Container, 'doc': 'the Container object to write'},
            {'name': 'herd', 'type': 'hdmf.common.resources.HERD',
             'doc': 'A HERD object to populate with references.',
             'default': None}, allow_extra=True)
    def write(self, **kwargs):
        container = popargs('container', kwargs)
        herd = popargs('herd', kwargs)

        """Optional: Write HERD."""
        if self.herd_path is not None:
            # If HERD is not provided, create a new one, else extend existing one
            if herd is None:
                from hdmf.common import HERD
                herd = HERD(type_map=self.manager.type_map)

            # add_ref_container to search for and resolve the TermSetWrapper
            herd.add_ref_container(container) # container would be the NWBFile
            # write HERD
            herd.to_zip(path=self.herd_path)

        """Write a container to the IO source."""
        f_builder = self.__manager.build(container, source=self.__source, root=True)
        self.write_builder(f_builder, **kwargs)

    @docval({'name': 'src_io', 'type': 'hdmf.backends.io.HDMFIO',
             'doc': 'the HDMFIO object for reading the data to export'},
            {'name': 'container', 'type': Container,
             'doc': ('the Container object to export. If None, then the entire contents of the HDMFIO object will be '
                     'exported'),
             'default': None},
            {'name': 'write_args', 'type': dict, 'doc': 'arguments to pass to :py:meth:`write_builder`',
             'default': dict()},
            {'name': 'clear_cache', 'type': bool, 'doc': 'whether to clear the build manager cache',
             'default': False})
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

        NOTE: When implementing export support on custom backends. Export does not update the Builder.source
              on the Builders. As such, when writing LinkBuilders we need to determine if LinkBuilder.source
              and LinkBuilder.builder.source are the same, and if so the link should be internal to the
              current file (even if the Builder.source points to a different location).
        """
        src_io, container, write_args, clear_cache = getargs('src_io', 'container', 'write_args', 'clear_cache', kwargs)
        if container is None and clear_cache:
            # clear all containers and builders from cache so that they can all get rebuilt with export=True.
            # constructing the container is not efficient but there is no elegant way to trigger a
            # rebuild of src_io with new source.
            container = src_io.read()
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

            # NOTE in HDF5IO, clear_cache is set to True when link_data is False
            if clear_cache:
                # clear all containers and builders from cache so that they can all get rebuilt with export=True
                src_io.manager.clear_cache()
            else:
                # clear only cached containers and builders where the container was modified
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

    @classmethod
    @abstractmethod
    def load_namespaces(cls,
        namespace_catalog: Union[NamespaceCatalog, TypeMap],
        path: Optional[Union[str, Path]] = None,
        namespaces: Optional[list[str]] = None,
        io: Optional['HDMFIO'] = None,
        **kwargs
    ) -> dict:
        """Load the namespaces from the file at the given path into the provided NamespaceCatalog or TypeMap.

        This method should be implemented by subclasses to load the namespaces that are relevant for the backend.

        :param namespace_catalog: The NamespaceCatalog (or TypeMap) to load the namespaces into.
        :param path: The path to the file from which to load the namespaces.
        :param namespaces: A list of namespace names to load. If None, all namespaces will be loaded.
        :param kwargs: Additional keyword arguments that may be needed for the specific backend.
        :return: A dictionary mapping namespace names to their dependencies.
        """
        pass

    @abstractmethod
    def load_namespaces_io(self,
        namespace_catalog: Union[NamespaceCatalog, TypeMap],
        namespaces: Optional[list[str]] = None,
    ) -> dict:
        """Load the namespaces from this HDMFIO object into the provided NamespaceCatalog or TypeMap.

        Similar to `load_namespaces`, but uses the already opened HDMFIO object.
        This method should be implemented by subclasses to load the namespaces that are relevant for the backend.

        :param namespace_catalog: The NamespaceCatalog (or TypeMap) to load the namespaces into.
        :param namespaces: A list of namespace names to load. If None, all namespaces will be loaded.
        :return: A dictionary mapping namespace names to their dependencies.
        """
        # NOTE: this function is separated from load_namespaces for developer clarity
        pass

    @staticmethod
    def generate_dataset_html(dataset):
        """Generates an html representation for a dataset"""
        array_info_dict = get_basic_array_info(dataset)
        repr_html = generate_array_html_repr(array_info_dict, dataset)

        return repr_html

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def __del__(self):
        self.close()
