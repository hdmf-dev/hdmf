import logging
import os.path
import warnings
from collections import deque
from functools import partial
from pathlib import Path

import numpy as np
from h5py import File, Group, Dataset, special_dtype, SoftLink, ExternalLink, Reference, RegionReference, check_dtype

from .h5_utils import (BuilderH5ReferenceDataset, BuilderH5RegionDataset, BuilderH5TableDataset, H5DataIO,
                       H5SpecReader, H5SpecWriter)
from ..io import HDMFIO, UnsupportedOperation
from ..warnings import BrokenLinkWarning
from ...build import (Builder, GroupBuilder, DatasetBuilder, LinkBuilder, BuildManager, RegionBuilder,
                      ReferenceBuilder, TypeMap, ObjectMapper)
from ...container import Container
from ...data_utils import AbstractDataChunkIterator
from ...spec import RefSpec, DtypeSpec, NamespaceCatalog, GroupSpec, NamespaceBuilder
from ...utils import docval, getargs, popargs, call_docval_func, get_data_shape, fmt_docval_args, get_docval

ROOT_NAME = 'root'
SPEC_LOC_ATTR = '.specloc'
H5_TEXT = special_dtype(vlen=str)
H5_BINARY = special_dtype(vlen=bytes)
H5_REF = special_dtype(ref=Reference)
H5_REGREF = special_dtype(ref=RegionReference)


class HDF5IO(HDMFIO):

    __ns_spec_path = 'namespace'  # path to the namespace dataset within a namespace group

    @docval({'name': 'path', 'type': (str, Path), 'doc': 'the path to the HDF5 file'},
            {'name': 'manager', 'type': (TypeMap, BuildManager),
             'doc': 'the BuildManager or a TypeMap to construct a BuildManager to use for I/O', 'default': None},
            {'name': 'mode', 'type': str,
             'doc': ('the mode to open the HDF5 file with, one of ("w", "r", "r+", "a", "w-", "x"). '
                     'See `h5py.File <http://docs.h5py.org/en/latest/high/file.html#opening-creating-files>`_ for '
                     'more details.')},
            {'name': 'comm', 'type': 'Intracomm',
             'doc': 'the MPI communicator to use for parallel I/O', 'default': None},
            {'name': 'file', 'type': File, 'doc': 'a pre-existing h5py.File object', 'default': None},
            {'name': 'driver', 'type': str, 'doc': 'driver for h5py to use when opening HDF5 file', 'default': None})
    def __init__(self, **kwargs):
        """Open an HDF5 file for IO.
        """
        self.logger = logging.getLogger('%s.%s' % (self.__class__.__module__, self.__class__.__qualname__))
        path, manager, mode, comm, file_obj, driver = popargs('path', 'manager', 'mode', 'comm', 'file', 'driver',
                                                              kwargs)

        if isinstance(path, Path):
            path = str(path)

        if file_obj is not None and os.path.abspath(file_obj.filename) != os.path.abspath(path):
            msg = 'You argued %s as this object\'s path, ' % path
            msg += 'but supplied a file with filename: %s' % file_obj.filename
            raise ValueError(msg)

        if file_obj is None and not os.path.exists(path) and (mode == 'r' or mode == 'r+') and driver != 'ros3':
            msg = "Unable to open file %s in '%s' mode. File does not exist." % (path, mode)
            raise UnsupportedOperation(msg)

        if file_obj is None and os.path.exists(path) and (mode == 'w-' or mode == 'x'):
            msg = "Unable to open file %s in '%s' mode. File already exists." % (path, mode)
            raise UnsupportedOperation(msg)

        if manager is None:
            manager = BuildManager(TypeMap(NamespaceCatalog()))
        elif isinstance(manager, TypeMap):
            manager = BuildManager(manager)
        self.__driver = driver
        self.__comm = comm
        self.__mode = mode
        self.__file = file_obj
        super().__init__(manager, source=path)
        self.__built = dict()       # keep track of each builder for each dataset/group/link for each file
        self.__read = dict()        # keep track of which files have been read. Key is the filename value is the builder
        self.__ref_queue = deque()  # a queue of the references that need to be added
        self.__dci_queue = deque()  # a queue of DataChunkIterators that need to be exhausted
        ObjectMapper.no_convert(Dataset)
        self._written_builders = dict()  # keep track of which builders were written (or read) by this IO object
        self.__open_links = []      # keep track of other files opened from links in this file

    @property
    def comm(self):
        """The MPI communicator to use for parallel I/O."""
        return self.__comm

    @property
    def _file(self):
        return self.__file

    @property
    def driver(self):
        return self.__driver

    @staticmethod
    def __resolve_file_obj(path, file_obj, driver):
        if isinstance(path, Path):
            path = str(path)

        if path is None and file_obj is None:
            raise ValueError("Either the 'path' or 'file' argument must be supplied.")

        if path is not None and file_obj is not None:  # consistency check
            if os.path.abspath(file_obj.filename) != os.path.abspath(path):
                msg = ("You argued '%s' as this object's path, but supplied a file with filename: %s"
                       % (path, file_obj.filename))
                raise ValueError(msg)

        if file_obj is None:
            file_kwargs = dict()
            if driver is not None:
                file_kwargs.update(driver=driver)
            file_obj = File(path, 'r', **file_kwargs)
        return file_obj

    @classmethod
    @docval({'name': 'namespace_catalog', 'type': (NamespaceCatalog, TypeMap),
             'doc': 'the NamespaceCatalog or TypeMap to load namespaces into'},
            {'name': 'path', 'type': (str, Path), 'doc': 'the path to the HDF5 file', 'default': None},
            {'name': 'namespaces', 'type': list, 'doc': 'the namespaces to load', 'default': None},
            {'name': 'file', 'type': File, 'doc': 'a pre-existing h5py.File object', 'default': None},
            {'name': 'driver', 'type': str, 'doc': 'driver for h5py to use when opening HDF5 file', 'default': None},
            returns=("dict mapping the names of the loaded namespaces to a dict mapping included namespace names and "
                     "the included data types"),
            rtype=dict)
    def load_namespaces(cls, **kwargs):
        """Load cached namespaces from a file.

        If `file` is not supplied, then an :py:class:`h5py.File` object will be opened for the given `path`, the
        namespaces will be read, and the File object will be closed. If `file` is supplied, then
        the given File object will be read from and not closed.

        :raises ValueError: if both `path` and `file` are supplied but `path` is not the same as the path of `file`.
        """
        namespace_catalog, path, namespaces, file_obj, driver = popargs(
            'namespace_catalog', 'path', 'namespaces', 'file', 'driver', kwargs)

        open_file_obj = cls.__resolve_file_obj(path, file_obj, driver)
        if file_obj is None:  # need to close the file object that we just opened
            with open_file_obj:
                return cls.__load_namespaces(namespace_catalog, namespaces, open_file_obj)
        return cls.__load_namespaces(namespace_catalog, namespaces, open_file_obj)

    @classmethod
    def __load_namespaces(cls, namespace_catalog, namespaces, file_obj):
        d = {}

        if not cls.__check_specloc(file_obj):
            return d

        namespace_versions = cls.__get_namespaces(file_obj)

        spec_group = file_obj[file_obj.attrs[SPEC_LOC_ATTR]]
        if namespaces is None:
            namespaces = list(spec_group.keys())

        readers = dict()
        deps = dict()
        for ns in namespaces:
            latest_version = namespace_versions[ns]
            ns_group = spec_group[ns][latest_version]
            reader = H5SpecReader(ns_group)
            readers[ns] = reader
            # for each namespace in the 'namespace' dataset, track all included namespaces (dependencies)
            for spec_ns in reader.read_namespace(cls.__ns_spec_path):
                deps[ns] = list()
                for s in spec_ns['schema']:
                    dep = s.get('namespace')
                    if dep is not None:
                        deps[ns].append(dep)

        order = cls._order_deps(deps)
        for ns in order:
            reader = readers[ns]
            d.update(namespace_catalog.load_namespaces(cls.__ns_spec_path, reader=reader))

        return d

    @classmethod
    def __check_specloc(cls, file_obj):
        if SPEC_LOC_ATTR not in file_obj.attrs:
            # this occurs in legacy files
            msg = "No cached namespaces found in %s" % file_obj.filename
            warnings.warn(msg)
            return False
        return True

    @classmethod
    @docval({'name': 'path', 'type': (str, Path), 'doc': 'the path to the HDF5 file', 'default': None},
            {'name': 'file', 'type': File, 'doc': 'a pre-existing h5py.File object', 'default': None},
            {'name': 'driver', 'type': str, 'doc': 'driver for h5py to use when opening HDF5 file', 'default': None},
            returns="dict mapping names to versions of the namespaces in the file", rtype=dict)
    def get_namespaces(cls, **kwargs):
        """Get the names and versions of the cached namespaces from a file.

        If `file` is not supplied, then an :py:class:`h5py.File` object will be opened for the given `path`, the
        namespaces will be read, and the File object will be closed. If `file` is supplied, then
        the given File object will be read from and not closed.

        If there are multiple versions of a namespace cached in the file, then only the latest one (using alphanumeric
        ordering) is returned. This is the version of the namespace that is loaded by HDF5IO.load_namespaces(...).

        :raises ValueError: if both `path` and `file` are supplied but `path` is not the same as the path of `file`.
        """
        path, file_obj, driver = popargs('path', 'file', 'driver', kwargs)

        open_file_obj = cls.__resolve_file_obj(path, file_obj, driver)
        if file_obj is None:  # need to close the file object that we just opened
            with open_file_obj:
                return cls.__get_namespaces(open_file_obj)
        return cls.__get_namespaces(open_file_obj)

    @classmethod
    def __get_namespaces(cls, file_obj):
        """Return a dict mapping namespace name to version string for the latest version of that namespace in the file.

        If there are multiple versions of a namespace cached in the file, then only the latest one (using alphanumeric
        ordering) is returned. This is the version of the namespace that is loaded by HDF5IO.load_namespaces(...).
        """
        used_version_names = dict()
        if not cls.__check_specloc(file_obj):
            return used_version_names

        spec_group = file_obj[file_obj.attrs[SPEC_LOC_ATTR]]
        namespaces = list(spec_group.keys())
        for ns in namespaces:
            ns_group = spec_group[ns]
            # NOTE: by default, objects within groups are iterated in alphanumeric order
            version_names = list(ns_group.keys())
            if len(version_names) > 1:
                # prior to HDMF 1.6.1, extensions without a version were written under the group name "unversioned"
                # make sure that if there is another group representing a newer version, that is read instead
                if 'unversioned' in version_names:
                    version_names.remove('unversioned')
            if len(version_names) > 1:
                # as of HDMF 1.6.1, extensions without a version are written under the group name "None"
                # make sure that if there is another group representing a newer version, that is read instead
                if 'None' in version_names:
                    version_names.remove('None')
            used_version_names[ns] = version_names[-1]  # save the largest in alphanumeric order

        return used_version_names

    @classmethod
    def _order_deps(cls, deps):
        """
        Order namespaces according to dependency for loading into a NamespaceCatalog

        Args:
            deps (dict): a dictionary that maps a namespace name to a list of name of
                         the namespaces on which the namespace is directly dependent
                         Example: {'a': ['b', 'c'], 'b': ['d'], 'c': ['d'], 'd': []}
                         Expected output: ['d', 'b', 'c', 'a']
        """
        order = list()
        keys = list(deps.keys())
        deps = dict(deps)
        for k in keys:
            if k in deps:
                cls.__order_deps_aux(order, deps, k)
        return order

    @classmethod
    def __order_deps_aux(cls, order, deps, key):
        """
        A recursive helper function for _order_deps
        """
        if key not in deps:
            return
        subdeps = deps.pop(key)
        for subk in subdeps:
            cls.__order_deps_aux(order, deps, subk)
        order.append(key)

    @classmethod
    def __convert_namespace(cls, ns_catalog, namespace):
        ns = ns_catalog.get_namespace(namespace)
        builder = NamespaceBuilder(ns.doc, ns.name,
                                   full_name=ns.full_name,
                                   version=ns.version,
                                   author=ns.author,
                                   contact=ns.contact)
        for elem in ns.schema:
            if 'namespace' in elem:
                inc_ns = elem['namespace']
                builder.include_namespace(inc_ns)
            else:
                source = elem['source']
                for dt in ns_catalog.get_types(source):
                    spec = ns_catalog.get_spec(namespace, dt)
                    if spec.parent is not None:
                        continue
                    h5_source = cls.__get_name(source)
                    spec = cls.__copy_spec(spec)
                    builder.add_spec(h5_source, spec)
        return builder

    @classmethod
    def __get_name(cls, path):
        return os.path.splitext(path)[0]

    @classmethod
    def __copy_spec(cls, spec):
        kwargs = dict()
        kwargs['attributes'] = cls.__get_new_specs(spec.attributes, spec)
        to_copy = ['doc', 'name', 'default_name', 'linkable', 'quantity', spec.inc_key(), spec.def_key()]
        if isinstance(spec, GroupSpec):
            kwargs['datasets'] = cls.__get_new_specs(spec.datasets, spec)
            kwargs['groups'] = cls.__get_new_specs(spec.groups, spec)
            kwargs['links'] = cls.__get_new_specs(spec.links, spec)
        else:
            to_copy.append('dtype')
            to_copy.append('shape')
            to_copy.append('dims')
        for key in to_copy:
            val = getattr(spec, key)
            if val is not None:
                kwargs[key] = val
        ret = spec.build_spec(kwargs)
        return ret

    @classmethod
    def __get_new_specs(cls, subspecs, spec):
        ret = list()
        for subspec in subspecs:
            if not spec.is_inherited_spec(subspec) or spec.is_overridden_spec(subspec):
                ret.append(subspec)
        return ret

    @classmethod
    @docval({'name': 'source_filename', 'type': str, 'doc': 'the path to the HDF5 file to copy'},
            {'name': 'dest_filename', 'type': str, 'doc': 'the name of the destination file'},
            {'name': 'expand_external', 'type': bool, 'doc': 'expand external links into new objects', 'default': True},
            {'name': 'expand_refs', 'type': bool, 'doc': 'copy objects which are pointed to by reference',
             'default': False},
            {'name': 'expand_soft', 'type': bool, 'doc': 'expand soft links into new objects', 'default': False}
            )
    def copy_file(self, **kwargs):
        """
        Convenience function to copy an HDF5 file while allowing external links to be resolved.

        .. warning::

            As of HDMF 2.0, this method is no longer supported and may be removed in a future version.
            Please use the export method or h5py.File.copy method instead.

        .. note::

            The source file will be opened in 'r' mode and the destination file will be opened in 'w' mode
            using h5py. To avoid possible collisions, care should be taken that, e.g., the source file is
            not opened already when calling this function.

        """

        warnings.warn("The copy_file class method is no longer supported and may be removed in a future version of "
                      "HDMF. Please use the export method or h5py.File.copy method instead.", DeprecationWarning)

        source_filename, dest_filename, expand_external, expand_refs, expand_soft = getargs('source_filename',
                                                                                            'dest_filename',
                                                                                            'expand_external',
                                                                                            'expand_refs',
                                                                                            'expand_soft',
                                                                                            kwargs)
        source_file = File(source_filename, 'r')
        dest_file = File(dest_filename, 'w')
        for objname in source_file["/"].keys():
            source_file.copy(source=objname,
                             dest=dest_file,
                             name=objname,
                             expand_external=expand_external,
                             expand_refs=expand_refs,
                             expand_soft=expand_soft,
                             shallow=False,
                             without_attrs=False,
                             )
        for objname in source_file['/'].attrs:
            dest_file['/'].attrs[objname] = source_file['/'].attrs[objname]
        source_file.close()
        dest_file.close()

    @docval({'name': 'container', 'type': Container, 'doc': 'the Container object to write'},
            {'name': 'cache_spec', 'type': bool,
             'doc': ('If True (default), cache specification to file (highly recommended). If False, do not cache '
                     'specification to file. The appropriate specification will then need to be loaded prior to '
                     'reading the file.'),
             'default': True},
            {'name': 'link_data', 'type': bool,
             'doc': 'If True (default), create external links to HDF5 Datasets. If False, copy HDF5 Datasets.',
             'default': True},
            {'name': 'exhaust_dci', 'type': bool,
             'doc': 'If True (default), exhaust DataChunkIterators one at a time. If False, exhaust them concurrently.',
             'default': True})
    def write(self, **kwargs):
        """Write the container to an HDF5 file."""
        if self.__mode == 'r':
            raise UnsupportedOperation(("Cannot write to file %s in mode '%s'. "
                                        "Please use mode 'r+', 'w', 'w-', 'x', or 'a'")
                                       % (self.source, self.__mode))

        cache_spec = popargs('cache_spec', kwargs)
        call_docval_func(super().write, kwargs)
        if cache_spec:
            self.__cache_spec()

    def __cache_spec(self):
        ref = self.__file.attrs.get(SPEC_LOC_ATTR)
        spec_group = None
        if ref is not None:
            spec_group = self.__file[ref]
        else:
            path = 'specifications'  # do something to figure out where the specifications should go
            spec_group = self.__file.require_group(path)
            self.__file.attrs[SPEC_LOC_ATTR] = spec_group.ref
        ns_catalog = self.manager.namespace_catalog
        for ns_name in ns_catalog.namespaces:
            ns_builder = self.__convert_namespace(ns_catalog, ns_name)
            namespace = ns_catalog.get_namespace(ns_name)
            group_name = '%s/%s' % (ns_name, namespace.version)
            if group_name in spec_group:
                continue
            ns_group = spec_group.create_group(group_name)
            writer = H5SpecWriter(ns_group)
            ns_builder.export(self.__ns_spec_path, writer=writer)

    _export_args = (
        {'name': 'src_io', 'type': 'HDMFIO', 'doc': 'the HDMFIO object for reading the data to export'},
        {'name': 'container', 'type': Container,
         'doc': ('the Container object to export. If None, then the entire contents of the HDMFIO object will be '
                 'exported'),
         'default': None},
        {'name': 'write_args', 'type': dict, 'doc': 'arguments to pass to :py:meth:`write_builder`',
         'default': dict()},
        {'name': 'cache_spec', 'type': bool, 'doc': 'whether to cache the specification to file',
         'default': True}
    )

    @docval(*_export_args)
    def export(self, **kwargs):
        """Export data read from a file from any backend to HDF5.

        See :py:meth:`hdmf.backends.io.HDMFIO.export` for more details.
        """
        if self.__mode != 'w':
            raise UnsupportedOperation("Cannot export to file %s in mode '%s'. Please use mode 'w'."
                                       % (self.source, self.__mode))

        src_io = getargs('src_io', kwargs)
        write_args, cache_spec = popargs('write_args', 'cache_spec', kwargs)

        if not isinstance(src_io, HDF5IO) and write_args.get('link_data', True):
            raise UnsupportedOperation("Cannot export from non-HDF5 backend %s to HDF5 with write argument "
                                       "link_data=True." % src_io.__class__.__name__)

        write_args['export_source'] = src_io.source  # pass export_source=src_io.source to write_builder
        ckwargs = kwargs.copy()
        ckwargs['write_args'] = write_args
        call_docval_func(super().export, ckwargs)
        if cache_spec:
            self.__cache_spec()

    @classmethod
    @docval({'name': 'path', 'type': str, 'doc': 'the path to the destination HDF5 file'},
            {'name': 'comm', 'type': 'Intracomm', 'doc': 'the MPI communicator to use for parallel I/O',
             'default': None},
            *_export_args)  # NOTE: src_io is required and is the second positional argument
    def export_io(self, **kwargs):
        """Export from one backend to HDF5 (class method).

        Convenience function for :py:meth:`export` where you do not need to
        instantiate a new `HDF5IO` object for writing. An `HDF5IO` object is created with mode 'w' and the given
        arguments.

        Example usage:

        .. code-block:: python

            old_io = HDF5IO('old.h5', 'r')
            HDF5IO.export_io(path='new_copy.h5', src_io=old_io)

        See :py:meth:`export` for more details.
        """
        path, comm = popargs('path', 'comm', kwargs)

        with HDF5IO(path=path, comm=comm, mode='w') as write_io:
            write_io.export(**kwargs)

    def read(self, **kwargs):
        if self.__mode == 'w' or self.__mode == 'w-' or self.__mode == 'x':
            raise UnsupportedOperation("Cannot read from file %s in mode '%s'. Please use mode 'r', 'r+', or 'a'."
                                       % (self.source, self.__mode))
        try:
            return call_docval_func(super().read, kwargs)
        except UnsupportedOperation as e:
            if str(e) == 'Cannot build data. There are no values.':  # pragma: no cover
                raise UnsupportedOperation("Cannot read data from file %s in mode '%s'. There are no values."
                                           % (self.source, self.__mode))

    @docval(returns='a GroupBuilder representing the data object', rtype='GroupBuilder')
    def read_builder(self):
        if not self.__file:
            raise UnsupportedOperation("Cannot read data from closed HDF5 file '%s'" % self.source)
        f_builder = self.__read.get(self.__file)
        # ignore cached specs when reading builder
        ignore = set()
        specloc = self.__file.attrs.get(SPEC_LOC_ATTR)
        if specloc is not None:
            ignore.add(self.__file[specloc].name)
        if f_builder is None:
            f_builder = self.__read_group(self.__file, ROOT_NAME, ignore=ignore)
            self.__read[self.__file] = f_builder
        return f_builder

    def __set_written(self, builder):
        """
        Mark this builder as written.

        :param builder: Builder object to be marked as written
        :type builder: Builder
        """
        builder_id = self.__builderhash(builder)
        self._written_builders[builder_id] = builder

    def get_written(self, builder):
        """Return True if this builder has been written to (or read from) disk by this IO object, False otherwise.

        :param builder: Builder object to get the written flag for
        :type builder: Builder

        :return: True if the builder is found in self._written_builders using the builder ID, False otherwise
        """
        builder_id = self.__builderhash(builder)
        return builder_id in self._written_builders

    def __builderhash(self, obj):
        """Return the ID of a builder for use as a unique hash."""
        return id(obj)

    def __set_built(self, fpath, id, builder):
        """
        Update self.__built to cache the given builder for the given file and id.

        :param fpath: Path to the HDF5 file containing the object
        :type fpath: str
        :param id: ID of the HDF5 object in the path
        :type id: h5py GroupID object
        :param builder: The builder to be cached
        """
        self.__built.setdefault(fpath, dict()).setdefault(id, builder)

    def __get_built(self, fpath, id):
        """
        Look up a builder for the given file and id in self.__built cache

        :param fpath: Path to the HDF5 file containing the object
        :type fpath: str
        :param id: ID of the HDF5 object in the path
        :type id: h5py GroupID object

        :return: Builder in the self.__built cache or None
        """

        fdict = self.__built.get(fpath)
        if fdict:
            return fdict.get(id)
        else:
            return None

    @docval({'name': 'h5obj', 'type': (Dataset, Group),
             'doc': 'the HDF5 object to the corresponding Builder object for'})
    def get_builder(self, **kwargs):
        """
        Get the builder for the corresponding h5py Group or Dataset

        :raises ValueError: When no builder has been constructed yet for the given h5py object
        """
        h5obj = getargs('h5obj', kwargs)
        fpath = h5obj.file.filename
        builder = self.__get_built(fpath, h5obj.id)
        if builder is None:
            msg = '%s:%s has not been built' % (fpath, h5obj.name)
            raise ValueError(msg)
        return builder

    @docval({'name': 'h5obj', 'type': (Dataset, Group),
             'doc': 'the HDF5 object to the corresponding Container/Data object for'})
    def get_container(self, **kwargs):
        """
        Get the container for the corresponding h5py Group or Dataset

        :raises ValueError: When no builder has been constructed yet for the given h5py object
        """
        h5obj = getargs('h5obj', kwargs)
        builder = self.get_builder(h5obj)
        container = self.manager.construct(builder)
        return container

    def __read_group(self, h5obj, name=None, ignore=set()):
        kwargs = {
            "attributes": self.__read_attrs(h5obj),
            "groups": dict(),
            "datasets": dict(),
            "links": dict()
        }

        for key, val in kwargs['attributes'].items():
            if isinstance(val, bytes):
                kwargs['attributes'][key] = val.decode('UTF-8')

        if name is None:
            name = str(os.path.basename(h5obj.name))
        for k in h5obj:
            sub_h5obj = h5obj.get(k)
            if not (sub_h5obj is None):
                if sub_h5obj.name in ignore:
                    continue
                link_type = h5obj.get(k, getlink=True)
                if isinstance(link_type, SoftLink) or isinstance(link_type, ExternalLink):
                    # Reading links might be better suited in its own function
                    # get path of link (the key used for tracking what's been built)
                    target_path = link_type.path
                    target_obj = sub_h5obj.file[target_path]
                    builder_name = os.path.basename(target_path)
                    parent_loc = os.path.dirname(target_path)
                    # get builder if already read, else build it
                    builder = self.__get_built(sub_h5obj.file.filename, target_obj.id)
                    if builder is None:
                        # NOTE: all links must have absolute paths
                        if isinstance(target_obj, Dataset):
                            builder = self.__read_dataset(target_obj, builder_name)
                        else:
                            builder = self.__read_group(target_obj, builder_name, ignore=ignore)
                        self.__set_built(sub_h5obj.file.filename,  target_obj.id, builder)
                    builder.location = parent_loc
                    link_builder = LinkBuilder(builder, k, source=h5obj.file.filename)
                    self.__set_written(link_builder)
                    kwargs['links'][builder_name] = link_builder
                    if isinstance(link_type, ExternalLink):
                        self.__open_links.append(sub_h5obj)
                else:
                    builder = self.__get_built(sub_h5obj.file.filename, sub_h5obj.id)
                    obj_type = None
                    read_method = None
                    if isinstance(sub_h5obj, Dataset):
                        read_method = self.__read_dataset
                        obj_type = kwargs['datasets']
                    else:
                        read_method = partial(self.__read_group, ignore=ignore)
                        obj_type = kwargs['groups']
                    if builder is None:
                        builder = read_method(sub_h5obj)
                        self.__set_built(sub_h5obj.file.filename, sub_h5obj.id, builder)
                    obj_type[builder.name] = builder
            else:
                warnings.warn(os.path.join(h5obj.name, k), BrokenLinkWarning)
                kwargs['datasets'][k] = None
                continue
        kwargs['source'] = h5obj.file.filename
        ret = GroupBuilder(name, **kwargs)
        self.__set_written(ret)
        return ret

    def __read_dataset(self, h5obj, name=None):
        kwargs = {
            "attributes": self.__read_attrs(h5obj),
            "dtype": h5obj.dtype,
            "maxshape": h5obj.maxshape
        }
        for key, val in kwargs['attributes'].items():
            if isinstance(val, bytes):
                kwargs['attributes'][key] = val.decode('UTF-8')

        if name is None:
            name = str(os.path.basename(h5obj.name))
        kwargs['source'] = h5obj.file.filename
        ndims = len(h5obj.shape)
        if ndims == 0:                                       # read scalar
            scalar = h5obj[()]
            if isinstance(scalar, bytes):
                scalar = scalar.decode('UTF-8')

            if isinstance(scalar, Reference):
                # TODO (AJTRITT):  This should call __read_ref to support Group references
                target = h5obj.file[scalar]
                target_builder = self.__read_dataset(target)
                self.__set_built(target.file.filename, target.id, target_builder)
                if isinstance(scalar, RegionReference):
                    d = RegionBuilder(scalar, target_builder)
                else:
                    d = ReferenceBuilder(target_builder)
                kwargs['data'] = d
                kwargs['dtype'] = d.dtype
            else:
                kwargs["data"] = scalar
        elif ndims == 1:
            d = None
            if h5obj.dtype.kind == 'O' and len(h5obj) > 0:
                elem1 = h5obj[0]
                if isinstance(elem1, (str, bytes)):
                    d = h5obj
                elif isinstance(elem1, RegionReference):  # read list of references
                    d = BuilderH5RegionDataset(h5obj, self)
                    kwargs['dtype'] = d.dtype
                elif isinstance(elem1, Reference):
                    d = BuilderH5ReferenceDataset(h5obj, self)
                    kwargs['dtype'] = d.dtype
            elif h5obj.dtype.kind == 'V':    # table / compound data type
                cpd_dt = h5obj.dtype
                ref_cols = [check_dtype(ref=cpd_dt[i]) or check_dtype(vlen=cpd_dt[i]) for i in range(len(cpd_dt))]
                d = BuilderH5TableDataset(h5obj, self, ref_cols)
                kwargs['dtype'] = HDF5IO.__compound_dtype_to_list(h5obj.dtype, d.dtype)
            else:
                d = h5obj
            kwargs["data"] = d
        else:
            kwargs["data"] = h5obj
        ret = DatasetBuilder(name, **kwargs)
        self.__set_written(ret)
        return ret

    @classmethod
    def __compound_dtype_to_list(cls, h5obj_dtype, dset_dtype):
        ret = []
        for name, dtype in zip(h5obj_dtype.fields, dset_dtype):
            ret.append({'name': name, 'dtype': dtype})
        return ret

    def __read_attrs(self, h5obj):
        ret = dict()
        for k, v in h5obj.attrs.items():
            if k == SPEC_LOC_ATTR:     # ignore cached spec
                continue
            if isinstance(v, RegionReference):
                raise ValueError("cannot read region reference attributes yet")
            elif isinstance(v, Reference):
                ret[k] = self.__read_ref(h5obj.file[v])
            else:
                ret[k] = v
        return ret

    def __read_ref(self, h5obj):
        ret = None
        ret = self.__get_built(h5obj.file.filename, h5obj.id)
        if ret is None:
            if isinstance(h5obj, Dataset):
                ret = self.__read_dataset(h5obj)
            elif isinstance(h5obj, Group):
                ret = self.__read_group(h5obj)
            else:
                raise ValueError("h5obj must be a Dataset or a Group - got %s" % str(h5obj))
            self.__set_built(h5obj.file.filename, h5obj.id, ret)
        return ret

    def open(self):
        if self.__file is None:
            open_flag = self.__mode
            kwargs = dict()
            if self.comm:
                kwargs.update(driver='mpio', comm=self.comm)

            if self.driver is not None:
                kwargs.update(driver=self.driver)

            self.__file = File(self.source, open_flag, **kwargs)

    def close(self):
        if self.__file is not None:
            self.__file.close()

    def close_linked_files(self):
        """Close all opened, linked-to files.

        MacOS and Linux automatically releases the linked-to file after the linking file is closed, but Windows does
        not, which prevents the linked-to file from being deleted or truncated. Use this method to close all opened,
        linked-to files.
        """
        for obj in self.__open_links:
            if obj:
                obj.file.close()
        self.__open_links = []

    @docval({'name': 'builder', 'type': GroupBuilder, 'doc': 'the GroupBuilder object representing the HDF5 file'},
            {'name': 'link_data', 'type': bool,
             'doc': 'If not specified otherwise link (True) or copy (False) HDF5 Datasets', 'default': True},
            {'name': 'exhaust_dci', 'type': bool,
             'doc': 'exhaust DataChunkIterators one at a time. If False, exhaust them concurrently',
             'default': True},
            {'name': 'export_source', 'type': str,
             'doc': 'The source of the builders when exporting', 'default': None})
    def write_builder(self, **kwargs):
        f_builder = popargs('builder', kwargs)
        link_data, exhaust_dci, export_source = getargs('link_data', 'exhaust_dci', 'export_source', kwargs)
        self.logger.debug("Writing GroupBuilder '%s' to path '%s' with kwargs=%s"
                          % (f_builder.name, self.source, kwargs))
        for name, gbldr in f_builder.groups.items():
            self.write_group(self.__file, gbldr, **kwargs)
        for name, dbldr in f_builder.datasets.items():
            self.write_dataset(self.__file, dbldr, **kwargs)
        for name, lbldr in f_builder.links.items():
            self.write_link(self.__file, lbldr)
        self.set_attributes(self.__file, f_builder.attributes)
        self.__add_refs()
        self.__exhaust_dcis()
        self.__set_written(f_builder)
        self.logger.debug("Done writing GroupBuilder '%s' to path '%s'" % (f_builder.name, self.source))

    def __add_refs(self):
        '''
        Add all references in the file.

        References get queued to be added at the end of write. This is because
        the current traversal algorithm (i.e. iterating over GroupBuilder items)
        does not happen in a guaranteed order. We need to figure out what objects
        will be references, and then write them after we write everything else.
        '''
        failed = set()
        while len(self.__ref_queue) > 0:
            call = self.__ref_queue.popleft()
            self.logger.debug("Adding reference with call id %d from queue (length %d)"
                              % (id(call), len(self.__ref_queue)))
            try:
                call()
            except KeyError:
                if id(call) in failed:
                    raise RuntimeError('Unable to resolve reference')
                self.logger.debug("Adding reference with call id %d failed. Appending call to queue" % id(call))
                failed.add(id(call))
                self.__ref_queue.append(call)

    def __exhaust_dcis(self):
        """
        Read and write from any queued DataChunkIterators in a round-robin fashion
        """
        while len(self.__dci_queue) > 0:
            self.logger.debug("Exhausting DataChunkIterator from queue (length %d)" % len(self.__dci_queue))
            dset, data = self.__dci_queue.popleft()
            if self.__write_chunk__(dset, data):
                self.__dci_queue.append((dset, data))

    @classmethod
    def get_type(cls, data):
        if isinstance(data, str):
            return H5_TEXT
        elif isinstance(data, Container):
            return H5_REF
        elif not hasattr(data, '__len__'):
            return type(data)
        else:
            if len(data) == 0:
                if hasattr(data, 'dtype'):
                    return data.dtype
                else:
                    raise ValueError('cannot determine type for empty data')
            return cls.get_type(data[0])

    __dtypes = {
        "float": np.float32,
        "float32": np.float32,
        "double": np.float64,
        "float64": np.float64,
        "long": np.int64,
        "int64": np.int64,
        "int": np.int32,
        "int32": np.int32,
        "short": np.int16,
        "int16": np.int16,
        "int8": np.int8,
        "uint64": np.uint64,
        "uint": np.uint32,
        "uint32": np.uint32,
        "uint16": np.uint16,
        "uint8": np.uint8,
        "bool": np.bool_,
        "text": H5_TEXT,
        "utf": H5_TEXT,
        "utf8": H5_TEXT,
        "utf-8": H5_TEXT,
        "ascii": H5_BINARY,
        "bytes": H5_BINARY,
        "ref": H5_REF,
        "reference": H5_REF,
        "object": H5_REF,
        "region": H5_REGREF,
        "isodatetime": H5_TEXT,
        "datetime": H5_TEXT,
    }

    @classmethod
    def __resolve_dtype__(cls, dtype, data):
        # TODO: These values exist, but I haven't solved them yet
        # binary
        # number
        dtype = cls.__resolve_dtype_helper__(dtype)
        if dtype is None:
            dtype = cls.get_type(data)
        return dtype

    @classmethod
    def __resolve_dtype_helper__(cls, dtype):
        if dtype is None:
            return None
        elif isinstance(dtype, str):
            return cls.__dtypes.get(dtype)
        elif isinstance(dtype, dict):
            return cls.__dtypes.get(dtype['reftype'])
        elif isinstance(dtype, np.dtype):
            # NOTE: some dtypes may not be supported, but we need to support writing of read-in compound types
            return dtype
        else:
            return np.dtype([(x['name'], cls.__resolve_dtype_helper__(x['dtype'])) for x in dtype])

    @docval({'name': 'obj', 'type': (Group, Dataset), 'doc': 'the HDF5 object to add attributes to'},
            {'name': 'attributes',
             'type': dict,
             'doc': 'a dict containing the attributes on the Group or Dataset, indexed by attribute name'})
    def set_attributes(self, **kwargs):
        obj, attributes = getargs('obj', 'attributes', kwargs)
        for key, value in attributes.items():
            try:
                if isinstance(value, (set, list, tuple)):
                    tmp = tuple(value)
                    if len(tmp) > 0:
                        if isinstance(tmp[0], str):
                            value = [np.unicode_(s) for s in tmp]
                        elif isinstance(tmp[0], bytes):
                            value = [np.string_(s) for s in tmp]
                        elif isinstance(tmp[0], Container):  # a list of references
                            self.__queue_ref(self._make_attr_ref_filler(obj, key, tmp))
                        else:
                            value = np.array(value)
                    self.logger.debug("Setting %s '%s' attribute '%s' to %s"
                                      % (obj.__class__.__name__, obj.name, key, value.__class__.__name__))
                    obj.attrs[key] = value
                elif isinstance(value, (Container, Builder, ReferenceBuilder)):           # a reference
                    self.__queue_ref(self._make_attr_ref_filler(obj, key, value))
                else:
                    self.logger.debug("Setting %s '%s' attribute '%s' to %s"
                                      % (obj.__class__.__name__, obj.name, key, value.__class__.__name__))
                    obj.attrs[key] = value                   # a regular scalar
            except Exception as e:
                msg = "unable to write attribute '%s' on object '%s'" % (key, obj.name)
                raise RuntimeError(msg) from e

    def _make_attr_ref_filler(self, obj, key, value):
        '''
            Make the callable for setting references to attributes
        '''
        self.logger.debug("Queueing set %s '%s' attribute '%s' to %s"
                          % (obj.__class__.__name__, obj.name, key, value.__class__.__name__))
        if isinstance(value, (tuple, list)):
            def _filler():
                ret = list()
                for item in value:
                    ret.append(self.__get_ref(item))
                obj.attrs[key] = ret
        else:
            def _filler():
                obj.attrs[key] = self.__get_ref(value)
        return _filler

    @docval({'name': 'parent', 'type': Group, 'doc': 'the parent HDF5 object'},
            {'name': 'builder', 'type': GroupBuilder, 'doc': 'the GroupBuilder to write'},
            {'name': 'link_data', 'type': bool,
             'doc': 'If not specified otherwise link (True) or copy (False) HDF5 Datasets', 'default': True},
            {'name': 'exhaust_dci', 'type': bool,
             'doc': 'exhaust DataChunkIterators one at a time. If False, exhaust them concurrently',
             'default': True},
            {'name': 'export_source', 'type': str,
             'doc': 'The source of the builders when exporting', 'default': None},
            returns='the Group that was created', rtype='Group')
    def write_group(self, **kwargs):
        parent, builder = popargs('parent', 'builder', kwargs)
        self.logger.debug("Writing GroupBuilder '%s' to parent group '%s'" % (builder.name, parent.name))
        if self.get_written(builder):
            self.logger.debug("    GroupBuilder '%s' is already written" % builder.name)
            group = parent[builder.name]
        else:
            self.logger.debug("    Creating group '%s'" % builder.name)
            group = parent.create_group(builder.name)
        # write all groups
        subgroups = builder.groups
        if subgroups:
            for subgroup_name, sub_builder in subgroups.items():
                # do not create an empty group without attributes or links
                self.write_group(group, sub_builder, **kwargs)
        # write all datasets
        datasets = builder.datasets
        if datasets:
            for dset_name, sub_builder in datasets.items():
                self.write_dataset(group, sub_builder, **kwargs)
        # write all links
        links = builder.links
        if links:
            for link_name, sub_builder in links.items():
                self.write_link(group, sub_builder)
        attributes = builder.attributes
        self.set_attributes(group, attributes)
        self.__set_written(builder)
        return group

    def __get_path(self, builder):
        """Get the path to the builder.

        Note that the root of the file has no name - it is just "/". Thus, the name of the root container is ignored.
        """
        curr = builder
        names = list()
        while curr.parent is not None:
            names.append(curr.name)
            curr = curr.parent
        delim = "/"
        path = "%s%s" % (delim, delim.join(reversed(names)))
        return path

    @docval({'name': 'parent', 'type': Group, 'doc': 'the parent HDF5 object'},
            {'name': 'builder', 'type': LinkBuilder, 'doc': 'the LinkBuilder to write'},
            returns='the Link that was created', rtype='Link')
    def write_link(self, **kwargs):
        parent, builder = getargs('parent', 'builder', kwargs)
        self.logger.debug("Writing LinkBuilder '%s' to parent group '%s'" % (builder.name, parent.name))
        if self.get_written(builder):
            self.logger.debug("    LinkBuilder '%s' is already written" % builder.name)
            return None
        name = builder.name
        target_builder = builder.builder
        path = self.__get_path(target_builder)
        # source will indicate target_builder's location
        if builder.source == target_builder.source:
            link_obj = SoftLink(path)
            self.logger.debug("    Creating SoftLink '%s/%s' to '%s'"
                              % (parent.name, name, link_obj.path))
        elif target_builder.source is not None:
            target_filename = os.path.abspath(target_builder.source)
            parent_filename = os.path.abspath(parent.file.filename)
            relative_path = os.path.relpath(target_filename, os.path.dirname(parent_filename))
            if target_builder.location is not None:
                path = target_builder.location + "/" + target_builder.name
            link_obj = ExternalLink(relative_path, path)
            self.logger.debug("    Creating ExternalLink '%s/%s' to '%s://%s'"
                              % (parent.name, name, link_obj.filename, link_obj.path))
        else:
            msg = 'cannot create external link to %s' % path
            raise ValueError(msg)
        parent[name] = link_obj
        self.__set_written(builder)
        return link_obj

    @docval({'name': 'parent', 'type': Group, 'doc': 'the parent HDF5 object'},  # noqa: C901
            {'name': 'builder', 'type': DatasetBuilder, 'doc': 'the DatasetBuilder to write'},
            {'name': 'link_data', 'type': bool,
             'doc': 'If not specified otherwise link (True) or copy (False) HDF5 Datasets', 'default': True},
            {'name': 'exhaust_dci', 'type': bool,
             'doc': 'exhaust DataChunkIterators one at a time. If False, exhaust them concurrently',
             'default': True},
            {'name': 'export_source', 'type': str,
             'doc': 'The source of the builders when exporting', 'default': None},
            returns='the Dataset that was created', rtype=Dataset)
    def write_dataset(self, **kwargs):  # noqa: C901
        """ Write a dataset to HDF5

        The function uses other dataset-dependent write functions, e.g,
        `__scalar_fill__`, `__list_fill__`, and `__setup_chunked_dset__` to write the data.
        """
        parent, builder = popargs('parent', 'builder', kwargs)
        link_data, exhaust_dci, export_source = getargs('link_data', 'exhaust_dci', 'export_source', kwargs)
        self.logger.debug("Writing DatasetBuilder '%s' to parent group '%s'" % (builder.name, parent.name))
        if self.get_written(builder):
            self.logger.debug("    DatasetBuilder '%s' is already written" % builder.name)
            return None
        name = builder.name
        data = builder.data
        options = dict()   # dict with additional
        if isinstance(data, H5DataIO):
            options['io_settings'] = data.io_settings
            link_data = data.link_data
            data = data.data
        else:
            options['io_settings'] = {}
        attributes = builder.attributes
        options['dtype'] = builder.dtype
        dset = None
        link = None

        # The user provided an existing h5py dataset as input and asked to create a link to the dataset
        if isinstance(data, Dataset):
            data_filename = os.path.abspath(data.file.filename)
            if link_data:
                if export_source is None:  # not exporting
                    parent_filename = os.path.abspath(parent.file.filename)
                    if data_filename != parent_filename:  # create external link to data
                        relative_path = os.path.relpath(data_filename, os.path.dirname(parent_filename))
                        link = ExternalLink(relative_path, data.name)
                        self.logger.debug("    Creating ExternalLink '%s/%s' to '%s://%s'"
                                          % (parent.name, name, link.filename, link.path))
                    else:  # create soft link to dataset already in this file -- possible if mode == 'r+'
                        link = SoftLink(data.name)
                        self.logger.debug("    Creating SoftLink '%s/%s' to '%s'"
                                          % (parent.name, name, link.path))
                    parent[name] = link
                else:  # exporting
                    export_source = os.path.abspath(export_source)
                    parent_filename = os.path.abspath(parent.file.filename)
                    if data_filename != export_source:  # dataset is in different file than export source
                        # possible if user adds a link to a dataset in a different file after reading export source
                        # to memory
                        relative_path = os.path.relpath(data_filename, os.path.dirname(parent_filename))
                        link = ExternalLink(relative_path, data.name)
                        self.logger.debug("    Creating ExternalLink '%s/%s' to '%s://%s'"
                                          % (parent.name, name, link.filename, link.path))
                        parent[name] = link
                    elif parent.name != data.parent.name:  # dataset is in export source and has different path
                        # so create a soft link to the dataset in this file
                        # possible if user adds a link to a dataset in export source after reading to memory
                        link = SoftLink(data.name)
                        self.logger.debug("    Creating SoftLink '%s/%s' to '%s'"
                                          % (parent.name, name, link.path))
                        parent[name] = link
                    else:  # dataset is in export source and has same path as the builder, so copy the dataset
                        self.logger.debug("    Copying data from '%s://%s' to '%s/%s'"
                                          % (data.file.filename, data.name, parent.name, name))
                        parent.copy(source=data,
                                    dest=parent,
                                    name=name,
                                    expand_soft=False,
                                    expand_external=False,
                                    expand_refs=False,
                                    without_attrs=True)
                        dset = parent[name]
            else:
                # TODO add option for case where there are multiple links to the same dataset within a file:
                # instead of copying the dset N times, copy it once and create soft links to it within the file
                self.logger.debug("    Copying data from '%s://%s' to '%s/%s'"
                                  % (data.file.filename, data.name, parent.name, name))
                parent.copy(source=data,
                            dest=parent,
                            name=name,
                            expand_soft=False,
                            expand_external=False,
                            expand_refs=False,
                            without_attrs=True)
                dset = parent[name]

        #  Write a compound dataset, i.e, a dataset with compound data type
        elif isinstance(options['dtype'], list):
            # do some stuff to figure out what data is a reference
            refs = list()
            for i, dts in enumerate(options['dtype']):
                if self.__is_ref(dts):
                    refs.append(i)
            # If one ore more of the parts of the compound data type are references then we need to deal with those
            if len(refs) > 0:
                try:
                    _dtype = self.__resolve_dtype__(options['dtype'], data)
                except Exception as exc:
                    msg = 'cannot add %s to %s - could not determine type' % (name, parent.name)
                    raise Exception(msg) from exc
                dset = parent.require_dataset(name, shape=(len(data),), dtype=_dtype, **options['io_settings'])
                self.__set_written(builder)
                self.logger.debug("Queueing reference resolution and set attribute on dataset '%s' containing "
                                  "object references. attributes: %s"
                                  % (name, list(attributes.keys())))

                @self.__queue_ref
                def _filler():
                    self.logger.debug("Resolving object references and setting attribute on dataset '%s' "
                                      "containing attributes: %s"
                                      % (name, list(attributes.keys())))
                    ret = list()
                    for item in data:
                        new_item = list(item)
                        for i in refs:
                            new_item[i] = self.__get_ref(item[i])
                        ret.append(tuple(new_item))
                    dset = parent[name]
                    dset[:] = ret
                    self.set_attributes(dset, attributes)

                return
            # If the compound data type contains only regular data (i.e., no references) then we can write it as usual
            else:
                dset = self.__list_fill__(parent, name, data, options)
        # Write a dataset containing references, i.e., a region or object reference.
        # NOTE: we can ignore options['io_settings'] for scalar data
        elif self.__is_ref(options['dtype']):
            _dtype = self.__dtypes.get(options['dtype'])
            # Write a scalar data region reference dataset
            if isinstance(data, RegionBuilder):
                dset = parent.require_dataset(name, shape=(), dtype=_dtype)
                self.__set_written(builder)
                self.logger.debug("Queueing reference resolution and set attribute on dataset '%s' containing a "
                                  "region reference. attributes: %s"
                                  % (name, list(attributes.keys())))

                @self.__queue_ref
                def _filler():
                    self.logger.debug("Resolving region reference and setting attribute on dataset '%s' "
                                      "containing attributes: %s"
                                      % (name, list(attributes.keys())))
                    ref = self.__get_ref(data.builder, data.region)
                    dset = parent[name]
                    dset[()] = ref
                    self.set_attributes(dset, attributes)
            # Write a scalar object reference dataset
            elif isinstance(data, ReferenceBuilder):
                dset = parent.require_dataset(name, dtype=_dtype, shape=())
                self.__set_written(builder)
                self.logger.debug("Queueing reference resolution and set attribute on dataset '%s' containing an "
                                  "object reference. attributes: %s"
                                  % (name, list(attributes.keys())))

                @self.__queue_ref
                def _filler():
                    self.logger.debug("Resolving object reference and setting attribute on dataset '%s' "
                                      "containing attributes: %s"
                                      % (name, list(attributes.keys())))
                    ref = self.__get_ref(data.builder)
                    dset = parent[name]
                    dset[()] = ref
                    self.set_attributes(dset, attributes)
            # Write an array dataset of references
            else:
                # Write a array of region references
                if options['dtype'] == 'region':
                    dset = parent.require_dataset(name, dtype=_dtype, shape=(len(data),), **options['io_settings'])
                    self.__set_written(builder)
                    self.logger.debug("Queueing reference resolution and set attribute on dataset '%s' containing "
                                      "region references. attributes: %s"
                                      % (name, list(attributes.keys())))

                    @self.__queue_ref
                    def _filler():
                        self.logger.debug("Resolving region references and setting attribute on dataset '%s' "
                                          "containing attributes: %s"
                                          % (name, list(attributes.keys())))
                        refs = list()
                        for item in data:
                            refs.append(self.__get_ref(item.builder, item.region))
                        dset = parent[name]
                        dset[()] = refs
                        self.set_attributes(dset, attributes)
                # Write array of object references
                else:
                    dset = parent.require_dataset(name, shape=(len(data),), dtype=_dtype, **options['io_settings'])
                    self.__set_written(builder)
                    self.logger.debug("Queueing reference resolution and set attribute on dataset '%s' containing "
                                      "object references. attributes: %s"
                                      % (name, list(attributes.keys())))

                    @self.__queue_ref
                    def _filler():
                        self.logger.debug("Resolving object references and setting attribute on dataset '%s' "
                                          "containing attributes: %s"
                                          % (name, list(attributes.keys())))
                        refs = list()
                        for item in data:
                            refs.append(self.__get_ref(item))
                        dset = parent[name]
                        dset[()] = refs
                        self.set_attributes(dset, attributes)
            return
        # write a "regular" dataset
        else:
            # Write a scalar dataset containing a single string
            if isinstance(data, (str, bytes)):
                dset = self.__scalar_fill__(parent, name, data, options)
            # Iterative write of a data chunk iterator
            elif isinstance(data, AbstractDataChunkIterator):
                dset = self.__setup_chunked_dset__(parent, name, data, options)
                self.__dci_queue.append((dset, data))
            # Write a regular in memory array (e.g., numpy array, list etc.)
            elif hasattr(data, '__len__'):
                dset = self.__list_fill__(parent, name, data, options)
            # Write a regular scalar dataset
            else:
                dset = self.__scalar_fill__(parent, name, data, options)
        # Create the attributes on the dataset only if we are the primary and not just a Soft/External link
        if link is None:
            self.set_attributes(dset, attributes)
        # Validate the attributes on the linked dataset
        elif len(attributes) > 0:
            pass
        self.__set_written(builder)
        if exhaust_dci:
            self.__exhaust_dcis()

    @classmethod
    def __scalar_fill__(cls, parent, name, data, options=None):
        dtype = None
        io_settings = {}
        if options is not None:
            dtype = options.get('dtype')
            io_settings = options.get('io_settings')
        if not isinstance(dtype, type):
            try:
                dtype = cls.__resolve_dtype__(dtype, data)
            except Exception as exc:
                msg = 'cannot add %s to %s - could not determine type' % (name, parent.name)
                raise Exception(msg) from exc
        try:
            dset = parent.create_dataset(name, data=data, shape=None, dtype=dtype, **io_settings)
        except Exception as exc:
            msg = "Could not create scalar dataset %s in %s" % (name, parent.name)
            raise Exception(msg) from exc
        return dset

    @classmethod
    def __setup_chunked_dset__(cls, parent, name, data, options=None):
        """
        Setup a dataset for writing to one-chunk-at-a-time based on the given DataChunkIterator

        :param parent: The parent object to which the dataset should be added
        :type parent: h5py.Group, h5py.File
        :param name: The name of the dataset
        :type name: str
        :param data: The data to be written.
        :type data: DataChunkIterator
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
        # Define the maxshape of the data if not provided by the user
        if 'maxshape' not in io_settings:
            io_settings['maxshape'] = data.maxshape
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

    @classmethod
    def __write_chunk__(cls, dset, data):
        """
        Read a chunk from the given DataChunkIterator and write it to the given Dataset

        :param dset: The Dataset to write to
        :type dset: Dataset
        :param data: The DataChunkIterator to read from
        :type data: DataChunkIterator
        :return: True of a chunk was written, False otherwise
        :rtype: bool

        """
        try:
            chunk_i = next(data)
        except StopIteration:
            return False
        if isinstance(chunk_i.selection, tuple):
            # Determine the minimum array dimensions to fit the chunk selection
            max_bounds = tuple([x.stop or 0 if isinstance(x, slice) else x+1 for x in chunk_i.selection])
        elif isinstance(chunk_i.selection, int):
            max_bounds = (chunk_i.selection+1, )
        elif isinstance(chunk_i.selection, slice):
            max_bounds = (chunk_i.selection.stop or 0, )
        else:
            msg = ("Chunk selection %s must be a single int, single slice, or tuple of slices "
                   "and/or integers") % str(chunk_i.selection)
            raise TypeError(msg)

        # Expand the dataset if needed
        dset.id.extend(max_bounds)
        # Write the data
        dset[chunk_i.selection] = chunk_i.data

        return True

    @classmethod
    def __chunked_iter_fill__(cls, parent, name, data, options=None):
        """
        Write data to a dataset one-chunk-at-a-time based on the given DataChunkIterator

        :param parent: The parent object to which the dataset should be added
        :type parent: h5py.Group, h5py.File
        :param name: The name of the dataset
        :type name: str
        :param data: The data to be written.
        :type data: DataChunkIterator
        :param options: Dict with options for creating a dataset. available options are 'dtype' and 'io_settings'
        :type options: dict

        """
        dset = cls.__setup_chunked_dset__(parent, name, data, options=options)
        read = True
        while read:
            read = cls.__write_chunk__(dset, data)
        return dset

    @classmethod
    def __list_fill__(cls, parent, name, data, options=None):
        # define the io settings and data type if necessary
        io_settings = {}
        dtype = None
        if options is not None:
            dtype = options.get('dtype')
            io_settings = options.get('io_settings')
        if not isinstance(dtype, type):
            try:
                dtype = cls.__resolve_dtype__(dtype, data)
            except Exception as exc:
                msg = 'cannot add %s to %s - could not determine type' % (name, parent.name)
                raise Exception(msg) from exc
        # define the data shape
        if 'shape' in io_settings:
            data_shape = io_settings.pop('shape')
        elif hasattr(data, 'shape'):
            data_shape = data.shape
        elif isinstance(dtype, np.dtype):
            data_shape = (len(data),)
        else:
            data_shape = get_data_shape(data)
        # Create the dataset
        try:
            dset = parent.create_dataset(name, shape=data_shape, dtype=dtype, **io_settings)
        except Exception as exc:
            msg = "Could not create dataset %s in %s with shape %s, dtype %s, and iosettings %s. %s" % \
                  (name, parent.name, str(data_shape), str(dtype), str(io_settings), str(exc))
            raise Exception(msg) from exc
        # Write the data
        if len(data) > dset.shape[0]:
            new_shape = list(dset.shape)
            new_shape[0] = len(data)
            dset.resize(new_shape)
        try:
            dset[:] = data
        except Exception as e:
            raise e
        return dset

    @docval({'name': 'container', 'type': (Builder, Container, ReferenceBuilder), 'doc': 'the object to reference',
             'default': None},
            {'name': 'region', 'type': (slice, list, tuple), 'doc': 'the region reference indexing object',
             'default': None},
            returns='the reference', rtype=Reference)
    def __get_ref(self, **kwargs):
        container, region = getargs('container', 'region', kwargs)
        if container is None:
            return None
        if isinstance(container, Builder):
            self.logger.debug("Getting reference for %s '%s'" % (container.__class__.__name__, container.name))
            if isinstance(container, LinkBuilder):
                builder = container.target_builder
            else:
                builder = container
        elif isinstance(container, ReferenceBuilder):
            self.logger.debug("Getting reference for %s '%s'" % (container.__class__.__name__, container.builder.name))
            builder = container.builder
        else:
            self.logger.debug("Getting reference for %s '%s'" % (container.__class__.__name__, container.name))
            builder = self.manager.build(container)
        path = self.__get_path(builder)
        self.logger.debug("Getting reference at path '%s'" % path)
        if isinstance(container, RegionBuilder):
            region = container.region
        if region is not None:
            dset = self.__file[path]
            if not isinstance(dset, Dataset):
                raise ValueError('cannot create region reference without Dataset')
            return self.__file[path].regionref[region]
        else:
            return self.__file[path].ref

    def __is_ref(self, dtype):
        if isinstance(dtype, DtypeSpec):
            return self.__is_ref(dtype.dtype)
        if isinstance(dtype, RefSpec):
            return True
        if isinstance(dtype, dict):  # may be dict from reading a compound dataset
            return self.__is_ref(dtype['dtype'])
        if isinstance(dtype, str):
            return dtype == DatasetBuilder.OBJECT_REF_TYPE or dtype == DatasetBuilder.REGION_REF_TYPE
        return False

    def __queue_ref(self, func):
        '''Set aside filling dset with references

        dest[sl] = func()

        Args:
           dset: the h5py.Dataset that the references need to be added to
           sl: the np.s_ (slice) object for indexing into dset
           func: a function to call to return the chunk of data, with
                 references filled in
        '''
        # TODO: come up with more intelligent way of
        # queueing reference resolution, based on reference
        # dependency
        self.__ref_queue.append(func)

    def __rec_get_ref(self, ref_list):
        ret = list()
        for elem in ref_list:
            if isinstance(elem, (list, tuple)):
                ret.append(self.__rec_get_ref(elem))
            elif isinstance(elem, (Builder, Container)):
                ret.append(self.__get_ref(elem))
            else:
                ret.append(elem)
        return ret

    @property
    def mode(self):
        """
        Return the HDF5 file mode. One of ("w", "r", "r+", "a", "w-", "x").
        """
        return self.__mode

    @classmethod
    @docval(*get_docval(H5DataIO.__init__))
    def set_dataio(cls, **kwargs):
        """
        Wrap the given Data object with an H5DataIO.

        This method is provided merely for convenience. It is the equivalent
        of the following:

        ```
        from hdmf.backends.hdf5 import H5DataIO
        data = ...
        data = H5DataIO(data)
        ```
        """
        cargs, ckwargs = fmt_docval_args(H5DataIO.__init__, kwargs)
        return H5DataIO(*cargs, **ckwargs)
