from ..utils import docval, popargs
from ..container import Container
from ..build import BuildManager, TypeMap

from . import hdf5


@docval({'name': 'container', 'type': Container, 'doc': 'the Container to export'},
        {'name': 'write_io_cls', 'type': type, 'doc': 'a subclass of HDMFIO to use for exporting the file'},
        {'name': 'type_map', 'type': TypeMap, 'doc': 'the TypeMap to use'},
        {'name': 'write_io_args', 'type': dict,
         'doc': 'dict of arguments to use when initializing a new instance of class write_io_cls',
         'default': dict()},
        {'name': 'write_args', 'type': dict, 'doc': 'dict of arguments to use when calling write_io.write',
         'default': dict()},
        {'name': 'keep_external_links', 'type': bool,
         'doc': ('whether to preserve links to external files. If False (default), all external links will be '
                 'resolved. This flag depends on support from the write_io_cls class'),
         'default': False})
def export_container(**kwargs):
    """
    Export the container to a new destination using the given HDF5IO class.

    The `container` can be an unwritten Container or a Container read from a source. Unlike HDMFIO.write,
    export_container allows writing Containers to a new destination, regardless of the source of the Container. It does
    this by creating a clean instance of the HDMFIO class in 'w' mode and a BuildManager flagged for exporting using
    the given TypeMap.

    The 'manager' key is not allowed in `write_io_args` because a new BuildManager will be used.

    Some arguments in `write_args` may not be supported during export.

    Example usage:

        # export container to out.h5 without caching the spec and with keeping external links
        export_container(
            container=container,
            write_io_cls=HDF5IO,
            type_map=type_map,
            write_io_args={'path': 'out.h5'},
            write_args={'cache_spec': False},
            keep_external_links=True,
        )
    """
    # NOTE: this function is designed to prevent users from accessing the HDMFIO or BuildManager instances created
    # during export, which could lead to unintended behavior
    container, type_map = popargs('container', 'type_map', kwargs)
    write_io_cls, write_io_args = popargs('write_io_cls', 'write_io_args', kwargs)
    write_args, keep_external_links = popargs('write_args', 'keep_external_links', kwargs)

    if 'manager' in write_io_args:
        raise ValueError("The 'manager' key is not allowed in write_io_args because a new BuildManager will be used.")

    export_manager = BuildManager(type_map, export=True)
    write_io_args['manager'] = export_manager
    with write_io_cls(**write_io_args) as write_io:
        write_io.write(container=container, **write_args)


@docval({'name': 'read_io_cls', 'type': type, 'doc': 'a subclass of HDMFIO to use for opening a source'},
        {'name': 'write_io_cls', 'type': type, 'doc': 'a subclass of HDMFIO to use for exporting'},
        {'name': 'type_map', 'type': TypeMap, 'doc': 'the TypeMap to use'},
        {'name': 'read_io_args', 'type': dict,
         'doc': 'dict of arguments to use when initializing a new instance of class read_io_cls',
         'default': dict()},
        {'name': 'read_args', 'type': dict,
         'doc': 'dict of arguments to use when calling read_io.read',
         'default': dict()},
        {'name': 'write_io_args', 'type': dict,
         'doc': 'dict of arguments to use when initializing a new instance of class write_io_cls',
         'default': dict()},
        {'name': 'write_args', 'type': dict, 'doc': 'dict of arguments to use when calling write_io.write',
         'default': dict()},
        {'name': 'keep_external_links', 'type': bool,
         'doc': ('whether to preserve links to external files. If False (default), all external links will be '
                 'resolved. This flag depends on support from the write_io_cls class'),
         'default': False})
def export_io(**kwargs):
    """
    Export from one HDMFIO class to another with the given arguments.

    Similar to the export_container function, except this function will read the contents of a source using the given
    read_io_cls and export those contents to a new destination. Arguments can be passed in for the read_io_cls.read and
    write_io.write methods.

    The 'manager' key is not allowed in `write_io_args` because a new BuildManager will be used.

    Some arguments in `write_args` may not be supported during export.

    Example usage:

        # export the contents of in.h5 to out.h5 without caching the spec and with keeping external links
        export_io(
            read_io_cls=HDF5IO,
            write_io_cls=HDF5IO,
            type_map=type_map,
            read_io_args={'path': 'in.h5'},
            read_args={},
            write_io_args={'path': 'out.h5'},
            write_args={'cache_spec': False},
            keep_external_links=True,
        )
    """
    read_io_cls, read_io_args, read_args = popargs('read_io_cls', 'read_io_args', 'read_args', kwargs)
    read_io = read_io_cls(**read_io_args)
    container = read_io.read(**read_args)
    export_container(container=container, **kwargs)
