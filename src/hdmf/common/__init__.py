'''This package will contain functions, classes, and objects
for reading and writing data in according to the HDMF-common specification
'''
import os.path
from copy import deepcopy

CORE_NAMESPACE = 'hdmf-common'
EXP_NAMESPACE = 'hdmf-experimental'


from ..spec import NamespaceCatalog  # noqa: E402
from ..utils import docval, getargs, get_docval  # noqa: E402
from ..backends.io import HDMFIO  # noqa: E402
from ..backends.hdf5 import HDF5IO  # noqa: E402
from ..validate import ValidatorMap  # noqa: E402
from ..build import BuildManager, TypeMap  # noqa: E402
from ..container import _set_exp  # noqa: E402


# a global type map
global __TYPE_MAP


# a function to register a container classes with the global map
@docval({'name': 'data_type', 'type': str, 'doc': 'the data_type to get the spec for'},
        {'name': 'namespace', 'type': str, 'doc': 'the name of the namespace', 'default': CORE_NAMESPACE},
        {"name": "container_cls", "type": type,
         "doc": "the class to map to the specified data_type", 'default': None},
        is_method=False)
def register_class(**kwargs):
    """Register an Container class to use for reading and writing a data_type from a specification
    If container_cls is not specified, returns a decorator for registering an Container subclass
    as the class for data_type in namespace.
    """
    data_type, namespace, container_cls = getargs('data_type', 'namespace', 'container_cls', kwargs)
    if namespace == EXP_NAMESPACE:
        def _dec(cls):
            _set_exp(cls)
            __TYPE_MAP.register_container_type(namespace, data_type, cls)
            return cls
    else:
        def _dec(cls):
            __TYPE_MAP.register_container_type(namespace, data_type, cls)
            return cls

    if container_cls is None:
        return _dec
    else:
        _dec(container_cls)


# a function to register an object mapper for a container class
@docval({"name": "container_cls", "type": type,
         "doc": "the Container class for which the given ObjectMapper class gets used for"},
        {"name": "mapper_cls", "type": type, "doc": "the ObjectMapper class to use to map", 'default': None},
        is_method=False)
def register_map(**kwargs):
    """Register an ObjectMapper to use for a Container class type
    If mapper_cls is not specified, returns a decorator for registering an ObjectMapper class
    as the mapper for container_cls. If mapper_cls specified, register the class as the mapper for container_cls
    """
    container_cls, mapper_cls = getargs('container_cls', 'mapper_cls', kwargs)

    def _dec(cls):
        __TYPE_MAP.register_map(container_cls, cls)
        return cls
    if mapper_cls is None:
        return _dec
    else:
        _dec(mapper_cls)


def __get_resources():
    try:
        from importlib.resources import files
    except ImportError:
        # TODO: Remove when python 3.9 becomes the new minimum
        from importlib_resources import files

    __location_of_this_file = files(__name__)
    __core_ns_file_name = 'namespace.yaml'
    __schema_dir = 'hdmf-common-schema/common'

    ret = dict()
    ret['namespace_path'] = str(__location_of_this_file / __schema_dir / __core_ns_file_name)
    return ret


def _get_resources():
    # LEGACY: Needed to support legacy implementation.
    return __get_resources()


@docval({'name': 'namespace_path', 'type': str,
         'doc': 'the path to the YAML with the namespace definition'},
        returns="the namespaces loaded from the given file", rtype=tuple,
        is_method=False)
def load_namespaces(**kwargs):
    '''
    Load namespaces from file
    '''
    namespace_path = getargs('namespace_path', kwargs)
    return __TYPE_MAP.load_namespaces(namespace_path)


def available_namespaces():
    return __TYPE_MAP.namespace_catalog.namespaces


# a function to get the container class for a give type
@docval({'name': 'data_type', 'type': str,
         'doc': 'the data_type to get the Container class for'},
        {'name': 'namespace', 'type': str, 'doc': 'the namespace the data_type is defined in'},
        is_method=False)
def get_class(**kwargs):
    """Get the class object of the Container subclass corresponding to a given neurdata_type.
    """
    data_type, namespace = getargs('data_type', 'namespace', kwargs)
    return __TYPE_MAP.get_dt_container_cls(data_type, namespace)


@docval({'name': 'extensions', 'type': (str, TypeMap, list),
         'doc': 'a path to a namespace, a TypeMap, or a list consisting paths to namespaces and TypeMaps',
         'default': None},
        returns="the namespaces loaded from the given file", rtype=tuple,
        is_method=False)
def get_type_map(**kwargs):
    '''
    Get a BuildManager to use for I/O using the given extensions. If no extensions are provided,
    return a BuildManager that uses the core namespace
    '''
    extensions = getargs('extensions', kwargs)
    type_map = None
    if extensions is None:
        type_map = deepcopy(__TYPE_MAP)
    else:
        if isinstance(extensions, TypeMap):
            type_map = extensions
        else:
            type_map = deepcopy(__TYPE_MAP)
        if isinstance(extensions, list):
            for ext in extensions:
                if isinstance(ext, str):
                    type_map.load_namespaces(ext)
                elif isinstance(ext, TypeMap):
                    type_map.merge(ext)
                else:
                    msg = 'extensions must be a list of paths to namespace specs or a TypeMaps'
                    raise ValueError(msg)
        elif isinstance(extensions, str):
            type_map.load_namespaces(extensions)
        elif isinstance(extensions, TypeMap):
            type_map.merge(extensions)
    return type_map


@docval(*get_docval(get_type_map),
        returns="a build manager with namespaces loaded from the given file", rtype=BuildManager,
        is_method=False)
def get_manager(**kwargs):
    '''
    Get a BuildManager to use for I/O using the given extensions. If no extensions are provided,
    return a BuildManager that uses the core namespace
    '''
    type_map = get_type_map(**kwargs)
    return BuildManager(type_map)


@docval({'name': 'io', 'type': HDMFIO,
         'doc': 'the HDMFIO object to read from'},
        {'name': 'namespace', 'type': str,
         'doc': 'the namespace to validate against', 'default': CORE_NAMESPACE},
        {'name': 'experimental', 'type': bool,
         'doc': 'data type is an experimental data type', 'default': False},
        returns="errors in the file", rtype=list,
        is_method=False)
def validate(**kwargs):
    """Validate an file against a namespace"""
    io, namespace, experimental = getargs('io', 'namespace', 'experimental', kwargs)
    if experimental:
        namespace = EXP_NAMESPACE
    builder = io.read_builder()
    validator = ValidatorMap(io.manager.namespace_catalog.get_namespace(name=namespace))
    return validator.validate(builder)


@docval(*get_docval(HDF5IO.__init__), is_method=False)
def get_hdf5io(**kwargs):
    """
    A convenience method for getting an HDF5IO object using an HDMF-common build manager if none is provided.
    """
    manager = getargs('manager', kwargs)
    if manager is None:
        kwargs['manager'] = get_manager()
    return HDF5IO(**kwargs)


# load the hdmf-common namespace
__resources = __get_resources()
if os.path.exists(__resources['namespace_path']):
    __TYPE_MAP = TypeMap(NamespaceCatalog())

    load_namespaces(__resources['namespace_path'])

    # import these so the TypeMap gets populated
    from . import io as __io  # noqa: E402

    from . import table  # noqa: E402
    from . import alignedtable  # noqa: E402
    from . import sparse  # noqa: E402
    from . import resources  # noqa: E402
    from . import multi  # noqa: E402

    # register custom class generators
    from .io.table import DynamicTableGenerator
    __TYPE_MAP.register_generator(DynamicTableGenerator)

    from .. import Data, Container
    __TYPE_MAP.register_container_type(CORE_NAMESPACE, 'Container', Container)
    __TYPE_MAP.register_container_type(CORE_NAMESPACE, 'Data', Data)

else:
    raise RuntimeError("Unable to load a TypeMap - no namespace file found")


DynamicTable = get_class('DynamicTable', CORE_NAMESPACE)
VectorData = get_class('VectorData', CORE_NAMESPACE)
VectorIndex = get_class('VectorIndex', CORE_NAMESPACE)
ElementIdentifiers = get_class('ElementIdentifiers', CORE_NAMESPACE)
DynamicTableRegion = get_class('DynamicTableRegion', CORE_NAMESPACE)
EnumData = get_class('EnumData', EXP_NAMESPACE)
CSRMatrix = get_class('CSRMatrix', CORE_NAMESPACE)
HERD = get_class('HERD', EXP_NAMESPACE)
SimpleMultiContainer = get_class('SimpleMultiContainer', CORE_NAMESPACE)
AlignedDynamicTable = get_class('AlignedDynamicTable', CORE_NAMESPACE)
