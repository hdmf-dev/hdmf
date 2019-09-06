'''This package will contain functions, classes, and objects
for reading and writing data in NWB format
'''
import os.path
from copy import deepcopy

CORE_NAMESPACE = 'hdmf-common'

from ..spec import NamespaceCatalog  # noqa: E402
from ..utils import docval, getargs, call_docval_func  # noqa: E402
from ..backends.io import HDMFIO  # noqa: E402
from ..validate import ValidatorMap  # noqa: E402
from ..build import BuildManager, TypeMap  # noqa: E402


# a global type map
global __TYPE_MAP


__rct_kwargs = list()


# a function to register a container classes with the global map
@docval({'name': 'data_type', 'type': str, 'doc': 'the data_type to get the spec for'},
        {'name': 'namespace', 'type': str, 'doc': 'the name of the namespace', 'default': CORE_NAMESPACE},
        {"name": "container_cls", "type": type,
         "doc": "the class to map to the specified data_type", 'default': None},
        is_method=False)
def register_class(**kwargs):
    """Register an NWBContainer class to use for reading and writing a data_type from a specification
    If container_cls is not specified, returns a decorator for registering an NWBContainer subclass
    as the class for data_type in namespace.
    """
    data_type, namespace, container_cls = getargs('data_type', 'namespace', 'container_cls', kwargs)

    def _dec(cls):
        __rct_kwargs.append({'data_type': data_type, 'namespace': namespace, 'container_cls': cls})
        __TYPE_MAP.register_container_type(namespace, data_type, cls)
        return cls
    if container_cls is None:
        return _dec
    else:
        _dec(container_cls)


__rm_kwargs = list()


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
        __rm_kwargs.append({'mapper_cls': cls, 'container_cls': container_cls})
        __TYPE_MAP.register_map(container_cls, cls)
        return cls
    if mapper_cls is None:
        return _dec
    else:
        _dec(mapper_cls)


def __get_resources():
    from pkg_resources import resource_filename
    from os.path import join
    __core_ns_file_name = 'namespace.yaml'
    __typemap_pkl_name = 'typemap.pkl'

    ret = dict()
    ret['namespace_path'] = join(resource_filename(__name__, 'hdmf-common-schema/common'), __core_ns_file_name)
    ret['cached_typemap_path'] = resource_filename(__name__, __typemap_pkl_name)
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


# load the core namespace i.e. base NWB specification
__resources = __get_resources()
if os.path.exists(__resources['cached_typemap_path']):
    import pickle
    with open(__resources['cached_typemap_path'], 'rb') as f:
        __TYPE_MAP = pickle.load(f)
elif os.path.exists(__resources['namespace_path']):
    __TYPE_MAP = TypeMap(NamespaceCatalog())

    load_namespaces(__resources['namespace_path'])

    # import these so the TypeMap gets populated
    from . import io as __io  # noqa: F401,E402

    from . import table  # noqa: F401,E402
    from . import sparse  # noqa: F401,E402

    for _ in __rct_kwargs:
        __TYPE_MAP.register_container_type(**_)
    for _ in __rm_kwargs:
        __TYPE_MAP.register_map(**_)
else:
    raise RuntimeError("Unable to load a TypeMap")


DynamicTable = __TYPE_MAP.get_container_cls(CORE_NAMESPACE, 'DynamicTable')
VectorData = __TYPE_MAP.get_container_cls(CORE_NAMESPACE, 'VectorData')
VectorIndex = __TYPE_MAP.get_container_cls(CORE_NAMESPACE, 'VectorIndex')
DynamicTableRegion = __TYPE_MAP.get_container_cls(CORE_NAMESPACE, 'DynamicTableRegion')
CSRMatrix = __TYPE_MAP.get_container_cls(CORE_NAMESPACE, 'CSRMatrix')


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


@docval({'name': 'extensions', 'type': (str, TypeMap, list),
         'doc': 'a path to a namespace, a TypeMap, or a list consisting paths to namespaces and TypeMaps',
         'default': None},
        returns="the namespaces loaded from the given file", rtype=tuple,
        is_method=False)
def get_manager(**kwargs):
    '''
    Get a BuildManager to use for I/O using the given extensions. If no extensions are provided,
    return a BuildManager that uses the core namespace
    '''
    type_map = call_docval_func(get_type_map, kwargs)
    return BuildManager(type_map)


# a function to get the container class for a give type
@docval({'name': 'data_type', 'type': str,
         'doc': 'the data_type to get the Container class for'},
        {'name': 'namespace', 'type': str, 'doc': 'the namespace the data_type is defined in'},
        is_method=False)
def get_class(**kwargs):
    """Get the class object of the Container subclass corresponding to a given neurdata_type.
    """
    data_type, namespace = getargs('data_type', 'namespace', kwargs)
    return __TYPE_MAP.get_container_cls(namespace, data_type)


@docval({'name': 'io', 'type': HDMFIO,
         'doc': 'the HDMFIO object to read from'},
        {'name': 'namespace', 'type': str,
         'doc': 'the namespace to validate against', 'default': CORE_NAMESPACE},
        returns="errors in the file", rtype=list,
        is_method=False)
def validate(**kwargs):
    """Validate an file against a namespace"""
    io, namespace = getargs('io', 'namespace', kwargs)
    builder = io.read_builder()
    validator = ValidatorMap(io.manager.namespace_catalog.get_namespace(name=namespace))
    return validator.validate(builder)
