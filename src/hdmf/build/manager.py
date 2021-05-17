import logging
from collections import OrderedDict, deque
from copy import copy

from .builders import DatasetBuilder, GroupBuilder, LinkBuilder, Builder, BaseBuilder
from .classgenerator import ClassGenerator, CustomClassGenerator, MCIClassGenerator
from ..container import AbstractContainer, Container, Data
from ..spec import DatasetSpec, GroupSpec, NamespaceCatalog, SpecReader
from ..spec.spec import BaseStorageSpec
from ..utils import docval, getargs, call_docval_func, ExtenderMeta


class Proxy:
    """
    A temporary object to represent a Container. This gets used when resolving the true location of a
    Container's parent.
    Proxy objects allow simple bookkeeping of all potential parents a Container may have.
    This object is used by providing all the necessary information for describing the object. This object
    gets passed around and candidates are accumulated. Upon calling resolve, all saved candidates are matched
    against the information (provided to the constructor). The candidate that has an exact match is returned.
    """

    def __init__(self, manager, source, location, namespace, data_type):
        self.__source = source
        self.__location = location
        self.__namespace = namespace
        self.__data_type = data_type
        self.__manager = manager
        self.__candidates = list()

    @property
    def source(self):
        """The source of the object e.g. file source"""
        return self.__source

    @property
    def location(self):
        """The location of the object. This can be thought of as a unique path"""
        return self.__location

    @property
    def namespace(self):
        """The namespace from which the data_type of this Proxy came from"""
        return self.__namespace

    @property
    def data_type(self):
        """The data_type of Container that should match this Proxy"""
        return self.__data_type

    @docval({"name": "object", "type": (BaseBuilder, Container), "doc": "the container or builder to get a proxy for"})
    def matches(self, **kwargs):
        obj = getargs('object', kwargs)
        if not isinstance(obj, Proxy):
            obj = self.__manager.get_proxy(obj)
        return self == obj

    @docval({"name": "container", "type": Container, "doc": "the Container to add as a candidate match"})
    def add_candidate(self, **kwargs):
        container = getargs('container', kwargs)
        self.__candidates.append(container)

    def resolve(self):
        for candidate in self.__candidates:
            if self.matches(candidate):
                return candidate
        raise ValueError("No matching candidate Container found for " + self)

    def __eq__(self, other):
        return self.data_type == other.data_type and \
               self.location == other.location and \
               self.namespace == other.namespace and \
               self.source == other.source

    def __repr__(self):
        ret = dict()
        for key in ('source', 'location', 'namespace', 'data_type'):
            ret[key] = getattr(self, key, None)
        return str(ret)


class BuildManager:
    """
    A class for managing builds of AbstractContainers
    """

    def __init__(self, type_map):
        self.logger = logging.getLogger('%s.%s' % (self.__class__.__module__, self.__class__.__qualname__))
        self.__builders = dict()
        self.__containers = dict()
        self.__active_builders = set()
        self.__type_map = type_map
        self.__ref_queue = deque()  # a queue of the ReferenceBuilders that need to be added

    @property
    def namespace_catalog(self):
        return self.__type_map.namespace_catalog

    @property
    def type_map(self):
        return self.__type_map

    @docval({"name": "object", "type": (BaseBuilder, AbstractContainer),
             "doc": "the container or builder to get a proxy for"},
            {"name": "source", "type": str,
             "doc": "the source of container being built i.e. file path", 'default': None})
    def get_proxy(self, **kwargs):
        obj = getargs('object', kwargs)
        if isinstance(obj, BaseBuilder):
            return self._get_proxy_builder(obj)
        elif isinstance(obj, AbstractContainer):
            return self._get_proxy_container(obj)

    def _get_proxy_builder(self, builder):
        dt = self.__type_map.get_builder_dt(builder)
        ns = self.__type_map.get_builder_ns(builder)
        stack = list()
        tmp = builder
        while tmp is not None:
            stack.append(tmp.name)
            tmp = self.__get_parent_dt_builder(tmp)
        loc = "/".join(reversed(stack))
        return Proxy(self, builder.source, loc, ns, dt)

    def _get_proxy_container(self, container):
        ns, dt = self.__type_map.get_container_ns_dt(container)
        stack = list()
        tmp = container
        while tmp is not None:
            if isinstance(tmp, Proxy):
                stack.append(tmp.location)
                break
            else:
                stack.append(tmp.name)
                tmp = tmp.parent
        loc = "/".join(reversed(stack))
        return Proxy(self, container.container_source, loc, ns, dt)

    @docval({"name": "container", "type": AbstractContainer, "doc": "the container to convert to a Builder"},
            {"name": "source", "type": str,
             "doc": "the source of container being built i.e. file path", 'default': None},
            {"name": "spec_ext", "type": BaseStorageSpec, "doc": "a spec that further refines the base specification",
             'default': None},
            {"name": "export", "type": bool, "doc": "whether this build is for exporting",
             'default': False},
            {"name": "root", "type": bool, "doc": "whether the container is the root of the build process",
             'default': False})
    def build(self, **kwargs):
        """ Build the GroupBuilder/DatasetBuilder for the given AbstractContainer"""
        container, export = getargs('container', 'export', kwargs)
        source, spec_ext, root = getargs('source', 'spec_ext', 'root', kwargs)
        result = self.get_builder(container)
        if root:
            self.__active_builders.clear()  # reset active builders at start of build process
        if result is None:
            self.logger.debug("Building new %s '%s' (container_source: %s, source: %s, extended spec: %s, export: %s)"
                              % (container.__class__.__name__, container.name, repr(container.container_source),
                                 repr(source), spec_ext is not None, export))
            # the container_source is not set or checked when exporting
            if not export:
                if container.container_source is None:
                    container.container_source = source
                elif source is None:
                    source = container.container_source
                else:
                    if container.container_source != source:
                        raise ValueError("Cannot change container_source once set: '%s' %s.%s"
                                         % (container.name, container.__class__.__module__,
                                            container.__class__.__name__))
            # NOTE: if exporting, then existing cached builder will be ignored and overridden with new build result
            result = self.__type_map.build(container, self, source=source, spec_ext=spec_ext, export=export)
            self.prebuilt(container, result)
            self.__active_prebuilt(result)
            self.logger.debug("Done building %s '%s'" % (container.__class__.__name__, container.name))
        elif not self.__is_active_builder(result) and container.modified:
            # if builder was built on file read and is then modified (append mode), it needs to be rebuilt
            self.logger.debug("Rebuilding modified %s '%s' (source: %s, extended spec: %s)"
                              % (container.__class__.__name__, container.name,
                                 repr(source), spec_ext is not None))
            result = self.__type_map.build(container, self, builder=result, source=source, spec_ext=spec_ext,
                                           export=export)
            self.logger.debug("Done rebuilding %s '%s'" % (container.__class__.__name__, container.name))
        else:
            self.logger.debug("Using prebuilt %s '%s' for %s '%s'"
                              % (result.__class__.__name__, result.name,
                                 container.__class__.__name__, container.name))
        if root:  # create reference builders only after building all other builders
            self.__add_refs()
            self.__active_builders.clear()  # reset active builders now that build process has completed
        return result

    @docval({"name": "container", "type": AbstractContainer, "doc": "the AbstractContainer to save as prebuilt"},
            {'name': 'builder', 'type': (DatasetBuilder, GroupBuilder),
             'doc': 'the Builder representation of the given container'})
    def prebuilt(self, **kwargs):
        ''' Save the Builder for a given AbstractContainer for future use '''
        container, builder = getargs('container', 'builder', kwargs)
        container_id = self.__conthash__(container)
        self.__builders[container_id] = builder
        builder_id = self.__bldrhash__(builder)
        self.__containers[builder_id] = container

    def __active_prebuilt(self, builder):
        """Save the Builder for future use during the active/current build process."""
        builder_id = self.__bldrhash__(builder)
        self.__active_builders.add(builder_id)

    def __is_active_builder(self, builder):
        """Return True if the Builder was created during the active/current build process."""
        builder_id = self.__bldrhash__(builder)
        return builder_id in self.__active_builders

    def __conthash__(self, obj):
        return id(obj)

    def __bldrhash__(self, obj):
        return id(obj)

    def __add_refs(self):
        '''
        Add ReferenceBuilders.

        References get queued to be added after all other objects are built. This is because
        the current traversal algorithm (i.e. iterating over specs)
        does not happen in a guaranteed order. We need to build the targets
        of the reference builders so that the targets have the proper parent,
        and then write the reference builders after we write everything else.
        '''
        while len(self.__ref_queue) > 0:
            call = self.__ref_queue.popleft()
            self.logger.debug("Adding ReferenceBuilder with call id %d from queue (length %d)"
                              % (id(call), len(self.__ref_queue)))
            call()

    def queue_ref(self, func):
        '''Set aside creating ReferenceBuilders'''
        # TODO: come up with more intelligent way of
        # queueing reference resolution, based on reference
        # dependency
        self.__ref_queue.append(func)

    def purge_outdated(self):
        containers_copy = self.__containers.copy()
        for container in containers_copy.values():
            if container.modified:
                container_id = self.__conthash__(container)
                builder = self.__builders.get(container_id)
                builder_id = self.__bldrhash__(builder)
                self.logger.debug("Purging %s '%s' for %s '%s' from prebuilt cache"
                                  % (builder.__class__.__name__, builder.name,
                                     container.__class__.__name__, container.name))
                self.__builders.pop(container_id)
                self.__containers.pop(builder_id)

    @docval({"name": "container", "type": AbstractContainer, "doc": "the container to get the builder for"})
    def get_builder(self, **kwargs):
        """Return the prebuilt builder for the given container or None if it does not exist."""
        container = getargs('container', kwargs)
        container_id = self.__conthash__(container)
        result = self.__builders.get(container_id)
        return result

    @docval({'name': 'builder', 'type': (DatasetBuilder, GroupBuilder),
             'doc': 'the builder to construct the AbstractContainer from'})
    def construct(self, **kwargs):
        """ Construct the AbstractContainer represented by the given builder """
        builder = getargs('builder', kwargs)
        if isinstance(builder, LinkBuilder):
            builder = builder.target
        builder_id = self.__bldrhash__(builder)
        result = self.__containers.get(builder_id)
        if result is None:
            parent_builder = self.__get_parent_dt_builder(builder)
            if parent_builder is not None:
                parent = self._get_proxy_builder(parent_builder)
                result = self.__type_map.construct(builder, self, parent)
            else:
                # we are at the top of the hierarchy,
                # so it must be time to resolve parents
                result = self.__type_map.construct(builder, self, None)
                self.__resolve_parents(result)
            self.prebuilt(result, builder)
        result.set_modified(False)
        return result

    def __resolve_parents(self, container):
        stack = [container]
        while len(stack) > 0:
            tmp = stack.pop()
            if isinstance(tmp.parent, Proxy):
                tmp.parent = tmp.parent.resolve()
            for child in tmp.children:
                stack.append(child)

    def __get_parent_dt_builder(self, builder):
        '''
        Get the next builder above the given builder
        that has a data_type
        '''
        tmp = builder.parent
        ret = None
        while tmp is not None:
            ret = tmp
            dt = self.__type_map.get_builder_dt(tmp)
            if dt is not None:
                break
            tmp = tmp.parent
        return ret

    # *** The following methods just delegate calls to self.__type_map ***

    @docval({'name': 'builder', 'type': Builder, 'doc': 'the Builder to get the class object for'})
    def get_cls(self, **kwargs):
        ''' Get the class object for the given Builder '''
        builder = getargs('builder', kwargs)
        return self.__type_map.get_cls(builder)

    @docval({"name": "container", "type": AbstractContainer, "doc": "the container to convert to a Builder"},
            returns='The name a Builder should be given when building this container', rtype=str)
    def get_builder_name(self, **kwargs):
        ''' Get the name a Builder should be given '''
        container = getargs('container', kwargs)
        return self.__type_map.get_builder_name(container)

    @docval({'name': 'spec', 'type': (DatasetSpec, GroupSpec), 'doc': 'the parent spec to search'},
            {'name': 'builder', 'type': (DatasetBuilder, GroupBuilder, LinkBuilder),
             'doc': 'the builder to get the sub-specification for'})
    def get_subspec(self, **kwargs):
        ''' Get the specification from this spec that corresponds to the given builder '''
        spec, builder = getargs('spec', 'builder', kwargs)
        return self.__type_map.get_subspec(spec, builder)

    @docval({'name': 'builder', 'type': (DatasetBuilder, GroupBuilder, LinkBuilder),
             'doc': 'the builder to get the sub-specification for'})
    def get_builder_ns(self, **kwargs):
        ''' Get the namespace of a builder '''
        builder = getargs('builder', kwargs)
        return self.__type_map.get_builder_ns(builder)

    @docval({'name': 'builder', 'type': (DatasetBuilder, GroupBuilder, LinkBuilder),
             'doc': 'the builder to get the data_type for'})
    def get_builder_dt(self, **kwargs):
        '''
        Get the data_type of a builder
        '''
        builder = getargs('builder', kwargs)
        return self.__type_map.get_builder_dt(builder)

    @docval({'name': 'builder', 'type': (GroupBuilder, DatasetBuilder, AbstractContainer),
             'doc': 'the builder or container to check'},
            {'name': 'parent_data_type', 'type': str,
             'doc': 'the potential parent data_type that refers to a data_type'},
            returns="True if data_type of *builder* is a sub-data_type of *parent_data_type*, False otherwise",
            rtype=bool)
    def is_sub_data_type(self, **kwargs):
        '''
        Return whether or not data_type of *builder* is a sub-data_type of *parent_data_type*
        '''
        builder, parent_dt = getargs('builder', 'parent_data_type', kwargs)
        if isinstance(builder, (GroupBuilder, DatasetBuilder)):
            ns = self.get_builder_ns(builder)
            dt = self.get_builder_dt(builder)
        else:  # builder is an AbstractContainer
            ns, dt = self.type_map.get_container_ns_dt(builder)
        return self.namespace_catalog.is_sub_data_type(ns, dt, parent_dt)


class TypeSource:
    '''A class to indicate the source of a data_type in a namespace.
    This class should only be used by TypeMap
    '''

    @docval({"name": "namespace", "type": str, "doc": "the namespace the from, which the data_type originated"},
            {"name": "data_type", "type": str, "doc": "the name of the type"})
    def __init__(self, **kwargs):
        namespace, data_type = getargs('namespace', 'data_type', kwargs)
        self.__namespace = namespace
        self.__data_type = data_type

    @property
    def namespace(self):
        return self.__namespace

    @property
    def data_type(self):
        return self.__data_type


class TypeMap:
    ''' A class to maintain the map between ObjectMappers and AbstractContainer classes
    '''

    @docval({'name': 'namespaces', 'type': NamespaceCatalog, 'doc': 'the NamespaceCatalog to use', 'default': None},
            {'name': 'mapper_cls', 'type': type, 'doc': 'the ObjectMapper class to use', 'default': None})
    def __init__(self, **kwargs):
        namespaces, mapper_cls = getargs('namespaces', 'mapper_cls', kwargs)
        if namespaces is None:
            namespaces = NamespaceCatalog()
        if mapper_cls is None:
            from .objectmapper import ObjectMapper  # avoid circular import
            mapper_cls = ObjectMapper
        self.__ns_catalog = namespaces
        self.__mappers = dict()  # already constructed ObjectMapper classes
        self.__mapper_cls = dict()  # the ObjectMapper class to use for each container type
        self.__container_types = OrderedDict()
        self.__data_types = dict()
        self.__default_mapper_cls = mapper_cls
        self.__class_generator = ClassGenerator()
        self.register_generator(CustomClassGenerator)
        self.register_generator(MCIClassGenerator)

    @property
    def namespace_catalog(self):
        return self.__ns_catalog

    @property
    def container_types(self):
        return self.__container_types

    def __copy__(self):
        ret = TypeMap(copy(self.__ns_catalog), self.__default_mapper_cls)
        ret.merge(self)
        return ret

    def __deepcopy__(self, memo):
        # XXX: From @nicain: All of a sudden legacy tests started
        #      needing this argument in deepcopy. Doesn't hurt anything, though.
        return self.__copy__()

    def copy_mappers(self, type_map):
        for namespace in self.__ns_catalog.namespaces:
            if namespace not in type_map.__container_types:
                continue
            for data_type in self.__ns_catalog.get_namespace(namespace).get_registered_types():
                container_cls = type_map.__container_types[namespace].get(data_type)
                if container_cls is None:
                    continue
                self.register_container_type(namespace, data_type, container_cls)
                if container_cls in type_map.__mapper_cls:
                    self.register_map(container_cls, type_map.__mapper_cls[container_cls])

    def merge(self, type_map, ns_catalog=False):
        if ns_catalog:
            self.namespace_catalog.merge(type_map.namespace_catalog)
        for namespace in type_map.__container_types:
            for data_type in type_map.__container_types[namespace]:
                container_cls = type_map.__container_types[namespace][data_type]
                self.register_container_type(namespace, data_type, container_cls)
        for container_cls in type_map.__mapper_cls:
            self.register_map(container_cls, type_map.__mapper_cls[container_cls])
        for custom_generators in reversed(type_map.__class_generator.custom_generators):
            # iterate in reverse order because generators are stored internally as a stack
            self.register_generator(custom_generators)

    @docval({"name": "generator", "type": type, "doc": "the CustomClassGenerator class to register"})
    def register_generator(self, **kwargs):
        """Add a custom class generator."""
        generator = getargs('generator', kwargs)
        self.__class_generator.register_generator(generator)

    @docval({'name': 'namespace_path', 'type': str, 'doc': 'the path to the file containing the namespaces(s) to load'},
            {'name': 'resolve', 'type': bool,
             'doc': 'whether or not to include objects from included/parent spec objects', 'default': True},
            {'name': 'reader',
             'type': SpecReader,
             'doc': 'the class to user for reading specifications', 'default': None},
            returns="the namespaces loaded from the given file", rtype=dict)
    def load_namespaces(self, **kwargs):
        '''Load namespaces from a namespace file.
        This method will call load_namespaces on the NamespaceCatalog used to construct this TypeMap. Additionally,
        it will process the return value to keep track of what types were included in the loaded namespaces. Calling
        load_namespaces here has the advantage of being able to keep track of type dependencies across namespaces.
        '''
        deps = call_docval_func(self.__ns_catalog.load_namespaces, kwargs)
        for new_ns, ns_deps in deps.items():
            for src_ns, types in ns_deps.items():
                for dt in types:
                    container_cls = self.get_dt_container_cls(dt, src_ns, autogen=False)
                    if container_cls is None:
                        container_cls = TypeSource(src_ns, dt)
                    self.register_container_type(new_ns, dt, container_cls)
        return deps

    @docval({"name": "namespace", "type": str, "doc": "the namespace containing the data_type"},
            {"name": "data_type", "type": str, "doc": "the data type to create a AbstractContainer class for"},
            {"name": "autogen", "type": bool, "doc": "autogenerate class if one does not exist", "default": True},
            returns='the class for the given namespace and data_type', rtype=type)
    def get_container_cls(self, **kwargs):
        """Get the container class from data type specification.
        If no class has been associated with the ``data_type`` from ``namespace``, a class will be dynamically
        created and returned.
        """
        # NOTE: this internally used function get_container_cls will be removed in favor of get_dt_container_cls
        namespace, data_type, autogen = getargs('namespace', 'data_type', 'autogen', kwargs)
        return self.get_dt_container_cls(data_type, namespace, autogen)

    @docval({"name": "data_type", "type": str, "doc": "the data type to create a AbstractContainer class for"},
            {"name": "namespace", "type": str, "doc": "the namespace containing the data_type", "default": None},
            {"name": "autogen", "type": bool, "doc": "autogenerate class if one does not exist", "default": True},
            returns='the class for the given namespace and data_type', rtype=type)
    def get_dt_container_cls(self, **kwargs):
        """Get the container class from data type specification.
        If no class has been associated with the ``data_type`` from ``namespace``, a class will be dynamically
        created and returned.

        Replaces get_container_cls but namespace is optional. If namespace is unknown, it will be looked up from
        all namespaces.
        """
        namespace, data_type, autogen = getargs('namespace', 'data_type', 'autogen', kwargs)

        # namespace is unknown, so look it up
        if namespace is None:
            for ns_key, ns_data_types in self.__container_types.items():
                # NOTE that the type_name may appear in multiple namespaces based on how they were resolved
                # but the same type_name should point to the same class
                if data_type in ns_data_types:
                    namespace = ns_key
                    break

        cls = self.__get_container_cls(namespace, data_type)
        if cls is None and autogen:  # dynamically generate a class
            spec = self.__ns_catalog.get_spec(namespace, data_type)
            self.__check_dependent_types(spec, namespace)
            parent_cls = self.__get_parent_cls(namespace, data_type, spec)
            attr_names = self.__default_mapper_cls.get_attr_names(spec)
            cls = self.__class_generator.generate_class(data_type, spec, parent_cls, attr_names, self)
            self.register_container_type(namespace, data_type, cls)
        return cls

    def __check_dependent_types(self, spec, namespace):
        """Ensure that classes for all types used by this type exist in this namespace and generate them if not.
        """
        def __check_dependent_types_helper(spec, namespace):
            if isinstance(spec, (GroupSpec, DatasetSpec)):
                if spec.data_type_inc is not None:
                    self.get_dt_container_cls(spec.data_type_inc, namespace)  # TODO handle recursive definitions
                if spec.data_type_def is not None:  # nested type definition
                    self.get_dt_container_cls(spec.data_type_def, namespace)
            else:  # spec is a LinkSpec
                self.get_dt_container_cls(spec.target_type, namespace)
            if isinstance(spec, GroupSpec):
                for child_spec in (spec.groups + spec.datasets + spec.links):
                    __check_dependent_types_helper(child_spec, namespace)

        if spec.data_type_inc is not None:
            self.get_dt_container_cls(spec.data_type_inc, namespace)
        if isinstance(spec, GroupSpec):
            for child_spec in (spec.groups + spec.datasets + spec.links):
                __check_dependent_types_helper(child_spec, namespace)

    def __get_parent_cls(self, namespace, data_type, spec):
        dt_hier = self.__ns_catalog.get_hierarchy(namespace, data_type)
        dt_hier = dt_hier[1:]  # remove the current data_type
        parent_cls = None
        for t in dt_hier:
            parent_cls = self.__get_container_cls(namespace, t)
            if parent_cls is not None:
                break
        if parent_cls is None:
            if isinstance(spec, GroupSpec):
                parent_cls = Container
            elif isinstance(spec, DatasetSpec):
                parent_cls = Data
            else:
                raise ValueError("Cannot generate class from %s" % type(spec))
        if type(parent_cls) is not ExtenderMeta:
            raise ValueError("parent class %s is not of type ExtenderMeta - %s" % (parent_cls, type(parent_cls)))
        return parent_cls

    def __get_container_cls(self, namespace, data_type):
        """Get the container class for the namespace, data_type. If the class doesn't exist yet, generate it."""
        if namespace not in self.__container_types:
            return None
        if data_type not in self.__container_types[namespace]:
            return None
        ret = self.__container_types[namespace][data_type]
        if isinstance(ret, TypeSource):  # data_type is a dependency from ret.namespace
            cls = self.get_dt_container_cls(ret.data_type, ret.namespace)  # get class / generate class
            # register the same class into this namespace (replaces TypeSource)
            self.register_container_type(namespace, data_type, cls)
            ret = cls
        return ret

    @docval({'name': 'obj', 'type': (GroupBuilder, DatasetBuilder, LinkBuilder, GroupSpec, DatasetSpec),
             'doc': 'the object to get the type key for'})
    def __type_key(self, obj):
        """
        A wrapper function to simplify the process of getting a type_key for an object.
        The type_key is used to get the data_type from a Builder's attributes.
        """
        if isinstance(obj, LinkBuilder):
            obj = obj.builder
        if isinstance(obj, (GroupBuilder, GroupSpec)):
            return self.__ns_catalog.group_spec_cls.type_key()
        else:
            return self.__ns_catalog.dataset_spec_cls.type_key()

    @docval({'name': 'builder', 'type': (DatasetBuilder, GroupBuilder, LinkBuilder),
             'doc': 'the builder to get the data_type for'})
    def get_builder_dt(self, **kwargs):
        '''
        Get the data_type of a builder
        '''
        builder = getargs('builder', kwargs)
        ret = None
        if isinstance(builder, LinkBuilder):
            builder = builder.builder
        if isinstance(builder, GroupBuilder):
            ret = builder.attributes.get(self.__ns_catalog.group_spec_cls.type_key())
        else:
            ret = builder.attributes.get(self.__ns_catalog.dataset_spec_cls.type_key())
        if isinstance(ret, bytes):
            ret = ret.decode('UTF-8')
        return ret

    @docval({'name': 'builder', 'type': (DatasetBuilder, GroupBuilder, LinkBuilder),
             'doc': 'the builder to get the sub-specification for'})
    def get_builder_ns(self, **kwargs):
        ''' Get the namespace of a builder '''
        builder = getargs('builder', kwargs)
        if isinstance(builder, LinkBuilder):
            builder = builder.builder
        ret = builder.attributes.get('namespace')
        return ret

    @docval({'name': 'builder', 'type': Builder,
             'doc': 'the Builder object to get the corresponding AbstractContainer class for'})
    def get_cls(self, **kwargs):
        ''' Get the class object for the given Builder '''
        builder = getargs('builder', kwargs)
        data_type = self.get_builder_dt(builder)
        if data_type is None:
            raise ValueError("No data_type found for builder %s" % builder.path)
        namespace = self.get_builder_ns(builder)
        if namespace is None:
            raise ValueError("No namespace found for builder %s" % builder.path)
        return self.get_dt_container_cls(data_type, namespace)

    @docval({'name': 'spec', 'type': (DatasetSpec, GroupSpec), 'doc': 'the parent spec to search'},
            {'name': 'builder', 'type': (DatasetBuilder, GroupBuilder, LinkBuilder),
             'doc': 'the builder to get the sub-specification for'})
    def get_subspec(self, **kwargs):
        ''' Get the specification from this spec that corresponds to the given builder '''
        spec, builder = getargs('spec', 'builder', kwargs)
        if isinstance(builder, LinkBuilder):
            builder_type = type(builder.builder)
        else:
            builder_type = type(builder)
        if issubclass(builder_type, DatasetBuilder):
            subspec = spec.get_dataset(builder.name)
        else:
            subspec = spec.get_group(builder.name)
        if subspec is None:
            # builder was generated from something with a data_type and a wildcard name
            if isinstance(builder, LinkBuilder):
                dt = self.get_builder_dt(builder.builder)
            else:
                dt = self.get_builder_dt(builder)
            if dt is not None:
                ns = self.get_builder_ns(builder)
                hierarchy = self.__ns_catalog.get_hierarchy(ns, dt)
                for t in hierarchy:
                    subspec = spec.get_data_type(t)
                    if subspec is not None:
                        break
        return subspec

    def get_container_ns_dt(self, obj):
        container_cls = obj.__class__
        namespace, data_type = self.get_container_cls_dt(container_cls)
        return namespace, data_type

    def get_container_cls_dt(self, cls):
        def_ret = (None, None)
        for _cls in cls.__mro__:
            ret = self.__data_types.get(_cls, def_ret)
            if ret is not def_ret:
                return ret
        return ret

    @docval({'name': 'namespace', 'type': str,
             'doc': 'the namespace to get the container classes for', 'default': None})
    def get_container_classes(self, **kwargs):
        namespace = getargs('namespace', kwargs)
        ret = self.__data_types.keys()
        if namespace is not None:
            ret = filter(lambda x: self.__data_types[x][0] == namespace, ret)
        return list(ret)

    @docval({'name': 'obj', 'type': (AbstractContainer, Builder), 'doc': 'the object to get the ObjectMapper for'},
            returns='the ObjectMapper to use for mapping the given object', rtype='ObjectMapper')
    def get_map(self, **kwargs):
        """ Return the ObjectMapper object that should be used for the given container """
        obj = getargs('obj', kwargs)
        # get the container class, and namespace/data_type
        if isinstance(obj, AbstractContainer):
            container_cls = obj.__class__
            namespace, data_type = self.get_container_cls_dt(container_cls)
            if namespace is None:
                raise ValueError("class %s is not mapped to a data_type" % container_cls)
        else:
            data_type = self.get_builder_dt(obj)
            namespace = self.get_builder_ns(obj)
            container_cls = self.get_cls(obj)
        # now build the ObjectMapper class
        mapper = self.__mappers.get(container_cls)
        if mapper is None:
            mapper_cls = self.__default_mapper_cls
            for cls in container_cls.__mro__:
                tmp_mapper_cls = self.__mapper_cls.get(cls)
                if tmp_mapper_cls is not None:
                    mapper_cls = tmp_mapper_cls
                    break
            spec = self.__ns_catalog.get_spec(namespace, data_type)
            mapper = mapper_cls(spec)
            self.__mappers[container_cls] = mapper
        return mapper

    @docval({"name": "namespace", "type": str, "doc": "the namespace containing the data_type to map the class to"},
            {"name": "data_type", "type": str, "doc": "the data_type to map the class to"},
            {"name": "container_cls", "type": (TypeSource, type), "doc": "the class to map to the specified data_type"})
    def register_container_type(self, **kwargs):
        ''' Map a container class to a data_type '''
        namespace, data_type, container_cls = getargs('namespace', 'data_type', 'container_cls', kwargs)
        spec = self.__ns_catalog.get_spec(namespace, data_type)  # make sure the spec exists
        self.__container_types.setdefault(namespace, dict())
        self.__container_types[namespace][data_type] = container_cls
        self.__data_types.setdefault(container_cls, (namespace, data_type))
        if not isinstance(container_cls, TypeSource):
            setattr(container_cls, spec.type_key(), data_type)
            setattr(container_cls, 'namespace', namespace)

    @docval({"name": "container_cls", "type": type,
             "doc": "the AbstractContainer class for which the given ObjectMapper class gets used for"},
            {"name": "mapper_cls", "type": type, "doc": "the ObjectMapper class to use to map"})
    def register_map(self, **kwargs):
        ''' Map a container class to an ObjectMapper class '''
        container_cls, mapper_cls = getargs('container_cls', 'mapper_cls', kwargs)
        if self.get_container_cls_dt(container_cls) == (None, None):
            raise ValueError('cannot register map for type %s - no data_type found' % container_cls)
        self.__mapper_cls[container_cls] = mapper_cls

    @docval({"name": "container", "type": AbstractContainer, "doc": "the container to convert to a Builder"},
            {"name": "manager", "type": BuildManager,
             "doc": "the BuildManager to use for managing this build", 'default': None},
            {"name": "source", "type": str,
             "doc": "the source of container being built i.e. file path", 'default': None},
            {"name": "builder", "type": BaseBuilder, "doc": "the Builder to build on", 'default': None},
            {"name": "spec_ext", "type": BaseStorageSpec, "doc": "a spec extension", 'default': None},
            {"name": "export", "type": bool, "doc": "whether this build is for exporting",
             'default': False})
    def build(self, **kwargs):
        """Build the GroupBuilder/DatasetBuilder for the given AbstractContainer"""
        container, manager, builder = getargs('container', 'manager', 'builder', kwargs)
        source, spec_ext, export = getargs('source', 'spec_ext', 'export', kwargs)

        # get the ObjectMapper to map between Spec objects and AbstractContainer attributes
        obj_mapper = self.get_map(container)
        if obj_mapper is None:
            raise ValueError('No ObjectMapper found for container of type %s' % str(container.__class__.__name__))

        # convert the container to a builder using the ObjectMapper
        if manager is None:
            manager = BuildManager(self)
        builder = obj_mapper.build(container, manager, builder=builder, source=source, spec_ext=spec_ext, export=export)

        # add additional attributes (namespace, data_type, object_id) to builder
        namespace, data_type = self.get_container_ns_dt(container)
        builder.set_attribute('namespace', namespace)
        builder.set_attribute(self.__type_key(obj_mapper.spec), data_type)
        builder.set_attribute(obj_mapper.spec.id_key(), container.object_id)
        return builder

    @docval({'name': 'builder', 'type': (DatasetBuilder, GroupBuilder),
             'doc': 'the builder to construct the AbstractContainer from'},
            {'name': 'build_manager', 'type': BuildManager,
             'doc': 'the BuildManager for constructing', 'default': None},
            {'name': 'parent', 'type': (Proxy, Container),
             'doc': 'the parent Container/Proxy for the Container being built', 'default': None})
    def construct(self, **kwargs):
        """ Construct the AbstractContainer represented by the given builder """
        builder, build_manager, parent = getargs('builder', 'build_manager', 'parent', kwargs)
        if build_manager is None:
            build_manager = BuildManager(self)
        obj_mapper = self.get_map(builder)
        if obj_mapper is None:
            dt = builder.attributes[self.namespace_catalog.group_spec_cls.type_key()]
            raise ValueError('No ObjectMapper found for builder of type %s' % dt)
        else:
            return obj_mapper.construct(builder, build_manager, parent)

    @docval({"name": "container", "type": AbstractContainer, "doc": "the container to convert to a Builder"},
            returns='The name a Builder should be given when building this container', rtype=str)
    def get_builder_name(self, **kwargs):
        ''' Get the name a Builder should be given '''
        container = getargs('container', kwargs)
        obj_mapper = self.get_map(container)
        if obj_mapper is None:
            raise ValueError('No ObjectMapper found for container of type %s' % str(container.__class__.__name__))
        else:
            return obj_mapper.get_builder_name(container)
