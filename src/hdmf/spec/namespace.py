import os.path
import ruamel.yaml as yaml
import string
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from copy import copy
from datetime import datetime
from warnings import warn

from .catalog import SpecCatalog
from .spec import DatasetSpec, GroupSpec
from ..utils import docval, getargs, popargs, get_docval, call_docval_func

_namespace_args = [
    {'name': 'doc', 'type': str, 'doc': 'a description about what this namespace represents'},
    {'name': 'name', 'type': str, 'doc': 'the name of this namespace'},
    {'name': 'schema', 'type': list, 'doc': 'location of schema specification files or other Namespaces'},
    {'name': 'full_name', 'type': str, 'doc': 'extended full name of this namespace', 'default': None},
    {'name': 'version', 'type': (str, tuple, list), 'doc': 'Version number of the namespace', 'default': None},
    {'name': 'date', 'type': (datetime, str),
     'doc': "Date last modified or released. Formatting is %Y-%m-%d %H:%M:%S, e.g, 2017-04-25 17:14:13",
     'default': None},
    {'name': 'author', 'type': (str, list), 'doc': 'Author or list of authors.', 'default': None},
    {'name': 'contact', 'type': (str, list),
     'doc': 'List of emails. Ordering should be the same as for author', 'default': None},
    {'name': 'catalog', 'type': SpecCatalog, 'doc': 'The SpecCatalog object for this SpecNamespace', 'default': None}
]


class SpecNamespace(dict):
    """
    A namespace for specifications
    """

    __types_key = 'data_types'

    UNVERSIONED = None  # value representing missing version

    @docval(*_namespace_args)
    def __init__(self, **kwargs):
        doc, full_name, name, version, date, author, contact, schema, catalog = \
            popargs('doc', 'full_name', 'name', 'version', 'date', 'author', 'contact', 'schema', 'catalog', kwargs)
        super().__init__()
        self['doc'] = doc
        self['schema'] = schema
        if any(c in string.whitespace for c in name):
            raise ValueError("'name' must not contain any whitespace")
        self['name'] = name
        if full_name is not None:
            self['full_name'] = full_name
        if version == str(SpecNamespace.UNVERSIONED):
            # the unversioned version may be written to file as a string and read from file as a string
            warn("Loaded namespace '%s' is unversioned. Please notify the extension author." % name)
            version = SpecNamespace.UNVERSIONED
        if version is None:
            # version is required on write -- see YAMLSpecWriter.write_namespace -- but can be None on read in order to
            # be able to read older files with extensions that are missing the version key.
            warn(("Loaded namespace '%s' is missing the required key 'version'. Version will be set to '%s'. "
                  "Please notify the extension author.") % (name, SpecNamespace.UNVERSIONED))
            version = SpecNamespace.UNVERSIONED
        self['version'] = version
        if date is not None:
            self['date'] = date
        if author is not None:
            self['author'] = author
        if contact is not None:
            self['contact'] = contact
        self.__catalog = catalog if catalog is not None else SpecCatalog()

    @classmethod
    def types_key(cls):
        ''' Get the key used for specifying types to include from a file or namespace

        Override this method to use a different name for 'data_types'
        '''
        return cls.__types_key

    @property
    def full_name(self):
        """String with full name or None"""
        return self.get('full_name', None)

    @property
    def contact(self):
        """String or list of strings with the contacts or None"""
        return self.get('contact', None)

    @property
    def author(self):
        """String or list of strings with the authors or None"""
        return self.get('author', None)

    @property
    def version(self):
        """
        String, list, or tuple with the version or SpecNamespace.UNVERSIONED
        if the version is missing or empty
        """
        return self.get('version', None) or SpecNamespace.UNVERSIONED

    @property
    def date(self):
        """Date last modified or released.

        :return: datetime object, string, or None"""
        return self.get('date', None)

    @property
    def name(self):
        """String with short name or None"""
        return self.get('name', None)

    @property
    def doc(self):
        return self['doc']

    @property
    def schema(self):
        return self['schema']

    def get_source_files(self):
        """
        Get the list of names of the source files included the schema of the namespace
        """
        return [item['source'] for item in self.schema if 'source' in item]

    @docval({'name': 'sourcefile', 'type': str, 'doc': 'Name of the source file'},
            returns='Dict with the source file documentation', rtype=dict)
    def get_source_description(self, sourcefile):
        """
        Get the description of a source file as described in the namespace. The result is a
        dict which contains the 'source' and optionally 'title', 'doc' and 'data_types'
        imported from the source file
        """
        for item in self.schema:
            if item.get('source', None) == sourcefile:
                return item

    @property
    def catalog(self):
        """The SpecCatalog containing all the Specs"""
        return self.__catalog

    @docval({'name': 'data_type', 'type': (str, type), 'doc': 'the data_type to get the spec for'})
    def get_spec(self, **kwargs):
        """Get the Spec object for the given data type"""
        data_type = getargs('data_type', kwargs)
        spec = self.__catalog.get_spec(data_type)
        if spec is None:
            raise ValueError("No specification for '%s' in namespace '%s'" % (data_type, self.name))
        return spec

    @docval(returns="the a tuple of the available data types", rtype=tuple)
    def get_registered_types(self, **kwargs):
        """Get the available types in this namespace"""
        return self.__catalog.get_registered_types()

    @docval({'name': 'data_type', 'type': (str, type), 'doc': 'the data_type to get the hierarchy of'},
            returns="a tuple with the type hierarchy", rtype=tuple)
    def get_hierarchy(self, **kwargs):
        ''' Get the extension hierarchy for the given data_type in this namespace'''
        data_type = getargs('data_type', kwargs)
        return self.__catalog.get_hierarchy(data_type)

    @classmethod
    def build_namespace(cls, **spec_dict):
        kwargs = copy(spec_dict)
        try:
            args = [kwargs.pop(x['name']) for x in get_docval(cls.__init__) if 'default' not in x]
        except KeyError as e:
            raise KeyError("'%s' not found in %s" % (e.args[0], str(spec_dict)))
        return cls(*args, **kwargs)


class SpecReader(metaclass=ABCMeta):

    @docval({'name': 'source', 'type': str, 'doc': 'the source from which this reader reads from'})
    def __init__(self, **kwargs):
        self.__source = getargs('source', kwargs)

    @property
    def source(self):
        return self.__source

    @abstractmethod
    def read_spec(self):
        pass

    @abstractmethod
    def read_namespace(self):
        pass


class YAMLSpecReader(SpecReader):

    @docval({'name': 'indir', 'type': str, 'doc': 'the path spec files are relative to', 'default': '.'})
    def __init__(self, **kwargs):
        super_kwargs = {'source': kwargs['indir']}
        call_docval_func(super().__init__, super_kwargs)

    def read_namespace(self, namespace_path):
        namespaces = None
        with open(namespace_path, 'r') as stream:
            yaml_obj = yaml.YAML(typ='safe', pure=True)
            d = yaml_obj.load(stream)
            namespaces = d.get('namespaces')
            if namespaces is None:
                raise ValueError("no 'namespaces' found in %s" % namespace_path)
        return namespaces

    def read_spec(self, spec_path):
        specs = None
        with open(self.__get_spec_path(spec_path), 'r') as stream:
            yaml_obj = yaml.YAML(typ='safe', pure=True)
            specs = yaml_obj.load(stream)
            if not ('datasets' in specs or 'groups' in specs):
                raise ValueError("no 'groups' or 'datasets' found in %s" % spec_path)
        return specs

    def __get_spec_path(self, spec_path):
        if os.path.isabs(spec_path):
            return spec_path
        return os.path.join(self.source, spec_path)


class NamespaceCatalog:

    @docval({'name': 'group_spec_cls', 'type': type,
             'doc': 'the class to use for group specifications', 'default': GroupSpec},
            {'name': 'dataset_spec_cls', 'type': type,
             'doc': 'the class to use for dataset specifications', 'default': DatasetSpec},
            {'name': 'spec_namespace_cls', 'type': type,
             'doc': 'the class to use for specification namespaces', 'default': SpecNamespace})
    def __init__(self, **kwargs):
        """Create a catalog for storing  multiple Namespaces"""
        self.__namespaces = OrderedDict()
        self.__dataset_spec_cls = getargs('dataset_spec_cls', kwargs)
        self.__group_spec_cls = getargs('group_spec_cls', kwargs)
        self.__spec_namespace_cls = getargs('spec_namespace_cls', kwargs)
        # keep track of all spec objects ever loaded, so we don't have
        # multiple object instances of a spec
        self.__loaded_specs = dict()
        self.__included_specs = dict()
        self.__included_sources = dict()

        self._loaded_specs = self.__loaded_specs

    def __copy__(self):
        ret = NamespaceCatalog(self.__group_spec_cls,
                               self.__dataset_spec_cls,
                               self.__spec_namespace_cls)
        ret.__namespaces = copy(self.__namespaces)
        ret.__loaded_specs = copy(self.__loaded_specs)
        ret.__included_specs = copy(self.__included_specs)
        ret.__included_sources = copy(self.__included_sources)
        return ret

    def merge(self, ns_catalog):
        for name, namespace in ns_catalog.__namespaces.items():
            self.add_namespace(name, namespace)

    @property
    @docval(returns='a tuple of the available namespaces', rtype=tuple)
    def namespaces(self):
        """The namespaces in this NamespaceCatalog"""
        return tuple(self.__namespaces.keys())

    @property
    def dataset_spec_cls(self):
        """The DatasetSpec class used in this NamespaceCatalog"""
        return self.__dataset_spec_cls

    @property
    def group_spec_cls(self):
        """The GroupSpec class used in this NamespaceCatalog"""
        return self.__group_spec_cls

    @property
    def spec_namespace_cls(self):
        """The SpecNamespace class used in this NamespaceCatalog"""
        return self.__spec_namespace_cls

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this namespace'},
            {'name': 'namespace', 'type': SpecNamespace, 'doc': 'the SpecNamespace object'})
    def add_namespace(self, **kwargs):
        """Add a namespace to this catalog"""
        name, namespace = getargs('name', 'namespace', kwargs)
        if name in self.__namespaces:
            raise KeyError("namespace '%s' already exists" % name)
        self.__namespaces[name] = namespace
        for dt in namespace.catalog.get_registered_types():
            source = namespace.catalog.get_spec_source_file(dt)
            # do not add types that have already been loaded
            # use dict with None values as ordered set because order of specs does matter
            self.__loaded_specs.setdefault(source, dict()).update({dt: None})

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this namespace'},
            returns="the SpecNamespace with the given name", rtype=SpecNamespace)
    def get_namespace(self, **kwargs):
        """Get the a SpecNamespace"""
        name = getargs('name', kwargs)
        ret = self.__namespaces.get(name)
        if ret is None:
            raise KeyError("'%s' not a namespace" % name)
        return ret

    @docval({'name': 'namespace', 'type': str, 'doc': 'the name of the namespace'},
            {'name': 'data_type', 'type': (str, type), 'doc': 'the data_type to get the spec for'},
            returns="the specification for writing the given object type to HDF5 ", rtype='Spec')
    def get_spec(self, **kwargs):
        '''
        Get the Spec object for the given type from the given Namespace
        '''
        namespace, data_type = getargs('namespace', 'data_type', kwargs)
        if namespace not in self.__namespaces:
            raise KeyError("'%s' not a namespace" % namespace)
        return self.__namespaces[namespace].get_spec(data_type)

    @docval({'name': 'namespace', 'type': str, 'doc': 'the name of the namespace'},
            {'name': 'data_type', 'type': (str, type), 'doc': 'the data_type to get the spec for'},
            returns="a tuple with the type hierarchy", rtype=tuple)
    def get_hierarchy(self, **kwargs):
        '''
        Get the type hierarchy for a given data_type in a given namespace
        '''
        namespace, data_type = getargs('namespace', 'data_type', kwargs)
        spec_ns = self.__namespaces.get(namespace)
        if spec_ns is None:
            raise KeyError("'%s' not a namespace" % namespace)
        return spec_ns.get_hierarchy(data_type)

    @docval({'name': 'namespace', 'type': str, 'doc': 'the name of the namespace containing the data_type'},
            {'name': 'data_type', 'type': str, 'doc': 'the data_type to check'},
            {'name': 'parent_data_type', 'type': str, 'doc': 'the potential parent data_type'},
            returns="True if *data_type* is a sub `data_type` of *parent_data_type*, False otherwise", rtype=bool)
    def is_sub_data_type(self, **kwargs):
        '''
        Return whether or not *data_type* is a sub `data_type` of *parent_data_type*
        '''
        ns, dt, parent_dt = getargs('namespace', 'data_type', 'parent_data_type', kwargs)
        hier = self.get_hierarchy(ns, dt)
        return parent_dt in hier

    @docval(rtype=tuple)
    def get_sources(self, **kwargs):
        '''
        Get all the source specification files that were loaded in this catalog
        '''
        return tuple(self.__loaded_specs.keys())

    @docval({'name': 'namespace', 'type': str, 'doc': 'the name of the namespace'},
            rtype=tuple)
    def get_namespace_sources(self, **kwargs):
        '''
        Get all the source specifications that were loaded for a given namespace
        '''
        namespace = getargs('namespace', kwargs)
        return tuple(self.__included_sources[namespace])

    @docval({'name': 'source', 'type': str, 'doc': 'the name of the source'},
            rtype=tuple)
    def get_types(self, **kwargs):
        '''
        Get the types that were loaded from a given source
        '''
        source = getargs('source', kwargs)
        ret = self.__loaded_specs.get(source)
        if ret is not None:
            ret = tuple(ret)
        else:
            ret = tuple()
        return ret

    def __load_spec_file(self, reader, spec_source, catalog, types_to_load=None, resolve=True):
        ret = self.__loaded_specs.get(spec_source)
        if ret is not None:
            raise ValueError("spec source '%s' already loaded" % spec_source)

        def __reg_spec(spec_cls, spec_dict):
            dt_def = spec_dict.get(spec_cls.def_key())
            if dt_def is None:
                msg = 'No data type def key found in spec %s' % spec_source
                raise ValueError(msg)
            if types_to_load and dt_def not in types_to_load:
                return
            if resolve:
                self.__resolve_includes(spec_cls, spec_dict, catalog)
            spec_obj = spec_cls.build_spec(spec_dict)
            return catalog.auto_register(spec_obj, spec_source)

        if ret is None:
            ret = dict()  # this is used as an ordered set -- values are all none
            d = reader.read_spec(spec_source)
            specs = d.get('datasets', list())
            for spec_dict in specs:
                self.__convert_spec_cls_keys(GroupSpec, self.__group_spec_cls, spec_dict)
                temp_dict = {k: None for k in __reg_spec(self.__dataset_spec_cls, spec_dict)}
                ret.update(temp_dict)
            specs = d.get('groups', list())
            for spec_dict in specs:
                self.__convert_spec_cls_keys(GroupSpec, self.__group_spec_cls, spec_dict)
                temp_dict = {k: None for k in __reg_spec(self.__group_spec_cls, spec_dict)}
                ret.update(temp_dict)
            self.__loaded_specs[spec_source] = ret
        return ret

    def __convert_spec_cls_keys(self, parent_cls, spec_cls, spec_dict):
        """Replace instances of data_type_def/inc in spec_dict with new values from spec_cls."""
        # this is necessary because the def_key and inc_key may be different in each namespace
        # NOTE: this does not handle more than one custom set of keys
        if parent_cls.def_key() in spec_dict:
            spec_dict[spec_cls.def_key()] = spec_dict.pop(parent_cls.def_key())
        if parent_cls.inc_key() in spec_dict:
            spec_dict[spec_cls.inc_key()] = spec_dict.pop(parent_cls.inc_key())

    def __resolve_includes(self, spec_cls, spec_dict, catalog):
        """Replace data type inc strings with the spec definition so the new spec is built with included fields.
        """
        dt_def = spec_dict.get(spec_cls.def_key())
        dt_inc = spec_dict.get(spec_cls.inc_key())
        if dt_inc is not None and dt_def is not None:
            parent_spec = catalog.get_spec(dt_inc)
            if parent_spec is None:
                msg = "Cannot resolve include spec '%s' for type '%s'" % (dt_inc, dt_def)
                raise ValueError(msg)
            # replace the inc key value from string to the inc spec so that the spec can be updated with all of the
            # attributes, datasets, groups, and links of the inc spec when spec_cls.build_spec(spec_dict) is called
            spec_dict[spec_cls.inc_key()] = parent_spec
        for subspec_dict in spec_dict.get('groups', list()):
            self.__resolve_includes(self.__group_spec_cls, subspec_dict, catalog)
        for subspec_dict in spec_dict.get('datasets', list()):
            self.__resolve_includes(self.__dataset_spec_cls, subspec_dict, catalog)

    def __load_namespace(self, namespace, reader, resolve=True):
        ns_name = namespace['name']
        if ns_name in self.__namespaces:  # pragma: no cover
            raise KeyError("namespace '%s' already exists" % ns_name)
        catalog = SpecCatalog()
        included_types = dict()
        for s in namespace['schema']:
            # types_key may be different in each spec namespace, so check both the __spec_namespace_cls types key
            # and the parent SpecNamespace types key. NOTE: this does not handle more than one custom types key
            types_to_load = s.get(self.__spec_namespace_cls.types_key(), s.get(SpecNamespace.types_key()))
            if types_to_load is not None:  # schema specifies specific types from 'source' or 'namespace'
                types_to_load = set(types_to_load)
            if 'source' in s:
                # read specs from file
                self.__load_spec_file(reader, s['source'], catalog, types_to_load=types_to_load, resolve=resolve)
                self.__included_sources.setdefault(ns_name, list()).append(s['source'])
            elif 'namespace' in s:
                # load specs from namespace
                try:
                    inc_ns = self.get_namespace(s['namespace'])
                except KeyError as e:
                    raise ValueError("Could not load namespace '%s'" % s['namespace']) from e
                if types_to_load is None:
                    types_to_load = inc_ns.get_registered_types()  # load all types in namespace
                registered_types = set()
                for ndt in types_to_load:
                    self.__register_type(ndt, inc_ns, catalog, registered_types)
                included_types[s['namespace']] = tuple(sorted(registered_types))
            else:
                raise ValueError("Spec '%s' schema must have either 'source' or 'namespace' key" % ns_name)
        # construct namespace
        ns = self.__spec_namespace_cls.build_namespace(catalog=catalog, **namespace)
        self.__namespaces[ns_name] = ns
        return included_types

    def __register_type(self, ndt, inc_ns, catalog, registered_types):
        spec = inc_ns.get_spec(ndt)
        spec_file = inc_ns.catalog.get_spec_source_file(ndt)
        self.__register_dependent_types(spec, inc_ns, catalog, registered_types)
        if isinstance(spec, DatasetSpec):
            built_spec = self.dataset_spec_cls.build_spec(spec)
        else:
            built_spec = self.group_spec_cls.build_spec(spec)
        registered_types.add(ndt)
        catalog.register_spec(built_spec, spec_file)

    def __register_dependent_types(self, spec, inc_ns, catalog, registered_types):
        """Ensure that classes for all types used by this type are registered
        """
        # TODO test cross-namespace registration...
        def __register_dependent_types_helper(spec, inc_ns, catalog, registered_types):
            if isinstance(spec, (GroupSpec, DatasetSpec)):
                if spec.data_type_inc is not None:
                    # TODO handle recursive definitions
                    self.__register_type(spec.data_type_inc, inc_ns, catalog, registered_types)
                if spec.data_type_def is not None:  # nested type definition
                    self.__register_type(spec.data_type_def, inc_ns, catalog, registered_types)
            else:  # spec is a LinkSpec
                self.__register_type(spec.target_type, inc_ns, catalog, registered_types)
            if isinstance(spec, GroupSpec):
                for child_spec in (spec.groups + spec.datasets + spec.links):
                    __register_dependent_types_helper(child_spec, inc_ns, catalog, registered_types)

        if spec.data_type_inc is not None:
            self.__register_type(spec.data_type_inc, inc_ns, catalog, registered_types)
        if isinstance(spec, GroupSpec):
            for child_spec in (spec.groups + spec.datasets + spec.links):
                __register_dependent_types_helper(child_spec, inc_ns, catalog, registered_types)

    @docval({'name': 'namespace_path', 'type': str, 'doc': 'the path to the file containing the namespaces(s) to load'},
            {'name': 'resolve',
             'type': bool,
             'doc': 'whether or not to include objects from included/parent spec objects', 'default': True},
            {'name': 'reader',
             'type': SpecReader,
             'doc': 'the class to user for reading specifications', 'default': None},
            returns='a dictionary describing the dependencies of loaded namespaces', rtype=dict)
    def load_namespaces(self, **kwargs):
        """Load the namespaces in the given file"""
        namespace_path, resolve, reader = getargs('namespace_path', 'resolve', 'reader', kwargs)
        if reader is None:
            # load namespace definition from file
            if not os.path.exists(namespace_path):
                msg = "namespace file '%s' not found" % namespace_path
                raise IOError(msg)
            reader = YAMLSpecReader(indir=os.path.dirname(namespace_path))
        ns_path_key = os.path.join(reader.source, os.path.basename(namespace_path))
        ret = self.__included_specs.get(ns_path_key)
        if ret is None:
            ret = dict()
        else:
            return ret
        namespaces = reader.read_namespace(namespace_path)
        to_load = list()
        for ns in namespaces:
            if ns['name'] in self.__namespaces:
                if ns['version'] != self.__namespaces.get(ns['name'])['version']:
                    # warn if the cached namespace differs from the already loaded namespace
                    warn("Ignoring cached namespace '%s' version %s because version %s is already loaded."
                         % (ns['name'], ns['version'], self.__namespaces.get(ns['name'])['version']))
            else:
                to_load.append(ns)
        # now load specs into namespace
        for ns in to_load:
            ret[ns['name']] = self.__load_namespace(ns, reader, resolve=resolve)
        self.__included_specs[ns_path_key] = ret
        return ret
