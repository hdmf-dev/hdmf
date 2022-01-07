import copy
import json
import os.path
import warnings
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from datetime import datetime
import ruamel.yaml as yaml

from .catalog import SpecCatalog
from .namespace import SpecNamespace
from .spec import GroupSpec, DatasetSpec
from ..utils import docval, getargs, popargs


class SpecWriter(metaclass=ABCMeta):

    @abstractmethod
    def write_spec(self, spec_file_dict, path):
        pass

    @abstractmethod
    def write_namespace(self, namespace, path):
        pass


class YAMLSpecWriter(SpecWriter):

    @docval({'name': 'outdir',
             'type': str,
             'doc': 'the path to write the directory to output the namespace and specs too', 'default': '.'})
    def __init__(self, **kwargs):
        self.__outdir = getargs('outdir', kwargs)

    def __dump_spec(self, specs, stream):
        specs_plain_dict = json.loads(json.dumps(specs))
        yaml_obj = yaml.YAML(typ='safe', pure=True)
        yaml_obj.default_flow_style = False
        yaml_obj.dump(specs_plain_dict, stream)

    def write_spec(self, spec_file_dict, path):
        out_fullpath = os.path.join(self.__outdir, path)
        spec_plain_dict = json.loads(json.dumps(spec_file_dict))
        sorted_data = self.sort_keys(spec_plain_dict)
        with open(out_fullpath, 'w') as fd_write:
            yaml_obj = yaml.YAML(pure=True)
            yaml_obj.dump(sorted_data, fd_write)

    def write_namespace(self, namespace, path):
        """Write the given namespace key-value pairs as YAML to the given path.

        :param namespace: SpecNamespace holding the key-value pairs that define the namespace
        :param path: File path to write the namespace to as YAML under the key 'namespaces'
        """
        with open(os.path.join(self.__outdir, path), 'w') as stream:
            # Convert the date to a string if necessary
            ns = namespace
            if 'date' in namespace and isinstance(namespace['date'], datetime):
                ns = copy.copy(ns)  # copy the namespace to avoid side-effects
                ns['date'] = ns['date'].isoformat()
            self.__dump_spec({'namespaces': [ns]}, stream)

    def reorder_yaml(self, path):
        """
        Open a YAML file, load it as python data, sort the data alphabetically, and write it back out to the
        same path.
        """
        with open(path, 'rb') as fd_read:
            yaml_obj = yaml.YAML(pure=True)
            data = yaml_obj.load(fd_read)
        self.write_spec(data, path)

    def sort_keys(self, obj):
        # Represent None as null
        def my_represent_none(self, data):
            return self.represent_scalar(u'tag:yaml.org,2002:null', u'null')

        yaml.representer.RoundTripRepresenter.add_representer(type(None), my_represent_none)

        order = ['neurodata_type_def', 'neurodata_type_inc', 'data_type_def', 'data_type_inc',
                 'name', 'default_name',
                 'dtype', 'target_type', 'dims', 'shape', 'default_value', 'value', 'doc',
                 'required', 'quantity', 'attributes', 'datasets', 'groups', 'links']
        if isinstance(obj, dict):
            keys = list(obj.keys())
            for k in order[::-1]:
                if k in keys:
                    keys.remove(k)
                    keys.insert(0, k)
            if 'neurodata_type_def' not in keys and 'name' in keys:
                keys.remove('name')
                keys.insert(0, 'name')
            return yaml.comments.CommentedMap(
                yaml.compat.ordereddict([(k, self.sort_keys(obj[k])) for k in keys])
            )
        elif isinstance(obj, list):
            return [self.sort_keys(v) for v in obj]
        elif isinstance(obj, tuple):
            return (self.sort_keys(v) for v in obj)
        else:
            return obj


class NamespaceBuilder:
    ''' A class for building namespace and spec files '''

    @docval({'name': 'doc', 'type': str, 'doc': 'Description about what the namespace represents'},
            {'name': 'name', 'type': str, 'doc': 'Name of the namespace'},
            {'name': 'full_name', 'type': str, 'doc': 'Extended full name of the namespace', 'default': None},
            {'name': 'version', 'type': (str, tuple, list), 'doc': 'Version number of the namespace', 'default': None},
            {'name': 'author', 'type': (str, list), 'doc': 'Author or list of authors.', 'default': None},
            {'name': 'contact', 'type': (str, list),
             'doc': 'List of emails. Ordering should be the same as for author', 'default': None},
            {'name': 'date', 'type': (datetime, str),
             'doc': "Date last modified or released. Formatting is %Y-%m-%d %H:%M:%S, e.g, 2017-04-25 17:14:13",
             'default': None},
            {'name': 'namespace_cls', 'type': type, 'doc': 'the SpecNamespace type', 'default': SpecNamespace})
    def __init__(self, **kwargs):
        ns_cls = popargs('namespace_cls', kwargs)
        if kwargs['version'] is None:
            # version is required on write as of HDMF 1.5. this check should prevent the writing of namespace files
            # without a verison
            raise ValueError("Namespace '%s' missing key 'version'. Please specify a version for the extension."
                             % kwargs['name'])
        self.__ns_args = copy.deepcopy(kwargs)
        self.__namespaces = OrderedDict()
        self.__sources = OrderedDict()
        self.__catalog = SpecCatalog()
        self.__dt_key = ns_cls.types_key()

    @docval({'name': 'source', 'type': str, 'doc': 'the path to write the spec to'},
            {'name': 'spec', 'type': (GroupSpec, DatasetSpec), 'doc': 'the Spec to add'})
    def add_spec(self, **kwargs):
        ''' Add a Spec to the namespace '''
        source, spec = getargs('source', 'spec', kwargs)
        self.__catalog.auto_register(spec, source)
        self.add_source(source)
        self.__sources[source].setdefault(self.__dt_key, list()).append(spec)

    @docval({'name': 'source', 'type': str, 'doc': 'the path to write the spec to'},
            {'name': 'doc', 'type': str, 'doc': 'additional documentation for the source file', 'default': None},
            {'name': 'title', 'type': str, 'doc': 'optional heading to be used for the source', 'default': None})
    def add_source(self, **kwargs):
        ''' Add a source file to the namespace '''
        source, doc, title = getargs('source', 'doc', 'title', kwargs)
        if '/' in source or source[0] == '.':
            raise ValueError('source must be a base file')
        source_dict = {'source': source}
        self.__sources.setdefault(source, source_dict)
        # Update the doc and title if given
        if doc is not None:
            self.__sources[source]['doc'] = doc
        if title is not None:
            self.__sources[source]['title'] = doc

    @docval({'name': 'data_type', 'type': str, 'doc': 'the data type to include'},
            {'name': 'source', 'type': str, 'doc': 'the source file to include the type from', 'default': None},
            {'name': 'namespace', 'type': str,
             'doc': 'the namespace from which to include the data type', 'default': None})
    def include_type(self, **kwargs):
        ''' Include a data type from an existing namespace or source '''
        dt, src, ns = getargs('data_type', 'source', 'namespace', kwargs)
        if src is not None:
            self.add_source(src)
            self.__sources[src].setdefault(self.__dt_key, list()).append(dt)
        elif ns is not None:
            self.include_namespace(ns)
            self.__namespaces[ns].setdefault(self.__dt_key, list()).append(dt)
        else:
            raise ValueError("must specify 'source' or 'namespace' when including type")

    @docval({'name': 'namespace', 'type': str, 'doc': 'the namespace to include'})
    def include_namespace(self, **kwargs):
        ''' Include an entire namespace '''
        namespace = getargs('namespace', kwargs)
        self.__namespaces.setdefault(namespace, {'namespace': namespace})

    @docval({'name': 'path', 'type': str, 'doc': 'the path to write the spec to'},
            {'name': 'outdir',
             'type': str,
             'doc': 'the path to write the directory to output the namespace and specs too', 'default': '.'},
            {'name': 'writer',
             'type': SpecWriter,
             'doc': 'the SpecWriter to use to write the namespace', 'default': None})
    def export(self, **kwargs):
        ''' Export the namespace to the given path.

        All new specification source files will be written in the same directory as the
        given path.
        '''
        ns_path, writer = getargs('path', 'writer', kwargs)
        if writer is None:
            writer = YAMLSpecWriter(outdir=getargs('outdir', kwargs))
        ns_args = copy.copy(self.__ns_args)
        ns_args['schema'] = list()
        for ns, info in self.__namespaces.items():
            ns_args['schema'].append(info)
        for path, info in self.__sources.items():
            out = SpecFileBuilder()
            dts = list()
            for spec in info[self.__dt_key]:
                if isinstance(spec, str):
                    dts.append(spec)
                else:
                    out.add_spec(spec)
            item = {'source': path}
            if 'doc' in info:
                item['doc'] = info['doc']
            if 'title' in info:
                item['title'] = info['title']
            if out and dts:
                raise ValueError('cannot include from source if writing to source')
            elif dts:
                item[self.__dt_key] = dts
            elif out:
                writer.write_spec(out, path)
            ns_args['schema'].append(item)
        namespace = SpecNamespace.build_namespace(**ns_args)
        writer.write_namespace(namespace, ns_path)

    @property
    def name(self):
        return self.__ns_args['name']


class SpecFileBuilder(dict):

    @docval({'name': 'spec', 'type': (GroupSpec, DatasetSpec), 'doc': 'the Spec to add'})
    def add_spec(self, **kwargs):
        spec = getargs('spec', kwargs)
        if isinstance(spec, GroupSpec):
            self.setdefault('groups', list()).append(spec)
        elif isinstance(spec, DatasetSpec):
            self.setdefault('datasets', list()).append(spec)


def export_spec(ns_builder, new_data_types, output_dir):
    """
    Create YAML specification files for a new namespace and extensions with
    the given data type specs.

    Args:
        ns_builder - NamespaceBuilder instance used to build the
                     namespace and extension
        new_data_types - Iterable of specs that represent new data types
                         to be added
    """

    if len(new_data_types) == 0:
        warnings.warn('No data types specified. Exiting.')
        return

    ns_path = ns_builder.name + '.namespace.yaml'
    ext_path = ns_builder.name + '.extensions.yaml'

    for data_type in new_data_types:
        ns_builder.add_spec(ext_path, data_type)

    ns_builder.export(ns_path, outdir=output_dir)
