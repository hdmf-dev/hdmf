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
from ..typing import validated


class SpecWriter(metaclass=ABCMeta):

    @abstractmethod
    def write_spec(self, spec_file_dict, path):
        pass

    @abstractmethod
    def write_namespace(self, namespace, path):
        pass


class YAMLSpecWriter(SpecWriter):

    @validated
    def __init__(self, outdir: str = '.'):
        """Initialize this spec.

        Args:
            outdir: the path to write the directory to output the namespace and specs too
        """
        self.__outdir = outdir

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

    @staticmethod
    def sort_keys(obj):
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
                yaml.compat.ordereddict([(k, YAMLSpecWriter.sort_keys(obj[k])) for k in keys])
            )
        elif isinstance(obj, list):
            return [YAMLSpecWriter.sort_keys(v) for v in obj]
        elif isinstance(obj, tuple):
            return (YAMLSpecWriter.sort_keys(v) for v in obj)
        else:
            return obj


class NamespaceBuilder:
    ''' A class for building namespace and spec files '''

    @validated
    def __init__(self,
                 doc: str,
                 name: str,
                 full_name: str | None = None,
                 version: str | tuple | list | None = None,
                 author: str | list | None = None,
                 contact: str | list | None = None,
                 date: datetime | str | None = None,
                 namespace_cls: type = SpecNamespace):
        """Initialize this spec.

        Args:
            doc: Description about what the namespace represents
            name: Name of the namespace
            full_name: Extended full name of the namespace
            version: Version number of the namespace
            author: Author or list of authors.
            contact: List of emails. Ordering should be the same as for author
            date: Date last modified or released. Formatting is %Y-%m-%d %H:%M:%S, e.g, 2017-04-25 17:14:13
            namespace_cls: the SpecNamespace type
        """
        ns_cls = namespace_cls
        if version is None:
            # version is required on write as of HDMF 1.5. this check should prevent the writing of namespace files
            # without a version
            raise ValueError("Namespace '%s' missing key 'version'. Please specify a version for the extension."
                             % name)
        self.__ns_args = copy.deepcopy(dict(doc=doc, name=name, full_name=full_name, version=version,
                                            author=author, contact=contact, date=date))
        self.__namespaces = OrderedDict()
        self.__sources = OrderedDict()
        self.__catalog = SpecCatalog()
        self.__dt_key = ns_cls.types_key()

    @validated
    def add_spec(self, source: str, spec: GroupSpec | DatasetSpec):
        """Add a Spec to the namespace

        Args:
            source: the path to write the spec to
            spec: the Spec to add
        """
        self.__catalog.auto_register(spec, source)
        self.add_source(source)
        self.__sources[source].setdefault(self.__dt_key, list()).append(spec)

    @validated
    def add_source(self, source: str, doc: str | None = None, title: str | None = None):
        """Add a source file to the namespace

        Args:
            source: the path to write the spec to
            doc: additional documentation for the source file
            title: optional heading to be used for the source
        """
        if '/' in source or source[0] == '.':
            raise ValueError('source must be a base file')
        source_dict = {'source': source}
        self.__sources.setdefault(source, source_dict)
        # Update the doc and title if given
        if doc is not None:
            self.__sources[source]['doc'] = doc
        if title is not None:
            self.__sources[source]['title'] = doc

    @validated
    def include_type(self, data_type: str, source: str | None = None, namespace: str | None = None):
        """Include a data type from an existing namespace or source

        Args:
            data_type: the data type to include
            source: the source file to include the type from
            namespace: the namespace from which to include the data type
        """
        dt, src, ns = data_type, source, namespace
        if src is not None:
            self.add_source(src)
            self.__sources[src].setdefault(self.__dt_key, list()).append(dt)
        elif ns is not None:
            self.include_namespace(ns)
            self.__namespaces[ns].setdefault(self.__dt_key, list()).append(dt)
        else:
            raise ValueError("must specify 'source' or 'namespace' when including type")

    @validated
    def include_namespace(self, namespace: str):
        """Include an entire namespace

        Args:
            namespace: the namespace to include
        """
        self.__namespaces.setdefault(namespace, {'namespace': namespace})

    @validated
    def export(self, path: str, outdir: str = '.', writer: SpecWriter | None = None):
        """Export the namespace to the given path.

        All new specification source files will be written in the same directory as the
        given path.

        Args:
            path: the path to write the spec to
            outdir: the path to write the directory to output the namespace and specs too
            writer: the SpecWriter to use to write the namespace
        """
        ns_path = path
        if writer is None:
            writer = YAMLSpecWriter(outdir=outdir)
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

    @validated
    def add_spec(self, spec: GroupSpec | DatasetSpec):
        """add_spec

        Args:
            spec: the Spec to add
        """
        if isinstance(spec, GroupSpec):
            self.setdefault('groups', list()).append(spec)
        elif isinstance(spec, DatasetSpec):
            self.setdefault('datasets', list()).append(spec)


def export_spec(ns_builder, new_data_types, output_dir):
    """
    Create YAML specification files for a new namespace and extensions with
    the given data type specs.

    Args:
        ns_builder: NamespaceBuilder instance used to build the
                     namespace and extension
        new_data_types: Iterable of specs that represent new data types
                         to be added
    """

    if len(new_data_types) == 0:
        warnings.warn('No data types specified. Exiting.', stacklevel=2)
        return

    ns_path = ns_builder.name + '.namespace.yaml'
    ext_path = ns_builder.name + '.extensions.yaml'

    for data_type in new_data_types:
        ns_builder.add_spec(ext_path, data_type)

    ns_builder.export(ns_path, outdir=output_dir)
