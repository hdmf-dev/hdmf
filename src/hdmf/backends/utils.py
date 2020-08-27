"""Module with utility functions and classes used for implementation of backends"""

import os
from ..spec import NamespaceCatalog, GroupSpec, NamespaceBuilder
from ..utils import docval,  popargs

class NamespaceToBuilderHelper(object):
    """Helper class used in HDF5IO (and possibly elsewhere) to convert a namespace to a builder for I/O"""

    @classmethod
    @docval({'name': 'ns_catalog', 'type': NamespaceCatalog, 'doc': 'the namespace catalog with the specs'},
            {'name': 'namespace', 'type': str, 'doc': 'the name of the namespace to be converted to a builder'},
            rtype=NamespaceBuilder)
    def convert_namespace(cls, **kwargs):
        """Convert a namespace to a builder"""
        ns_catalog, namespace = popargs('ns_catalog', 'namespace', kwargs)
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
                    h5_source = cls.get_source_name(source)
                    spec = cls.__copy_spec(spec)
                    builder.add_spec(h5_source, spec)
        return builder

    @classmethod
    @docval({'name': 'source', 'type': str, 'doc': "source path"})
    def get_source_name(self, source):
        return os.path.splitext(source)[0]

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
