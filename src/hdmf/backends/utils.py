"""Module with utility functions and classes used for implementation of I/O backends"""
import os
from ..spec import NamespaceCatalog, NamespaceBuilder
from ..utils import docval,  popargs


class WriteStatusTracker(dict):
    """
    Helper class used for tracking the write status of builders. I.e., to track whether a
    builder has been written or not.
    """
    def __init__(self):
        pass

    def __builderhash(self, builder):
        """Return the ID of a builder for use as a unique hash."""
        # NOTE: id may not be sufficient if builders are created inline in the function call, in which
        #       case the id is the id of the functions parameter, so it can be the same for different
        #       builders. This should typically only happen in unit testing, but just to be safe.
        return str(id(builder)) + "_" + str(builder.name)

    def set_written(self, builder):
        """
        Mark this builder as written.

        :param builder: Builder object to be marked as written
        :type builder: Builder
        """
        # currently all values in self._written_builders are True, so this could be a set but is a dict for
        # future flexibility
        builder_id = self.__builderhash(builder)
        self[builder_id] = True

    def get_written(self, builder):
        """Return True if this builder has been written to (or read from) disk by this IO object, False otherwise.

        :param builder: Builder object to get the written flag for
        :type builder: Builder

        :return: True if the builder is found in self._written_builders using the builder ID, False otherwise
        """
        builder_id = self.__builderhash(builder)
        return self.get(builder_id, False)


class NamespaceToBuilderHelper(object):
    """Helper class used to convert a namespace to a builder for I/O"""

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
                # Remove extension from source to create path for writing
                file_source = os.path.splitext(source)[0]
                # Use the cached unresolved specs to preserve original structure
                spec_dict = ns_catalog.get_spec_source_dict(source)
                specs = spec_dict.get('datasets', []) + spec_dict.get('groups', [])
                for spec in specs:
                    builder.add_spec(file_source, spec)
        return builder
