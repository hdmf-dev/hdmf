import uuid
import numpy as np

from . import register_class
from ..container import Table, Row, Container

from ..utils import docval, call_docval_func, popargs


def _check_id(table, id):
    if id >= 0 and len(table.which(id=id)) == 0:
        return np.uint64(id)
    else:
        raise ValueError('id must be a non-negative integer that is not already in the table: %d' % id)



class KeyTable(Table):

    __defaultname__ = 'keys'

    __columns__ = (
        {'name': 'key_name', 'type': str, 'doc': 'The user key that maps to the resource term / registry symbol.'},
    )


class Key(Row):

    __table__ = KeyTable


class ResourceTable(Table):

    __defaultname__ = 'resources'

    __columns__ = (
        {'name': 'keytable_id', 'type': (str, Key), 'doc': 'The user key that maps to the resource term / registry symbol.'},
        {'name': 'resource_name', 'type': str, 'doc': 'The resource/registry that the term/symbol comes from.'},
        {'name': 'resource_entity_id', 'type': str, 'doc': 'The unique resource identifier for the resource term / registry symbol.'},
        {'name': 'resource_entity_uri', 'type': str, 'doc': 'The unique resource identifier for the resource term / registry symbol.'},
    )


class ResourceEntity(Row):

    __table__ = ResourceTable


class ObjectTable(Table):

    __defaultname__ = 'objects'

    __columns__ = (
        {'name': 'object_id', 'type': (str, uuid.UUID), 'doc': 'The user key that maps to the resource term / registry symbol.'},
        {'name': 'field', 'type': str, 'doc': 'The resource/registry that the term/symbol comes from.'},
    )


class Object(Row):

    __table__ = ObjectTable


class ObjectKeyTable(Table):

    __defaultname__ = 'object_keys'

    __columns__ = (
        {'name': 'objecttable_id', 'type': (str, Object), 'doc': 'The user key that maps to the resource term / registry symbol.'},
        {'name': 'keytable_id', 'type': (str, Key), 'doc': 'The user key that maps to the resource term / registry symbol.'},
    )


class ObjectKey(Row):

    __table__ = ObjectKeyTable


@register_class('ExternalResources')
class ExternalResources(Container):
    """A table for mapping user terms (i.e. keys) to resource entities."""

    __fields__ = (
        {'name': 'keys', 'child': True},
        {'name': 'resources', 'child': True},
        {'name': 'objects', 'child': True},
        {'name': 'object_keys', 'child': True},
    )

    def __init__(self, keys=None, resources=None, objects=None, object_keys=None):
        self.keys = keys or KeyTable()
        self.resources = resources or ResourceTable()
        self.objects = objects or ObjectTable()
        self.object_keys = object_keys or ObjectKeyTable()

    def add_key(self, key):
        return Key(key, table=self.keys)

    def add_resource(self, key, resource_name, resource_entity_id, resource_entity_uri):
        if not isinstance(key, Key):
            key = self.add_key(key)
        resource_entity = ResourceEntity(key, resource_name, resource_entity_id, resource_entity_uri, table=self.resources)
        return resource_entity

    def add_object(self, container, field):
        if isinstance(container, Container):
            container = container.object_id
        obj = Object(container, field, table=self.objects)
        return obj

    def add_external_reference(self, obj, key):
        return ObjectKeyTable(obj, key, table=self.object_keys)

