import uuid
import numpy as np

from . import register_class
from ..container import Table, Row, Container

from ..utils import docval, popargs


def _check_id(table, id):
    if id >= 0 and len(table.which(id=id)) == 0:
        return np.uint64(id)
    else:
        raise ValueError('id must be a non-negative integer that is not already in the table: %d' % id)


class KeyTable(Table):

    __defaultname__ = 'keys'

    __columns__ = (
        {'name': 'key_name', 'type': str,
         'doc': 'The user key that maps to the resource term / registry symbol.'},
    )


class Key(Row):

    __table__ = KeyTable


class ResourceTable(Table):

    __defaultname__ = 'resources'

    __columns__ = (
        {'name': 'keytable_id', 'type': (str, Key),
         'doc': 'The user key that maps to the resource term / registry symbol.'},
        {'name': 'resource_name', 'type': str,
         'doc': 'The resource/registry that the term/symbol comes from.'},
        {'name': 'resource_entity_id', 'type': str,
         'doc': 'The unique resource identifier for the resource term / registry symbol.'},
        {'name': 'resource_entity_uri', 'type': str,
         'doc': 'The unique resource identifier for the resource term / registry symbol.'},
    )


class ResourceEntity(Row):

    __table__ = ResourceTable


class ObjectTable(Table):

    __defaultname__ = 'objects'

    __columns__ = (
        {'name': 'object_id', 'type': (str, uuid.UUID),
         'doc': 'The user key that maps to the resource term / registry symbol.'},
        {'name': 'field', 'type': str, 'doc': 'The resource/registry that the term/symbol comes from.'},
    )


class Object(Row):

    __table__ = ObjectTable


class ObjectKeyTable(Table):

    __defaultname__ = 'object_keys'

    __columns__ = (
        {'name': 'objecttable_id', 'type': (str, Object),
         'doc': 'The user key that maps to the resource term / registry symbol.'},
        {'name': 'keytable_id', 'type': (str, Key),
         'doc': 'The user key that maps to the resource term / registry symbol.'},
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

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this ExternalResources container'},
            {'name': 'keys', 'type': KeyTable, 'default': None,
             'doc': 'the table storing user keys for referencing resources'},
            {'name': 'resources', 'type': ResourceTable, 'default': None,
             'doc': 'the table storing resource information'},
            {'name': 'objects', 'type': ObjectTable, 'default': None,
             'doc': 'the table storing object information'},
            {'name': 'object_keys', 'type': ObjectKeyTable, 'default': None,
             'doc': 'the table storing object-resource relationships'})
    def __init__(self, **kwargs):
        name = popargs('name', kwargs)
        super().__init__(name)
        self.keys = kwargs['keys'] or KeyTable()
        self.resources = kwargs['resources'] or ResourceTable()
        self.objects = kwargs['objects'] or ObjectTable()
        self.object_keys = kwargs['object_keys'] or ObjectKeyTable()

    def add_key(self, key):
        return Key(key, table=self.keys)

    @docval({'name': 'key', 'type': (str, Key), 'doc': 'the key to associate the resource with'},
            {'name': 'resource_name', 'type': str, 'doc': 'the name of the resource'},
            {'name': 'resource_entity_id', 'type': str, 'doc': 'the entity at the resource to associate'},
            {'name': 'resource_entity_uri', 'type': str, 'doc': 'the URL for the entity at the resource'})
    def add_resource(self, **kwargs):
        key = kwargs['key']
        resource_name = kwargs['resource_name']
        resource_entity_id = kwargs['resource_entity_id']
        resource_entity_uri = kwargs['resource_entity_uri']
        if not isinstance(key, Key):
            key = self.add_key(key)
        resource_entity = ResourceEntity(key, resource_name, resource_entity_id, resource_entity_uri,
                                         table=self.resources)
        return resource_entity

    @docval({'name': 'container', 'type': (Container, str), 'doc': 'the Container to add'},
            {'name': 'field', 'type': str, 'doc': 'the field on the Container to add'})
    def add_object(self, **kwargs):
        container, field = popargs('container', 'field', kwargs)
        if isinstance(container, Container):
            container = container.object_id
        obj = Object(container, field, table=self.objects)
        return obj

    @docval({'name': 'obj', 'type': (int, Object), 'doc': 'the Object to that uses the Key'},
            {'name': 'key', 'type': (int, Key), 'doc': 'the Key that the Object uses'})
    def add_external_reference(self, **kwargs):
        obj, key = popargs('obj', 'key', kwargs)
        return ObjectKey(obj, key, table=self.object_keys)

    def _check_object_field(self, container, field):
        object_id = self.objects.which(object_id=container)
        if len(object_id) > 0:
            field_id = self.objects.which(field=field)
            object_id = list(set(object_id) & set(field_id))

        if len(object_id) == 1:
            return self.objects.row[object_id[0]]
        else:
            return self.add_object(container, field)

    @docval({'name': 'key_name', 'type': str, 'doc': 'the name of the key to get'},
            {'name': 'container', 'type': (str, Container), 'doc': 'the Container that uses the key', 'default': None},
            {'name': 'field', 'type': str, 'doc': 'the field of the Container that uses the key', 'default': None})
    def get_key(self, **kwargs):
        key_name, container, field = popargs('key_name', 'container', 'field', kwargs)
        key_id = self.keys.which(key_name=key_name)
        if container is not None and field is not None:
            # if same key is used multiple times, determine
            # which instance based on the Container
            object_field = self._check_object_field(container, field)
            key_tmp = self.object_keys['keytable_id', object_field.id]
            if key_tmp in key_id:
                return self.keys.row[key_tmp]
            else:
                raise ValueError("No key with name '%s' for container '%s' and field '%s'" %
                                 (key_name, container, field))
        else:
            if len(key_id) == 0:
                # the key has never been used before
                raise ValueError("key '%s' does not exist" % key_name)
            elif len(key_id) > 1:
                return [self.keys.row[i] for i in key_id]
            else:
                return self.keys.row[key_id[0]]

    @docval({'name': 'container', 'type': (str, Container), 'doc': 'the Container that uses the key', 'default': None},
            {'name': 'field', 'type': str, 'doc': 'the field of the Container that uses the key', 'default': None},
            {'name': 'key', 'type': (str, Key), 'doc': 'the name of the key to get', 'default': None},
            {'name': 'resource_name', 'type': str, 'doc': 'the online resource (i.e. database) name', 'default': None},
            {'name': 'entity_id', 'type': str, 'doc': 'the identifier for the entity at the resource', 'default': None},
            {'name': 'entity_uri', 'type': str, 'doc': 'the URI for the identifier at the resource', 'default': None})
    def add_ref(self, **kwargs):
        container = kwargs['container']
        field = kwargs['field']
        key = kwargs['key']
        resource_name = kwargs['resource_name']
        resource_id = kwargs['entity_id']
        resource_uri = kwargs['entity_uri']

        if isinstance(container, Container):
            container = container.object_id

        object_field = None

        # get Key object by searching the table
        if not isinstance(key, Key):
            key_id = self.keys.which(key_name=key)

            if len(key_id) == 0:
                # the key has never been used before
                key = self.add_key(key)

            elif len(key_id) > 1:
                # if same key is used multiple times, determine
                # which instance based on the Container
                object_field = self._check_object_field(container, field)

                key_tmp = self.object_keys['keytable_id', object_field.id]
                if key_tmp in key_id:
                    key = self.keys.row[key_tmp]
                else:
                    key = self.add_key(key)
            else:
                key = self.keys.row[key_id[0]]

        if object_field is None:
            object_field = self._check_object_field(container, field)

        resource_entity = self.add_resource(key, resource_name, resource_id, resource_uri)
        self.add_external_reference(object_field, key)

        return resource_entity
