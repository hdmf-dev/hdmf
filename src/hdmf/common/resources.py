import pandas as pd

from . import register_class, EXP_NAMESPACE
from ..container import Table, Row, Container, AbstractContainer
from ..utils import docval, popargs


class KeyTable(Table):
    """
    A table for storing keys used to reference external resources
    """

    __defaultname__ = 'keys'

    __columns__ = (
        {'name': 'key', 'type': str,
         'doc': 'The user key that maps to the resource term / registry symbol.'},
    )


class Key(Row):
    """
    A Row class for representing rows in the KeyTable
    """

    __table__ = KeyTable


class ResourceTable(Table):
    """
    A table for storing names of ontology sources and their uri
    """

    __defaultname__ = 'resources'

    __columns__ = (
        {'name': 'resource', 'type': str,
         'doc': 'The resource/registry that the term/symbol comes from.'},
        {'name': 'resource_uri', 'type': str,
         'doc': 'The URI for the resource term / registry symbol.'},
    )


class Resource(Row):
    """
    A Row class for representing rows in the ResourceTable
    """

    __table__ = ResourceTable


class EntityTable(Table):
    """
    A table for storing the external resources a key refers to
    """

    __defaultname__ = 'entities'

    __columns__ = (
        {'name': 'keys_idx', 'type': (int, Key),
         'doc': ('The index into the keys table for the user key that '
                 'maps to the resource term / registry symbol.')},
        {'name': 'resources_idx', 'type': (int, Resource),
         'doc': 'The index into the ResourceTable.'},
        {'name': 'entity_id', 'type': str,
         'doc': 'The unique ID for the resource term / registry symbol.'},
        {'name': 'entity_uri', 'type': str,
         'doc': 'The URI for the resource term / registry symbol.'},
    )


class Entity(Row):
    """
    A Row class for representing rows in the EntityTable
    """

    __table__ = EntityTable


class ObjectTable(Table):
    """
    A table for storing objects (i.e. Containers) that contain keys that refer to external resources
    """

    __defaultname__ = 'objects'

    __columns__ = (
        {'name': 'object_id', 'type': str,
         'doc': 'The object ID for the Container/Data'},
        {'name': 'field', 'type': str,
         'doc': 'The field on the Container/Data that uses an external resource reference key'},
    )


class Object(Row):
    """
    A Row class for representing rows in the ObjectTable
    """

    __table__ = ObjectTable


class ObjectKeyTable(Table):
    """
    A table for identifying which keys are used by which objects for referring to external resources
    """

    __defaultname__ = 'object_keys'

    __columns__ = (
        {'name': 'objects_idx', 'type': (int, Object),
         'doc': 'the index into the objects table for the object that uses the key'},
        {'name': 'keys_idx', 'type': (int, Key),
         'doc': 'the index into the key table that is used to make an external resource reference'}
    )


class ObjectKey(Row):
    """
    A Row class for representing rows in the ObjectKeyTable
    """

    __table__ = ObjectKeyTable


@register_class('ExternalResources', EXP_NAMESPACE)
class ExternalResources(Container):
    """A table for mapping user terms (i.e. keys) to resource entities."""

    __fields__ = (
        {'name': 'keys', 'child': True},
        {'name': 'resources', 'child': True},
        {'name': 'objects', 'child': True},
        {'name': 'object_keys', 'child': True},
        {'name': 'entities', 'child': True},
    )

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this ExternalResources container'},
            {'name': 'keys', 'type': KeyTable, 'default': None,
             'doc': 'the table storing user keys for referencing resources'},
            {'name': 'resources', 'type': ResourceTable, 'default': None,
             'doc': 'the table for storing names of resources and their uri'},
            {'name': 'entities', 'type': EntityTable, 'default': None,
             'doc': 'the table storing entity information'},
            {'name': 'objects', 'type': ObjectTable, 'default': None,
             'doc': 'the table storing object information'},
            {'name': 'object_keys', 'type': ObjectKeyTable, 'default': None,
             'doc': 'the table storing object-resource relationships'})
    def __init__(self, **kwargs):
        name = popargs('name', kwargs)
        super().__init__(name)
        self.keys = kwargs['keys'] or KeyTable()
        self.resources = kwargs['resources'] or ResourceTable()
        self.entities = kwargs['entities'] or EntityTable()
        self.objects = kwargs['objects'] or ObjectTable()
        self.object_keys = kwargs['object_keys'] or ObjectKeyTable()

    @docval({'name': 'key_name', 'type': str,
             'doc': 'the name of the key to be added'})
    def _add_key(self, **kwargs):
        """
        Add a key to be used for making references to external resources

        It is possible to use the same *key_name* to refer to different resources so long as the *key_name* is not
        used within the same object and field. To do so, this method must be called for the two different resources.
        The returned Key objects must be managed by the caller so as to be appropriately passed to subsequent calls
        to methods for storing information about the different resources.
        """
        key = kwargs['key_name']
        return Key(key, table=self.keys)

    @docval({'name': 'key', 'type': (str, Key), 'doc': 'the key to associate the entity with'},
            {'name': 'resources_idx', 'type': (int, Resource), 'doc': 'the id of the resource'},
            {'name': 'entity_id', 'type': str, 'doc': 'unique entity id'},
            {'name': 'entity_uri', 'type': str, 'doc': 'the URI for the entity'})
    def _add_entity(self, **kwargs):
        """
        Add an entity that will be referenced to using the given key
        """
        key = kwargs['key']
        resources_idx = kwargs['resources_idx']
        entity_id = kwargs['entity_id']
        entity_uri = kwargs['entity_uri']
        if not isinstance(key, Key):
            key = self._add_key(key)
        resource_entity = Entity(key, resources_idx, entity_id, entity_uri, table=self.entities)
        return resource_entity

    @docval({'name': 'resource', 'type': str, 'doc': 'the name of the ontology resource'},
            {'name': 'uri', 'type': str, 'doc': 'uri associated with ontology resource'})
    def _add_resource(self, **kwargs):
        """
        Add resource name and uri to ResourceTable that will be referenced by the ResourceTable idx.
        """
        resource_name = kwargs['resource']
        uri = kwargs['uri']
        resource = Resource(resource_name, uri, table=self.resources)
        return resource

    @docval({'name': 'container', 'type': (str, AbstractContainer),
             'doc': 'the Container/Data object to add or the object_id for the Container/Data object to add'},
            {'name': 'field', 'type': str, 'doc': 'the field on the Container to add'})
    def _add_object(self, **kwargs):
        """
        Add an object that references an external resource
        """
        container, field = popargs('container', 'field', kwargs)
        if isinstance(container, AbstractContainer):
            container = container.object_id
        obj = Object(container, field, table=self.objects)
        return obj

    @docval({'name': 'obj', 'type': (int, Object), 'doc': 'the Object to that uses the Key'},
            {'name': 'key', 'type': (int, Key), 'doc': 'the Key that the Object uses'})
    def _add_external_reference(self, **kwargs):
        """
        Specify that an object (i.e. container and field) uses a key to reference
        an external resource
        """
        obj, key = popargs('obj', 'key', kwargs)
        return ObjectKey(obj, key, table=self.object_keys)

    def _check_object_field(self, container, field):
        """
        A helper function for checking if a container and field have been added.

        The container can be either an object_id string or a AbstractContainer.

        If the container and field have not been added, add the pair and return
        the corresponding Object. Otherwise, just return the Object.
        """
        if isinstance(container, str):
            objecttable_idx = self.objects.which(object_id=container)
        else:
            objecttable_idx = self.objects.which(object_id=container.object_id)

        if len(objecttable_idx) > 0:
            field_idx = self.objects.which(field=field)
            objecttable_idx = list(set(objecttable_idx) & set(field_idx))

        if len(objecttable_idx) == 1:
            return self.objects.row[objecttable_idx[0]]
        elif len(objecttable_idx) == 0:
            return self._add_object(container, field)
        else:
            raise ValueError("Found multiple instances of the same object_id and field in object table")

    @docval({'name': 'key_name', 'type': str, 'doc': 'the name of the key to get'},
            {'name': 'container', 'type': (str, AbstractContainer), 'default': None,
             'doc': ('the Container/Data object that uses the key or '
                     'the object_id for the Container/Data object that uses the key')},
            {'name': 'field', 'type': str, 'doc': 'the field of the Container that uses the key', 'default': None})
    def get_key(self, **kwargs):
        """
        Return a Key or a list of Key objects that correspond to the given key.

        If container and field are provided, the Key that corresponds to the given name of the key
        for the given container and field is returned.
        """
        key_name, container, field = popargs('key_name', 'container', 'field', kwargs)
        key_idx_matches = self.keys.which(key=key_name)
        if container is not None and field is not None:
            # if same key is used multiple times, determine
            # which instance based on the Container
            object_field = self._check_object_field(container, field)
            for row_idx in self.object_keys.which(objects_idx=object_field.idx):
                key_idx = self.object_keys['keys_idx', row_idx]
                if key_idx in key_idx_matches:
                    return self.keys.row[key_idx]
            raise ValueError("No key with name '%s' for container '%s' and field '%s'" % (key_name, container, field))
        else:
            if len(key_idx_matches) == 0:
                # the key has never been used before
                raise ValueError("key '%s' does not exist" % key_name)
            elif len(key_idx_matches) > 1:
                return [self.keys.row[i] for i in key_idx_matches]
            else:
                return self.keys.row[key_idx_matches[0]]

    @docval({'name': 'resource_name', 'type': str, 'default': None})
    def get_resource(self, **kwargs):
        """
        Retrieve resource object with the given resource_name.
        """
        resource_table_idx = self.resources.which(resource=kwargs['resource_name'])
        if len(resource_table_idx) == 0:
            # Resource hasn't been created
            msg = "No resource '%s' exists. Use _add_resource to create a new resource" % kwargs['resource_name']
            raise ValueError(msg)
        else:
            return self.resources.row[resource_table_idx[0]]

    @docval({'name': 'container', 'type': (str, AbstractContainer), 'default': None,
             'doc': ('the Container/Data object that uses the key or '
                     'the object_id for the Container/Data object that uses the key')},
            {'name': 'field', 'type': str, 'doc': 'the field of the Container/Data that uses the key', 'default': None},
            {'name': 'key', 'type': (str, Key), 'default': None,
             'doc': 'the name of the key or the Row object from the KeyTable for the key to add a resource for'},
            {'name': 'resources_idx', 'type': Resource, 'doc': 'the resourcetable id', 'default': None},
            {'name': 'resource_name', 'type': str, 'doc': 'the name of the resource to be created', 'default': None},
            {'name': 'resource_uri', 'type': str, 'doc': 'the uri of the resource to be created', 'default': None},
            {'name': 'entity_id', 'type': str, 'doc': 'the identifier for the entity at the resource', 'default': None},
            {'name': 'entity_uri', 'type': str, 'doc': 'the URI for the identifier at the resource', 'default': None})
    def add_ref(self, **kwargs):
        """
        Add information about an external reference used in this file.

        It is possible to use the same name of the key to refer to different resources
        so long as the name of the key is not used within the same object and field.
        This method does not support such functionality by default. The different
        keys must be added separately using _add_key and passed to the *key* argument
        in separate calls of this method. If a resource with the same name already
        exists, then it will be used and the resource_uri will be ignored.
        """
        container = kwargs['container']
        field = kwargs['field']
        key = kwargs['key']
        entity_id = kwargs['entity_id']
        entity_uri = kwargs['entity_uri']
        add_entity = False

        object_field = self._check_object_field(container, field)
        if not isinstance(key, Key):
            key_idx_matches = self.keys.which(key=key)
        # if same key is used multiple times, determine
        # which instance based on the Container
            for row_idx in self.object_keys.which(objects_idx=object_field.idx):
                key_idx = self.object_keys['keys_idx', row_idx]
                if key_idx in key_idx_matches:
                    msg = "Use Key Object when referencing an existing (container, field, key)"
                    raise ValueError(msg)

        if not isinstance(key, Key):
            key = self._add_key(key)

        if kwargs['resources_idx'] is not None and kwargs['resource_name'] is None and kwargs['resource_uri'] is None:
            resource_table_idx = kwargs['resources_idx']
        elif (
            kwargs['resources_idx'] is not None
            and (kwargs['resource_name'] is not None
                 or kwargs['resource_uri'] is not None)):
            msg = "Can't have resource_idx with resource_name or resource_uri."
            raise ValueError(msg)
        elif len(self.resources.which(resource=kwargs['resource_name'])) == 0:
            resource_name = kwargs['resource_name']
            resource_uri = kwargs['resource_uri']
            resource_table_idx = self._add_resource(resource_name, resource_uri)
        else:
            idx = self.resources.which(resource=kwargs['resource_name'])
            resource_table_idx = self.resources.row[idx[0]]

        if (resource_table_idx is not None and entity_id is not None and entity_uri is not None):
            add_entity = True
        elif not (resource_table_idx is None and entity_id is None and resource_uri is None):
            msg = ("Specify resource, entity_id, and entity_uri arguments."
                   "All three are required to create a reference")
            raise ValueError(msg)

        if add_entity:
            entity = self._add_entity(key, resource_table_idx, entity_id, entity_uri)
            self._add_external_reference(object_field, key)

        return key, resource_table_idx, entity

    @docval({'name': 'keys', 'type': (list, Key), 'default': None,
             'doc': 'the Key(s) to get external resource data for'},
            rtype=pd.DataFrame, returns='a DataFrame with keys and external resource data')
    def get_keys(self, **kwargs):
        """
        Return a DataFrame with information about keys used to make references to external resources.
        The DataFrame will contain the following columns:
            - *key_name*:              the key that will be used for referencing an external resource
            - *resources_idx*:         the index for the resourcetable
            - *entity_id*:    the index for the entity at the external resource
            - *entity_uri*:   the URI for the entity at the external resource

        It is possible to use the same *key_name* to refer to different resources so long as the *key_name* is not
        used within the same object and field. This method does not support such functionality by default. To
        select specific keys, use the *keys* argument to pass in the Key object(s) representing the desired keys. Note,
        if the same *key_name* is used more than once, multiple calls to this method with different Key objects will
        be required to keep the different instances separate. If a single call is made, it is left up to the caller to
        distinguish the different instances.
        """
        keys = popargs('keys', kwargs)
        if keys is None:
            keys = [self.keys.row[i] for i in range(len(self.keys))]
        else:
            if not isinstance(keys, list):
                keys = [keys]
        data = list()
        for key in keys:
            rsc_ids = self.entities.which(keys_idx=key.idx)
            for rsc_id in rsc_ids:
                rsc_row = self.entities.row[rsc_id].todict()
                rsc_row.pop('keys_idx')
                rsc_row['key_name'] = key.key
                data.append(rsc_row)
        return pd.DataFrame(data=data, columns=['key_name', 'resources_idx',
                                                'entity_id', 'entity_uri'])
