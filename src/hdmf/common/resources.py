import pandas as pd
import re
from . import register_class, EXP_NAMESPACE
from . import get_type_map
from ..container import Table, Row, Container, AbstractContainer
from ..utils import docval, popargs
from ..build import TypeMap


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
        {'name': 'relative_path', 'type': str,
         'doc': ('The relative_path of the attribute on the Container/Data that uses',
                 'an external resource reference key')},
        {'name': 'field', 'type': str,
         'doc': ('the field of the compound data type using'
                 'an external resource')}
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
        used within the same object and relative_path. To do so, this method must be called for the
        two different resources.

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
            {'name': 'relative_path', 'type': str, 'doc': 'the relative_path on the Container to add'},
            {'name': 'field', 'type': str, 'default': None,
             'doc': ('the field of the compound data type using'
                     'an external resource')})
    def _add_object(self, **kwargs):
        """
        Add an object that references an external resource
        """
        container, relative_path, field = popargs('container', 'relative_path', 'field', kwargs)
        if isinstance(container, AbstractContainer):
            container = container.object_id
        obj = Object(container, relative_path, field, table=self.objects)
        return obj

    @docval({'name': 'obj', 'type': (int, Object), 'doc': 'the Object to that uses the Key'},
            {'name': 'key', 'type': (int, Key), 'doc': 'the Key that the Object uses'})
    def _add_object_key(self, **kwargs):
        """
        Specify that an object (i.e. container and relative_path) uses a key to reference
        an external resource
        """
        obj, key = popargs('obj', 'key', kwargs)
        return ObjectKey(obj, key, table=self.object_keys)

    @docval({'name': 'container', 'type': (str, AbstractContainer), 'default': None,
             'doc': ('the Container/Data object that uses the key or '
                     'the object_id for the Container/Data object that uses the key')},
            {'name': 'relative_path', 'type': str,
             'doc': 'the relative_path of the Container that uses the key', 'default': None},
            {'name': 'field', 'type': str, 'default': None,
             'doc': ('the field of the compound data type using'
                     'an external resource')})
    def _check_object_field(self, container, relative_path, field):
        """
        A helper function for checking if a container and relative_path have been added.

        The container can be either an object_id string or a AbstractContainer.

        If the container and relative_path have not been added, add the pair and return
        the corresponding Object. Otherwise, just return the Object.
        """
        if field is None:
            field = ''
        if isinstance(container, str):
            objecttable_idx = self.objects.which(object_id=container)
        else:
            objecttable_idx = self.objects.which(object_id=container.object_id)

        if len(objecttable_idx) > 0:
            field_idx = self.objects.which(relative_path=relative_path)
            objecttable_idx = list(set(objecttable_idx) & set(field_idx))

        if len(objecttable_idx) == 1:
            return self.objects.row[objecttable_idx[0]]
        elif len(objecttable_idx) == 0:
            return self._add_object(container, relative_path, field)
        else:
            raise ValueError("Found multiple instances of the same object_id and relative_path in object table")

    @docval({'name': 'key_name', 'type': str, 'doc': 'the name of the key to get'},
            {'name': 'container', 'type': (str, AbstractContainer), 'default': None,
             'doc': ('the Container/Data object that uses the key or '
                     'the object_id for the Container/Data object that uses the key')},
            {'name': 'relative_path', 'type': str, 'doc': 'the relative_path of the Container that uses the key',
            'default': None},
            {'name': 'field', 'type': str, 'default': None,
             'doc': ('the field of the compound data type using'
                     'an external resource')})
    def get_key(self, **kwargs):
        """
        Return a Key or a list of Key objects that correspond to the given key.

        If container and relative_path are provided, the Key that corresponds to the given name of the key
        for the given container and relative_path is returned.
        """
        key_name, container, relative_path, field = popargs('key_name', 'container', 'relative_path', 'field', kwargs)
        key_idx_matches = self.keys.which(key=key_name)
        if container is not None and relative_path is not None:
            # if same key is used multiple times, determine
            # which instance based on the Container
            object_field = self._check_object_field(container, relative_path, field)
            for row_idx in self.object_keys.which(objects_idx=object_field.idx):
                key_idx = self.object_keys['keys_idx', row_idx]
                if key_idx in key_idx_matches:
                    return self.keys.row[key_idx]
            msg = "No key '%s' for container '%s' and relative_path '%s'" % (key_name, container, relative_path)
            raise ValueError(msg)
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
            {'name': 'attribute', 'type': str,
             'doc': 'the attribute of the container for the external reference', 'default': None},
            {'name': 'key', 'type': (str, Key), 'default': None,
             'doc': 'the name of the key or the Row object from the KeyTable for the key to add a resource for'},
            {'name': 'resources_idx', 'type': Resource, 'doc': 'the resourcetable id', 'default': None},
            {'name': 'resource_name', 'type': str, 'doc': 'the name of the resource to be created', 'default': None},
            {'name': 'resource_uri', 'type': str, 'doc': 'the uri of the resource to be created', 'default': None},
            {'name': 'entity_id', 'type': str, 'doc': 'the identifier for the entity at the resource', 'default': None},
            {'name': 'entity_uri', 'type': str, 'doc': 'the URI for the identifier at the resource', 'default': None},
            {'name': 'type_map', 'type': TypeMap, 'doc': 'type_map used', 'default': None},
            {'name': 'field', 'type': str, 'default': None,
             'doc': ('the field of the compound data type using'
                     'an external resource')})
    def add_ref(self, **kwargs):
        """
        Add information about an external reference used in this file.

        It is possible to use the same name of the key to refer to different resources
        so long as the name of the key is not used within the same object and relative_path.
        This method does not support such functionality by default. The different
        keys must be added separately using _add_key and passed to the *key* argument
        in separate calls of this method. If a resource with the same name already
        exists, then it will be used and the resource_uri will be ignored.

        In the current version of add_ref, the user adds a container and inputs a relative_path of the
        attribute that is linked to/using some external resource. This is a problem when future
        users want to query the data. The creator of the data could name relative_path something others
        than what relative_path is supposed to be, i.e the path to the attribute.

        Another issue was the oversight on which container we should be using. For example,
        DynamicTable is itself a data_type, but also contains data_types, i.e the columns of VectorData.
        When we are adding some external resource, we are saying that the column is linked to
        that resource and not the table. As a result, it is the VectorData object_id that should be
        added to the ObjectTable and not the DynamicTable object_id.

        We are then presented with cases that need to be supported:
        1. Trivial Case: The container is the same as the attribute that has an external resource.
        The object_id is the id of the attribute and the relative_path is blank. Why is the relative_path blank?

        2. Attribute Case: An attribute of a container is being linked to an external resource.
        (Non-nested, i.e along the lines that just the VectorData column of a DynamicTable).
        The object_id is is that of the attribute (in this case the attribute is a data_type) and
        the relative_path is blank.

        3. Non-DataType Attribute Case: Similar to the Attribute Case prior; however, the attribute is
        not a data_type and so does not have an id. The object_id to be added to the ObjectTable will be
        the nearest data_type parent and the relative_path is the path to the attribute.

        """
        ###############################################################
        container = kwargs['container']
        attribute = kwargs['attribute']
        key = kwargs['key']
        field = kwargs['field']
        entity_id = kwargs['entity_id']
        entity_uri = kwargs['entity_uri']
        type_map = kwargs['type_map']
        add_entity = False

        if type_map is None:
            type_map = get_type_map()
        if attribute is None:  # Trivial Case
            relative_path = ''
            object_field = self._check_object_field(container, relative_path, field)
        else:  # DataType Attribute Case
            attribute_object = getattr(container, attribute)  # returns attribute object
            if isinstance(attribute_object, AbstractContainer):
                relative_path = ''
                object_field = self._check_object_field(attribute_object, relative_path, field)
            else:  # Non-DataType Attribute Case:
                # type_map = get_type_map()
                obj_mapper = type_map.get_map(container)
                spec = obj_mapper.get_attr_spec(attr_name=attribute)
                parent_spec = spec.parent  # return the parent spec of the attribute
                if parent_spec.data_type is None:
                    while parent_spec.data_type is None:
                        parent_spec = parent_spec.parent  # find the closest parent with a data_type
                    parent_cls = type_map.get_dt_container_cls(data_type=parent_spec.data_type, autogen=False)
                    if isinstance(container, parent_cls):
                        parent_id = container.object_id
                        # We need to get the path of the spec for relative_path
                        absolute_path = spec.path
                        relative_path = re.sub("^.+?(?="+container.data_type+")", "", absolute_path)
                        object_field = self._check_object_field(parent_id, relative_path, field)
                    else:
                        msg = 'Container not the nearest data_type'
                        raise ValueError(msg)
                else:
                    parent_id = container.object_id  # container needs to be the parent
                    absolute_path = spec.path
                    relative_path = re.sub("^.+?(?="+container.data_type+")", "", absolute_path)
                    object_field = self._check_object_field(parent_id, relative_path, field)

        if not isinstance(key, Key):
            key_idx_matches = self.keys.which(key=key)
        # if same key is used multiple times, determine
        # which instance based on the Container
            for row_idx in self.object_keys.which(objects_idx=object_field.idx):
                key_idx = self.object_keys['keys_idx', row_idx]
                if key_idx in key_idx_matches:
                    msg = "Use Key Object when referencing an existing (container, relative_path, key)"
                    raise ValueError(msg)

        if not isinstance(key, Key):
            key = self._add_key(key)
            self._add_object_key(object_field, key)

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

        return key, resource_table_idx, entity

    @docval({'name': 'container', 'type': (str, AbstractContainer),
             'doc': 'the Container/data object that is linked to resources/entities',
             'default': None},
            {'name': 'relative_path', 'type': str,
             'doc': 'the relative_path of the Container',
             'default': None},
            {'name': 'field', 'type': str, 'default': None,
             'doc': ('the field of the compound data type using'
                     'an external resource')})
    def get_object_resources(self, **kwargs):
        """
        Get all entities/resources associated with an object
        """
        container = kwargs['container']
        relative_path = kwargs['relative_path']
        field = kwargs['field']

        if relative_path is None:
            relative_path = ''

        keys = []
        entities = []
        if container is not None and relative_path is not None:
            object_field = self._check_object_field(container, relative_path, field)
            # Find all keys associated with the object
            for row_idx in self.object_keys.which(objects_idx=object_field.idx):
                keys.append(self.object_keys['keys_idx', row_idx])
            # Find all the entities/resources for each key.
            for key_idx in keys:
                entity_idx = self.entities.which(keys_idx=key_idx)
                entities.append(self.entities.__getitem__(entity_idx[0]))
            df = pd.DataFrame(entities, columns=['keys_idx', 'resource_idx', 'entity_id', 'entity_uri'])
        return df

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
        used within the same object and relative_path. This method does not support such functionality by default. To
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
