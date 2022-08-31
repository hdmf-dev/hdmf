import pandas as pd
import re
from . import register_class, EXP_NAMESPACE
from . import get_type_map
from ..container import Table, Row, Container, AbstractContainer
from ..utils import docval, popargs, AllowPositional
from ..build import TypeMap


class KeyTable(Table):
    """
    A table for storing keys used to reference external resources.
    """

    __defaultname__ = 'keys'

    __columns__ = (
        {'name': 'key', 'type': str,
         'doc': 'The user key that maps to the resource term / registry symbol.'},
    )


class Key(Row):
    """
    A Row class for representing rows in the KeyTable.
    """

    __table__ = KeyTable


class ResourceTable(Table):
    """
    A table for storing names and URIs of ontology sources.
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
    A Row class for representing rows in the ResourceTable.
    """

    __table__ = ResourceTable


class EntityTable(Table):
    """
    A table for storing the external resources a key refers to.
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
    A Row class for representing rows in the EntityTable.
    """

    __table__ = EntityTable


class ObjectTable(Table):
    """
    A table for storing objects (i.e. Containers) that contain keys that refer to external resources.
    """

    __defaultname__ = 'objects'

    __columns__ = (
        {'name': 'object_id', 'type': str,
         'doc': 'The object ID for the Container/Data.'},
        {'name': 'relative_path', 'type': str,
         'doc': ('The relative_path of the attribute of the object that uses ',
                 'an external resource reference key. Use an empty string if not applicable.')},
        {'name': 'field', 'type': str,
         'doc': ('The field of the compound data type using an external resource. '
                 'Use an empty string if not applicable.')}
    )


class Object(Row):
    """
    A Row class for representing rows in the ObjectTable.
    """

    __table__ = ObjectTable


class ObjectKeyTable(Table):
    """
    A table for identifying which keys are used by which objects for referring to external resources.
    """

    __defaultname__ = 'object_keys'

    __columns__ = (
        {'name': 'objects_idx', 'type': (int, Object),
         'doc': 'The index into the objects table for the Object that uses the Key.'},
        {'name': 'keys_idx', 'type': (int, Key),
         'doc': 'The index into the keys table that is used to make an external resource reference.'}
    )


class ObjectKey(Row):
    """
    A Row class for representing rows in the ObjectKeyTable.
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

    @docval({'name': 'name', 'type': str, 'doc': 'The name of this ExternalResources container.'},
            {'name': 'keys', 'type': KeyTable, 'default': None,
             'doc': 'The table storing user keys for referencing resources.'},
            {'name': 'resources', 'type': ResourceTable, 'default': None,
             'doc': 'The table for storing names and URIs of resources.'},
            {'name': 'entities', 'type': EntityTable, 'default': None,
             'doc': 'The table storing entity information.'},
            {'name': 'objects', 'type': ObjectTable, 'default': None,
             'doc': 'The table storing object information.'},
            {'name': 'object_keys', 'type': ObjectKeyTable, 'default': None,
             'doc': 'The table storing object-resource relationships.'},
            {'name': 'type_map', 'type': TypeMap, 'default': None,
             'doc': 'The type map. If None is provided, the HDMF-common type map will be used.'},
            allow_positional=AllowPositional.WARNING)
    def __init__(self, **kwargs):
        name = popargs('name', kwargs)
        super().__init__(name)
        self.keys = kwargs['keys'] or KeyTable()
        self.resources = kwargs['resources'] or ResourceTable()
        self.entities = kwargs['entities'] or EntityTable()
        self.objects = kwargs['objects'] or ObjectTable()
        self.object_keys = kwargs['object_keys'] or ObjectKeyTable()
        self.type_map = kwargs['type_map'] or get_type_map()

    @docval({'name': 'key_name', 'type': str, 'doc': 'The name of the key to be added.'})
    def _add_key(self, **kwargs):
        """
        Add a key to be used for making references to external resources.

        It is possible to use the same *key_name* to refer to different resources so long as the *key_name* is not
        used within the same object, relative_path, and field. To do so, this method must be called for the
        two different resources.

        The returned Key objects must be managed by the caller so as to be appropriately passed to subsequent calls
        to methods for storing information about the different resources.
        """
        key = kwargs['key_name']
        return Key(key, table=self.keys)

    @docval({'name': 'key', 'type': (str, Key), 'doc': 'The key to associate the entity with.'},
            {'name': 'resources_idx', 'type': (int, Resource), 'doc': 'The id of the resource.'},
            {'name': 'entity_id', 'type': str, 'doc': 'The unique entity id.'},
            {'name': 'entity_uri', 'type': str, 'doc': 'The URI for the entity.'})
    def _add_entity(self, **kwargs):
        """
        Add an entity that will be referenced to using the given key.
        """
        key = kwargs['key']
        resources_idx = kwargs['resources_idx']
        entity_id = kwargs['entity_id']
        entity_uri = kwargs['entity_uri']
        if not isinstance(key, Key):
            key = self._add_key(key)
        resource_entity = Entity(key, resources_idx, entity_id, entity_uri, table=self.entities)
        return resource_entity

    @docval({'name': 'resource', 'type': str, 'doc': 'The name of the ontology resource.'},
            {'name': 'uri', 'type': str, 'doc': 'The URI associated with ontology resource.'})
    def _add_resource(self, **kwargs):
        """
        Add resource name and URI to ResourceTable that will be referenced by the ResourceTable idx.
        """
        resource_name = kwargs['resource']
        uri = kwargs['uri']
        resource = Resource(resource_name, uri, table=self.resources)
        return resource

    @docval({'name': 'container', 'type': (str, AbstractContainer),
             'doc': 'The Container/Data object to add or the object id of the Container/Data object to add.'},
            {'name': 'relative_path', 'type': str,
             'doc': ('The relative_path of the attribute of the object that uses ',
                     'an external resource reference key. Use an empty string if not applicable.')},
            {'name': 'field', 'type': str, 'default': '',
             'doc': ('The field of the compound data type using an external resource.')})
    def _add_object(self, **kwargs):
        """
        Add an object that references an external resource.
        """
        container, relative_path, field = popargs('container', 'relative_path', 'field', kwargs)
        if isinstance(container, AbstractContainer):
            container = container.object_id
        obj = Object(container, relative_path, field, table=self.objects)
        return obj

    @docval({'name': 'obj', 'type': (int, Object), 'doc': 'The Object that uses the Key.'},
            {'name': 'key', 'type': (int, Key), 'doc': 'The Key that the Object uses.'})
    def _add_object_key(self, **kwargs):
        """
        Specify that an object (i.e. container and relative_path) uses a key to reference
        an external resource.
        """
        obj, key = popargs('obj', 'key', kwargs)
        return ObjectKey(obj, key, table=self.object_keys)

    @docval({'name': 'container', 'type': (str, AbstractContainer),
             'doc': ('The Container/Data object that uses the key or '
                     'the object id for the Container/Data object that uses the key.')},
            {'name': 'relative_path', 'type': str,
             'doc': ('The relative_path of the attribute of the object that uses ',
                     'an external resource reference key. Use an empty string if not applicable.'),
             'default': ''},
            {'name': 'field', 'type': str, 'default': '',
             'doc': ('The field of the compound data type using an external resource.')})
    def _check_object_field(self, container, relative_path, field):
        """
        Check if a container, relative path, and field have been added.

        The container can be either an object_id string or an AbstractContainer.

        If the container, relative_path, and field have not been added, add them
        and return the corresponding Object. Otherwise, just return the Object.
        """
        if isinstance(container, str):
            objecttable_idx = self.objects.which(object_id=container)
        else:
            objecttable_idx = self.objects.which(object_id=container.object_id)

        if len(objecttable_idx) > 0:
            relative_path_idx = self.objects.which(relative_path=relative_path)
            field_idx = self.objects.which(field=field)
            objecttable_idx = list(set(objecttable_idx) & set(relative_path_idx) & set(field_idx))

        if len(objecttable_idx) == 1:
            return self.objects.row[objecttable_idx[0]]
        elif len(objecttable_idx) == 0:
            return self._add_object(container, relative_path, field)
        else:
            raise ValueError("Found multiple instances of the same object id, relative path, "
                             "and field in objects table.")

    @docval({'name': 'key_name', 'type': str, 'doc': 'The name of the Key to get.'},
            {'name': 'container', 'type': (str, AbstractContainer), 'default': None,
             'doc': ('The Container/Data object that uses the key or '
                     'the object id for the Container/Data object that uses the key.')},
            {'name': 'relative_path', 'type': str,
             'doc': ('The relative_path of the attribute of the object that uses ',
                     'an external resource reference key. Use an empty string if not applicable.'),
             'default': ''},
            {'name': 'field', 'type': str, 'default': '',
             'doc': ('The field of the compound data type using an external resource.')})
    def get_key(self, **kwargs):
        """
        Return a Key or a list of Key objects that correspond to the given key.

        If container, relative_path, and field are provided, the Key that corresponds to the given name of the key
        for the given container, relative_path, and field is returned.
        """
        key_name, container, relative_path, field = popargs('key_name', 'container', 'relative_path', 'field', kwargs)
        key_idx_matches = self.keys.which(key=key_name)

        if container is not None:
            # if same key is used multiple times, determine
            # which instance based on the Container
            object_field = self._check_object_field(container, relative_path, field)
            for row_idx in self.object_keys.which(objects_idx=object_field.idx):
                key_idx = self.object_keys['keys_idx', row_idx]
                if key_idx in key_idx_matches:
                    return self.keys.row[key_idx]
            msg = ("No key '%s' found for container '%s', relative_path '%s', and field '%s'"
                   % (key_name, container, relative_path, field))
            raise ValueError(msg)
        else:
            if len(key_idx_matches) == 0:
                # the key has never been used before
                raise ValueError("key '%s' does not exist" % key_name)
            elif len(key_idx_matches) > 1:
                return [self.keys.row[i] for i in key_idx_matches]
            else:
                return self.keys.row[key_idx_matches[0]]

    @docval({'name': 'resource_name', 'type': str, 'doc': 'The name of the resource.'})
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
             'doc': ('The Container/Data object that uses the key or '
                     'the object_id for the Container/Data object that uses the key.')},
            {'name': 'attribute', 'type': str,
             'doc': 'The attribute of the container for the external reference.', 'default': None},
            {'name': 'field', 'type': str, 'default': '',
             'doc': ('The field of the compound data type using an external resource.')},
            {'name': 'key', 'type': (str, Key), 'default': None,
             'doc': 'The name of the key or the Key object from the KeyTable for the key to add a resource for.'},
            {'name': 'resources_idx', 'type': Resource, 'doc': 'The Resource from the ResourceTable.', 'default': None},
            {'name': 'resource_name', 'type': str, 'doc': 'The name of the resource to be created.', 'default': None},
            {'name': 'resource_uri', 'type': str, 'doc': 'The URI of the resource to be created.', 'default': None},
            {'name': 'entity_id', 'type': str, 'doc': 'The identifier for the entity at the resource.',
             'default': None},
            {'name': 'entity_uri', 'type': str, 'doc': 'The URI for the identifier at the resource.', 'default': None}
            )
    def add_ref(self, **kwargs):
        """
        Add information about an external reference used in this file.

        It is possible to use the same name of the key to refer to different resources
        so long as the name of the key is not used within the same object, relative_path, and
        field combination. This method does not support such functionality by default.
        """
        ###############################################################
        container = kwargs['container']
        attribute = kwargs['attribute']
        key = kwargs['key']
        field = kwargs['field']
        entity_id = kwargs['entity_id']
        entity_uri = kwargs['entity_uri']
        add_entity = False

        if attribute is None:  # Trivial Case
            relative_path = ''
            object_field = self._check_object_field(container, relative_path, field)
        else:  # DataType Attribute Case
            attribute_object = getattr(container, attribute)  # returns attribute object
            if isinstance(attribute_object, AbstractContainer):
                relative_path = ''
                object_field = self._check_object_field(attribute_object, relative_path, field)
            else:  # Non-DataType Attribute Case:
                obj_mapper = self.type_map.get_map(container)
                spec = obj_mapper.get_attr_spec(attr_name=attribute)
                parent_spec = spec.parent  # return the parent spec of the attribute
                if parent_spec.data_type is None:
                    while parent_spec.data_type is None:
                        parent_spec = parent_spec.parent  # find the closest parent with a data_type
                    parent_cls = self.type_map.get_dt_container_cls(data_type=parent_spec.data_type, autogen=False)
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
                    # this regex removes everything prior to the container on the absolute_path
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
             'doc': 'The Container/data object that is linked to resources/entities.'},
            {'name': 'relative_path', 'type': str,
             'doc': ('The relative_path of the attribute of the object that uses ',
                     'an external resource reference key. Use an empty string if not applicable.'),
             'default': ''},
            {'name': 'field', 'type': str, 'default': '',
             'doc': ('The field of the compound data type using an external resource.')})
    def get_object_resources(self, **kwargs):
        """
        Get all entities/resources associated with an object.
        """
        container = kwargs['container']
        relative_path = kwargs['relative_path']
        field = kwargs['field']

        keys = []
        entities = []
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
             'doc': 'The Key(s) to get external resource data for.'},
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
        used within the same object, relative_path, field. This method doesn't support such functionality by default. To
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

    @docval({'name': 'use_categories', 'type': bool, 'default': False,
             'doc': 'Use a multi-index on the columns to indicate which category each column belongs to.'},
            rtype=pd.DataFrame, returns='A DataFrame with all data merged into a flat, denormalized table.')
    def to_dataframe(self, **kwargs):
        """
        Convert the data from the keys, resources, entities, objects, and object_keys tables
        to a single joint dataframe. I.e., here data is being denormalized, e.g., keys that
        are used across multiple enities or objects will duplicated across the corresponding
        rows.

        Returns: :py:class:`~pandas.DataFrame` with all data merged into a single, flat, denormalized table.

        """
        use_categories = popargs('use_categories', kwargs)
        # Step 1: Combine the entities, keys, and resources,table
        entities_df = self.entities.to_dataframe()
        # Map the keys to the entities by 1) convert to dataframe, 2) select rows based on the keys_idx
        # from the entities table, expanding the dataframe to have the same number of rows as the
        # entities, and 3) reset the index to avoid duplicate values in the index, which causes errors when merging
        keys_mapped_df = self.keys.to_dataframe().iloc[entities_df['keys_idx']].reset_index(drop=True)
        # Map the resources to entities using the same strategy as for the keys
        resources_mapped_df = self.resources.to_dataframe().iloc[entities_df['resources_idx']].reset_index(drop=True)
        # Merge the mapped keys and resources with the entities tables
        entities_df = pd.concat(objs=[entities_df, keys_mapped_df, resources_mapped_df],
                                axis=1, verify_integrity=False)
        # Add a column for the entity id (for consistency with the other tables and to facilitate query)
        entities_df['entities_idx'] = entities_df.index

        # Step 2: Combine the the object_keys and objects tables
        object_keys_df = self.object_keys.to_dataframe()
        objects_mapped_df = self.objects.to_dataframe().iloc[object_keys_df['objects_idx']].reset_index(drop=True)
        object_keys_df = pd.concat(objs=[object_keys_df, objects_mapped_df],
                                   axis=1,
                                   verify_integrity=False)

        # Step 3: merge the combined entities_df and object_keys_df DataFrames
        result_df = pd.concat(
            # Create for each row in the objects_keys table a DataFrame with all corresponding data from all tables
            objs=[pd.merge(
                    # Find all entities that correspond to the row i of the object_keys_table
                    entities_df[entities_df['keys_idx'] == object_keys_df['keys_idx'].iloc[i]].reset_index(drop=True),
                    # Get a DataFrame for row i of the objects_keys_table
                    object_keys_df.iloc[[i, ]],
                    # Merge the entities and object_keys on the keys_idx column so that the values from the single
                    # object_keys_table row are copied across all corresponding rows in the entities table
                    on='keys_idx')
                  for i in range(len(object_keys_df))],
            # Concatenate the rows of the objs
            axis=0,
            verify_integrity=False)

        # Step 4: Clean up the index and sort columns by table type and name
        result_df.reset_index(inplace=True, drop=True)
        column_labels = [('objects', 'objects_idx'), ('objects', 'object_id'), ('objects', 'field'),
                         ('keys', 'keys_idx'), ('keys', 'key'),
                         ('resources', 'resources_idx'), ('resources', 'resource'), ('resources', 'resource_uri'),
                         ('entities', 'entities_idx'), ('entities', 'entity_id'), ('entities', 'entity_uri')]
        # sort the columns based on our custom order
        result_df = result_df.reindex(labels=[c[1] for c in column_labels],
                                      axis=1)
        # Add the categories if requested
        if use_categories:
            result_df.columns = pd.MultiIndex.from_tuples(column_labels)
        # return the result
        return result_df

    @docval({'name': 'db_file', 'type': str, 'doc': 'Name of the SQLite database file'},
            rtype=pd.DataFrame, returns='A DataFrame with all data merged into a flat, denormalized table.')
    def export_to_sqlite(self, db_file):
        """
        Save the keys, resources, entities, objects, and object_keys tables using sqlite3 to the given db_file.

        The function will first create the tables (if they do not already exist) and then
        add the data from this ExternalResource object to the database. If the database file already
        exists, then the data will be appended as rows to the existing database tables.

        Note, the index values of foreign keys (e.g., keys_idx, objects_idx, resources_idx) in the tables
        will not match between the ExternalResources here and the exported database, but they are adjusted
        automatically here, to ensure the foreign keys point to the correct rows in the exported database.
        This is because: 1) ExternalResources uses 0-based indexing for foreign keys, whereas SQLite uses
        1-based indexing and 2) if data is appended to existing tables then a corresponding additional
        offset must be applied to the relevant foreign keys.

        :raises: The function will raise errors if connection to the database fails. If
                 the given db_file already exists, then there is also the possibility that
                 certain updates may result in errors if there are collisions between the
                 new and existing data.
        """
        import sqlite3
        # connect to the database
        connection = sqlite3.connect(db_file)
        cursor = connection.cursor()
        # sql calls to setup the tables
        sql_create_keys_table = """ CREATE TABLE IF NOT EXISTS keys (
                                        id integer PRIMARY KEY,
                                        key text NOT NULL
                                    ); """
        sql_create_objects_table = """ CREATE TABLE IF NOT EXISTS objects (
                                            id integer PRIMARY KEY,
                                            object_id text NOT NULL,
                                            relative_path text NOT NULL,
                                            field text
                                       ); """
        sql_create_resources_table = """ CREATE TABLE IF NOT EXISTS resources (
                                             id integer PRIMARY KEY,
                                             resource text NOT NULL,
                                             resource_uri text NOT NULL
                                        ); """
        sql_create_object_keys_table = """ CREATE TABLE IF NOT EXISTS object_keys (
                                               id integer PRIMARY KEY,
                                               objects_idx int NOT NULL,
                                               keys_idx int NOT NULL,
                                               FOREIGN KEY (objects_idx) REFERENCES objects (id),
                                               FOREIGN KEY (keys_idx) REFERENCES keys (id)
                                        ); """
        sql_create_entities_table = """ CREATE TABLE IF NOT EXISTS entities (
                                             id integer PRIMARY KEY,
                                             keys_idx int NOT NULL,
                                             resources_idx int NOT NULL,
                                             entity_id text NOT NULL,
                                             entity_uri text NOT NULL,
                                             FOREIGN KEY (keys_idx) REFERENCES keys (id),
                                             FOREIGN KEY (resources_idx) REFERENCES resources (id)
                                        ); """
        # execute setting up the tables
        cursor.execute(sql_create_keys_table)
        cursor.execute(sql_create_objects_table)
        cursor.execute(sql_create_resources_table)
        cursor.execute(sql_create_object_keys_table)
        cursor.execute(sql_create_entities_table)

        # NOTE: sqlite uses a 1-based row-index so we need to update all foreign key columns accordingly
        # NOTE: If we are adding to an existing sqlite database then we need to also adjust for he number of rows
        keys_offset = len(cursor.execute('select * from keys;').fetchall()) + 1
        objects_offset = len(cursor.execute('select * from objects;').fetchall()) + 1
        resources_offset = len(cursor.execute('select * from resources;').fetchall()) + 1

        # populate the tables and fix foreign keys during insert
        cursor.executemany(" INSERT INTO keys(key) VALUES(?) ", self.keys[:])
        connection.commit()
        cursor.executemany(" INSERT INTO objects(object_id, relative_path, field) VALUES(?, ?, ?) ", self.objects[:])
        connection.commit()
        cursor.executemany(" INSERT INTO resources(resource, resource_uri) VALUES(?, ?) ", self.resources[:])
        connection.commit()
        cursor.executemany(
            " INSERT INTO object_keys(objects_idx, keys_idx) VALUES(?+%i, ?+%i) " % (objects_offset, keys_offset),
            self.object_keys[:])
        connection.commit()
        cursor.executemany(
            " INSERT INTO entities(keys_idx, resources_idx, entity_id, entity_uri) VALUES(?+%i, ?+%i, ?, ?) "
            % (keys_offset, resources_offset),
            self.entities[:])
        connection.commit()
        connection.close()
