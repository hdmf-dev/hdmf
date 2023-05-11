import pandas as pd
import numpy as np
from . import register_class, EXP_NAMESPACE
from . import get_type_map
from ..container import Table, Row, Container, AbstractContainer, ExternalResourcesManager
from ..utils import docval, popargs, AllowPositional
from ..build import TypeMap
from glob import glob
import os


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


class EntityTable(Table):
    """
    A table for storing the external resources a key refers to.
    """

    __defaultname__ = 'entities'

    __columns__ = (
        {'name': 'keys_idx', 'type': (int, Key),
         'doc': ('The index into the keys table for the user key that '
                 'maps to the resource term / registry symbol.')},
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


class FileTable(Table):
    """
    A table for storing file ids used in external resources.
    """

    __defaultname__ = 'files'

    __columns__ = (
        {'name': 'file_object_id', 'type': str,
         'doc': 'The file id of the file that contains the object'},
    )


class File(Row):
    """
    A Row class for representing rows in the FileTable.
    """

    __table__ = FileTable


class ObjectTable(Table):
    """
    A table for storing objects (i.e. Containers) that contain keys that refer to external resources.
    """

    __defaultname__ = 'objects'

    __columns__ = (
        {'name': 'files_idx', 'type': int,
         'doc': 'The row idx for the file_object_id in FileTable containing the object.'},
        {'name': 'object_id', 'type': str,
         'doc': 'The object ID for the Container/Data.'},
        {'name': 'object_type', 'type': str,
         'doc': 'The type of the object. This is also the parent in relative_path.'},
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
        {'name': 'files', 'child': True},
        {'name': 'objects', 'child': True},
        {'name': 'object_keys', 'child': True},
        {'name': 'entities', 'child': True},
    )

    @docval({'name': 'keys', 'type': KeyTable, 'default': None,
             'doc': 'The table storing user keys for referencing resources.'},
            {'name': 'files', 'type': FileTable, 'default': None,
             'doc': 'The table for storing file ids used in external resources.'},
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
        name = 'external_resources'
        super().__init__(name)
        self.keys = kwargs['keys'] or KeyTable()
        self.files = kwargs['files'] or FileTable()
        self.entities = kwargs['entities'] or EntityTable()
        self.objects = kwargs['objects'] or ObjectTable()
        self.object_keys = kwargs['object_keys'] or ObjectKeyTable()
        self.type_map = kwargs['type_map'] or get_type_map()

    @staticmethod
    def assert_external_resources_equal(left, right, check_dtype=True):
        """
        Compare that the keys, resources, entities, objects, and object_keys tables match

        :param left: ExternalResources object to compare with right
        :param right: ExternalResources object to compare with left
        :param check_dtype: Enforce strict checking of dtypes. Dtypes may be different
            for example for ids, where depending on how the data was saved
            ids may change from int64 to int32. (Default: True)
        :returns: The function returns True if all values match. If mismatches are found,
            AssertionError will be raised.
        :raises AssertionError: Raised if any differences are found. The function collects
            all differences into a single error so that the assertion will indicate
            all found differences.
        """
        errors = []
        try:
            pd.testing.assert_frame_equal(left.keys.to_dataframe(),
                                          right.keys.to_dataframe(),
                                          check_dtype=check_dtype)
        except AssertionError as e:
            errors.append(e)
        try:
            pd.testing.assert_frame_equal(left.files.to_dataframe(),
                                          right.files.to_dataframe(),
                                          check_dtype=check_dtype)
        except AssertionError as e:
            errors.append(e)
        try:
            pd.testing.assert_frame_equal(left.objects.to_dataframe(),
                                          right.objects.to_dataframe(),
                                          check_dtype=check_dtype)
        except AssertionError as e:
            errors.append(e)
        try:
            pd.testing.assert_frame_equal(left.entities.to_dataframe(),
                                          right.entities.to_dataframe(),
                                          check_dtype=check_dtype)
        except AssertionError as e:
            errors.append(e)
        try:
            pd.testing.assert_frame_equal(left.object_keys.to_dataframe(),
                                          right.object_keys.to_dataframe(),
                                          check_dtype=check_dtype)
        except AssertionError as e:
            errors.append(e)
        if len(errors) > 0:
            msg = ''.join(str(e)+"\n\n" for e in errors)
            raise AssertionError(msg)
        return True

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

    @docval({'name': 'file_object_id', 'type': str, 'doc': 'The id of the file'})
    def _add_file(self, **kwargs):
        """
        Add a file to be used for making references to external resources.

        This is optional when working in HDMF.
        """
        file_object_id = kwargs['file_object_id']
        return File(file_object_id, table=self.files)

    @docval({'name': 'key', 'type': (str, Key), 'doc': 'The key to associate the entity with.'},
            {'name': 'entity_id', 'type': str, 'doc': 'The unique entity id.'},
            {'name': 'entity_uri', 'type': str, 'doc': 'The URI for the entity.'})
    def _add_entity(self, **kwargs):
        """
        Add an entity that will be referenced to using the given key.
        """
        key = kwargs['key']
        entity_id = kwargs['entity_id']
        entity_uri = kwargs['entity_uri']
        if not isinstance(key, Key):
            key = self._add_key(key)
        entity = Entity(key, entity_id, entity_uri, table=self.entities)
        return entity

    @docval({'name': 'container', 'type': (str, AbstractContainer),
             'doc': 'The Container/Data object to add or the object id of the Container/Data object to add.'},
            {'name': 'files_idx', 'type': int,
             'doc': 'The file_object_id row idx.'},
            {'name': 'object_type', 'type': str, 'default': None,
             'doc': ('The type of the object. This is also the parent in relative_path. If omitted, '
                     'the name of the container class is used.')},
            {'name': 'relative_path', 'type': str,
             'doc': ('The relative_path of the attribute of the object that uses ',
                     'an external resource reference key. Use an empty string if not applicable.')},
            {'name': 'field', 'type': str, 'default': '',
             'doc': ('The field of the compound data type using an external resource.')})
    def _add_object(self, **kwargs):
        """
        Add an object that references an external resource.
        """
        files_idx, container, object_type, relative_path, field = popargs('files_idx',
                                                                          'container',
                                                                          'object_type',
                                                                          'relative_path',
                                                                          'field', kwargs)

        if object_type is None:
            object_type = container.__class__.__name__

        if isinstance(container, AbstractContainer):
            container = container.object_id
        obj = Object(files_idx, container, object_type, relative_path, field, table=self.objects)
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

    @docval({'name': 'file',  'type': ExternalResourcesManager, 'doc': 'The file associated with the container.'},
            {'name': 'container', 'type': AbstractContainer,
             'doc': ('The Container/Data object that uses the key or '
                     'the object id for the Container/Data object that uses the key.')},
            {'name': 'relative_path', 'type': str,
             'doc': ('The relative_path of the attribute of the object that uses ',
                     'an external resource reference key. Use an empty string if not applicable.'),
             'default': ''},
            {'name': 'field', 'type': str, 'default': '',
             'doc': ('The field of the compound data type using an external resource.')},
            {'name': 'create', 'type': bool, 'default': True})
    def _check_object_field(self, **kwargs):
        """
        Check if a container, relative path, and field have been added.

        The container can be either an object_id string or an AbstractContainer.

        If the container, relative_path, and field have not been added, add them
        and return the corresponding Object. Otherwise, just return the Object.
        """
        file = kwargs['file']
        container = kwargs['container']
        relative_path = kwargs['relative_path']
        field = kwargs['field']
        create = kwargs['create']
        file_object_id = file.object_id
        files_idx = self.files.which(file_object_id=file_object_id)

        if len(files_idx) > 1:
            raise ValueError("Found multiple instances of the same file.")
        elif len(files_idx) == 1:
            files_idx = files_idx[0]
        else:
            self._add_file(file_object_id)
            files_idx = self.files.which(file_object_id=file_object_id)[0]

        objecttable_idx = self.objects.which(object_id=container.object_id)

        if len(objecttable_idx) > 0:
            relative_path_idx = self.objects.which(relative_path=relative_path)
            field_idx = self.objects.which(field=field)
            objecttable_idx = list(set(objecttable_idx) & set(relative_path_idx) & set(field_idx))
        if len(objecttable_idx) == 1:
            return self.objects.row[objecttable_idx[0]]
        elif len(objecttable_idx) == 0 and create:
            return self._add_object(files_idx=files_idx, container=container, relative_path=relative_path, field=field)
        elif len(objecttable_idx) == 0 and not create:
            raise ValueError("Object not in Object Table.")
        else:
            raise ValueError("Found multiple instances of the same object id, relative path, "
                             "and field in objects table.")

    @docval({'name': 'container', 'type': (str, AbstractContainer),
             'doc': ('The Container/Data object that uses the key or '
                     'the object id for the Container/Data object that uses the key.')})
    def _get_file_from_container(self, **kwargs):
        """
        Method to retrieve a file associated with the container in the case a file is not provided.
        """
        container = kwargs['container']

        if isinstance(container, ExternalResourcesManager):
            file = container
            return file
        else:
            parent = container.parent
            if parent is not None:
                while parent is not None:
                    if isinstance(parent, ExternalResourcesManager):
                        file = parent
                        return file
                    else:
                        parent = parent.parent
            else:
                msg = 'Could not find file. Add container to the file.'
                raise ValueError(msg)

    @docval({'name': 'key_name', 'type': str, 'doc': 'The name of the Key to get.'},
            {'name': 'file', 'type': ExternalResourcesManager, 'doc': 'The file associated with the container.',
             'default': None},
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
        Return a Key.

        If container, relative_path, and field are provided, the Key that corresponds to the given name of the key
        for the given container, relative_path, and field is returned.
        """
        key_name, container, relative_path, field = popargs('key_name', 'container', 'relative_path', 'field', kwargs)
        key_idx_matches = self.keys.which(key=key_name)

        file = kwargs['file']

        if container is not None:
            if file is None:
                file = self._get_file_from_container(container=container)
            # if same key is used multiple times, determine
            # which instance based on the Container
            object_field = self._check_object_field(file=file,
                                                    container=container,
                                                    relative_path=relative_path,
                                                    field=field)
            for row_idx in self.object_keys.which(objects_idx=object_field.idx):
                key_idx = self.object_keys['keys_idx', row_idx]
                if key_idx in key_idx_matches:
                    return self.keys.row[key_idx]
            msg = "No key found with that container."
            raise ValueError(msg)
        else:
            if len(key_idx_matches) == 0:
                # the key has never been used before
                raise ValueError("key '%s' does not exist" % key_name)
            elif len(key_idx_matches) > 1:
                msg = "There are more than one key with that name. Please search with additional information."
                raise ValueError(msg)
            else:
                return self.keys.row[key_idx_matches[0]]

    @docval({'name': 'container', 'type': (str, AbstractContainer), 'default': None,
             'doc': ('The Container/Data object that uses the key or '
                     'the object_id for the Container/Data object that uses the key.')},
            {'name': 'attribute', 'type': str,
             'doc': 'The attribute of the container for the external reference.', 'default': None},
            {'name': 'field', 'type': str, 'default': '',
             'doc': ('The field of the compound data type using an external resource.')},
            {'name': 'key', 'type': (str, Key), 'default': None,
             'doc': 'The name of the key or the Key object from the KeyTable for the key to add a resource for.'},
            {'name': 'entity_id', 'type': str, 'doc': 'The identifier for the entity at the resource.'},
            {'name': 'entity_uri', 'type': str, 'doc': 'The URI for the identifier at the resource.'},
            {'name': 'file',  'type': ExternalResourcesManager, 'doc': 'The file associated with the container.',
             'default': None},
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
        file = kwargs['file']

        if file is None:
            file = self._get_file_from_container(container=container)

        if attribute is None:  # Trivial Case
            relative_path = ''
            object_field = self._check_object_field(file=file,
                                                    container=container,
                                                    relative_path=relative_path,
                                                    field=field)
        else:  # DataType Attribute Case
            attribute_object = getattr(container, attribute)  # returns attribute object
            if isinstance(attribute_object, AbstractContainer):
                relative_path = ''
                object_field = self._check_object_field(file=file,
                                                        container=attribute_object,
                                                        relative_path=relative_path,
                                                        field=field)
            else:  # Non-DataType Attribute Case:
                obj_mapper = self.type_map.get_map(container)
                spec = obj_mapper.get_attr_spec(attr_name=attribute)
                parent_spec = spec.parent  # return the parent spec of the attribute
                if parent_spec.data_type is None:
                    while parent_spec.data_type is None:
                        parent_spec = parent_spec.parent  # find the closest parent with a data_type
                    parent_cls = self.type_map.get_dt_container_cls(data_type=parent_spec.data_type, autogen=False)
                    if isinstance(container, parent_cls):
                        parent = container
                        # We need to get the path of the spec for relative_path
                        absolute_path = spec.path
                        relative_path = absolute_path[absolute_path.find('/')+1:]
                        object_field = self._check_object_field(file=file,
                                                                container=parent,
                                                                relative_path=relative_path,
                                                                field=field)
                    else:
                        msg = 'Container not the nearest data_type'
                        raise ValueError(msg)
                else:
                    parent = container  # container needs to be the parent
                    absolute_path = spec.path
                    relative_path = absolute_path[absolute_path.find('/')+1:]
                    # this regex removes everything prior to the container on the absolute_path
                    object_field = self._check_object_field(file=file,
                                                            container=parent,
                                                            relative_path=relative_path,
                                                            field=field)

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

        entity = self._add_entity(key, entity_id, entity_uri)

        return key, entity

    @docval({'name': 'object_type', 'type': str,
             'doc': 'The type of the object. This is also the parent in relative_path.'},
            {'name': 'relative_path', 'type': str,
             'doc': ('The relative_path of the attribute of the object that uses ',
                     'an external resource reference key. Use an empty string if not applicable.'),
             'default': ''},
            {'name': 'field', 'type': str, 'default': '',
             'doc': ('The field of the compound data type using an external resource.')},
            {'name': 'all_instances', 'type': bool, 'default': False,
             'doc': ('The bool to return a dataframe with all instances of the object_type.',
                     'If True, relative_path and field inputs will be ignored.')})
    def get_object_type(self, **kwargs):
        """
        Get all entities/resources associated with an object_type.
        """
        object_type = kwargs['object_type']
        relative_path = kwargs['relative_path']
        field = kwargs['field']
        all_instances = kwargs['all_instances']

        df = self.to_dataframe()

        if all_instances:
            df = df.loc[df['object_type'] == object_type]
        else:
            df = df.loc[(df['object_type'] == object_type)
                        & (df['relative_path'] == relative_path)
                        & (df['field'] == field)]
        return df

    @docval({'name': 'file',  'type': ExternalResourcesManager, 'doc': 'The file.',
             'default': None},
            {'name': 'container', 'type': (str, AbstractContainer),
             'doc': 'The Container/data object that is linked to resources/entities.'},
            {'name': 'attribute', 'type': str,
             'doc': 'The attribute of the container for the external reference.', 'default': None},
            {'name': 'relative_path', 'type': str,
             'doc': ('The relative_path of the attribute of the object that uses ',
                     'an external resource reference key. Use an empty string if not applicable.'),
             'default': ''},
            {'name': 'field', 'type': str, 'default': '',
             'doc': ('The field of the compound data type using an external resource.')})
    def get_object_entities(self, **kwargs):
        """
        Get all entities/resources associated with an object.
        """
        file = kwargs['file']
        container = kwargs['container']
        attribute = kwargs['attribute']
        relative_path = kwargs['relative_path']
        field = kwargs['field']

        if file is None:
            file = self._get_file_from_container(container=container)

        keys = []
        entities = []
        if attribute is None:
            object_field = self._check_object_field(file=file,
                                                    container=container,
                                                    relative_path=relative_path,
                                                    field=field,
                                                    create=False)
        else:
            object_field = self._check_object_field(file=file,
                                                    container=container[attribute],
                                                    relative_path=relative_path,
                                                    field=field,
                                                    create=False)
        # Find all keys associated with the object
        for row_idx in self.object_keys.which(objects_idx=object_field.idx):
            keys.append(self.object_keys['keys_idx', row_idx])
        # Find all the entities/resources for each key.
        for key_idx in keys:
            entity_idx = self.entities.which(keys_idx=key_idx)
            entities.append(list(self.entities.__getitem__(entity_idx[0])))
        df = pd.DataFrame(entities, columns=['keys_idx', 'entity_id', 'entity_uri'])

        key_names = []
        for idx in df['keys_idx']:
            key_id_val = self.keys.to_dataframe().iloc[int(idx)]['key']
            key_names.append(key_id_val)

        df['keys_idx'] = key_names
        df = df.rename(columns={'keys_idx': 'key_names', 'entity_id': 'entity_id', 'entity_uri': 'entity_uri'})
        return df

    @docval({'name': 'use_categories', 'type': bool, 'default': False,
             'doc': 'Use a multi-index on the columns to indicate which category each column belongs to.'},
            rtype=pd.DataFrame, returns='A DataFrame with all data merged into a flat, denormalized table.')
    def to_dataframe(self, **kwargs):
        """
        Convert the data from the keys, resources, entities, objects, and object_keys tables
        to a single joint dataframe. I.e., here data is being denormalized, e.g., keys that
        are used across multiple entities or objects will duplicated across the corresponding
        rows.

        Returns: :py:class:`~pandas.DataFrame` with all data merged into a single, flat, denormalized table.

        """
        use_categories = popargs('use_categories', kwargs)
        # Step 1: Combine the entities, keys, and files table
        entities_df = self.entities.to_dataframe()
        # Map the keys to the entities by 1) convert to dataframe, 2) select rows based on the keys_idx
        # from the entities table, expanding the dataframe to have the same number of rows as the
        # entities, and 3) reset the index to avoid duplicate values in the index, which causes errors when merging
        keys_mapped_df = self.keys.to_dataframe().iloc[entities_df['keys_idx']].reset_index(drop=True)
        # Map the resources to entities using the same strategy as for the keys
        # resources_mapped_df = self.resources.to_dataframe().iloc[entities_df['resources_idx']].reset_index(drop=True)
        # Merge the mapped keys and resources with the entities tables
        entities_df = pd.concat(objs=[entities_df, keys_mapped_df],
                                axis=1, verify_integrity=False)
        # Add a column for the entity id (for consistency with the other tables and to facilitate query)
        entities_df['entities_idx'] = entities_df.index

        # Step 2: Combine the the files, object_keys and objects tables
        object_keys_df = self.object_keys.to_dataframe()
        objects_mapped_df = self.objects.to_dataframe().iloc[object_keys_df['objects_idx']].reset_index(drop=True)
        object_keys_df = pd.concat(objs=[object_keys_df, objects_mapped_df],
                                   axis=1,
                                   verify_integrity=False)
        files_df = self.files.to_dataframe().iloc[object_keys_df['files_idx']].reset_index(drop=True)
        file_object_object_key_df = pd.concat(objs=[object_keys_df, files_df],
                                              axis=1,
                                              verify_integrity=False)
        # Step 3: merge the combined entities_df and object_keys_df DataFrames
        result_df = pd.concat(
            # Create for each row in the objects_keys table a DataFrame with all corresponding data from all tables
            objs=[pd.merge(
                    # Find all entities that correspond to the row i of the object_keys_table
                    entities_df[entities_df['keys_idx'] == object_keys_df['keys_idx'].iloc[i]].reset_index(drop=True),
                    # Get a DataFrame for row i of the objects_keys_table
                    file_object_object_key_df.iloc[[i, ]],
                    # Merge the entities and object_keys on the keys_idx column so that the values from the single
                    # object_keys_table row are copied across all corresponding rows in the entities table
                    on='keys_idx')
                  for i in range(len(object_keys_df))],
            # Concatenate the rows of the objs
            axis=0,
            verify_integrity=False)

        # Step 4: Clean up the index and sort columns by table type and name
        result_df.reset_index(inplace=True, drop=True)
        # ADD files
        file_id_col = []
        for idx in result_df['files_idx']:
            file_id_val = self.files.to_dataframe().iloc[int(idx)]['file_object_id']
            file_id_col.append(file_id_val)

        result_df['file_object_id'] = file_id_col
        column_labels = [('files', 'file_object_id'),
                         ('objects', 'objects_idx'), ('objects', 'object_id'), ('objects', 'files_idx'),
                         ('objects', 'object_type'), ('objects', 'relative_path'), ('objects', 'field'),
                         ('keys', 'keys_idx'), ('keys', 'key'),
                         ('entities', 'entities_idx'), ('entities', 'entity_id'), ('entities', 'entity_uri')]
        # sort the columns based on our custom order
        result_df = result_df.reindex(labels=[c[1] for c in column_labels],
                                      axis=1)
        result_df = result_df.astype({'keys_idx': 'uint32',
                                      'objects_idx': 'uint32',
                                      'files_idx': 'uint32',
                                      'entities_idx': 'uint32'})
        # Add the categories if requested
        if use_categories:
            result_df.columns = pd.MultiIndex.from_tuples(column_labels)
        # return the result
        return result_df

    @docval({'name': 'path', 'type': str, 'doc': 'path of the folder tsv file to write'})
    def to_norm_tsv(self, **kwargs):
        """
        Write the tables in ExternalResources to individual tsv files.
        """
        folder_path = kwargs['path']
        for child in self.children:
            df = child.to_dataframe()
            df.to_csv(folder_path+'/'+child.name+'.tsv', sep='\t', index=False)

    @classmethod
    @docval({'name': 'path', 'type': str, 'doc': 'path of the folder containing the tsv files to read'},
            returns="ExternalResources loaded from TSV", rtype="ExternalResources")
    def from_norm_tsv(cls, **kwargs):
        path = kwargs['path']
        tsv_paths = glob(path+'/*')

        for file in tsv_paths:
            file_name = os.path.basename(file)
            if file_name == 'files.tsv':
                files_df = pd.read_csv(file, sep='\t').replace(np.nan, '')
                files = FileTable().from_dataframe(df=files_df, name='files', extra_ok=False)
                continue
            if file_name == 'keys.tsv':
                keys_df = pd.read_csv(file, sep='\t').replace(np.nan, '')
                keys = KeyTable().from_dataframe(df=keys_df, name='keys', extra_ok=False)
                continue
            if file_name == 'entities.tsv':
                entities_df = pd.read_csv(file, sep='\t').replace(np.nan, '')
                entities = EntityTable().from_dataframe(df=entities_df, name='entities', extra_ok=False)
                continue
            if file_name == 'objects.tsv':
                objects_df = pd.read_csv(file, sep='\t').replace(np.nan, '')
                objects = ObjectTable().from_dataframe(df=objects_df, name='objects', extra_ok=False)
                continue
            if file_name == 'object_keys.tsv':
                object_keys_df = pd.read_csv(file, sep='\t').replace(np.nan, '')
                object_keys = ObjectKeyTable().from_dataframe(df=object_keys_df, name='object_keys', extra_ok=False)
                continue

        # we need to check the idx columns in entities, objects, and object_keys
        keys_idx = entities['keys_idx']
        for idx in keys_idx:
            if not int(idx) < keys.__len__():
                msg = "Key Index out of range in EntityTable. Please check for alterations."
                raise ValueError(msg)

        files_idx = objects['files_idx']
        for idx in files_idx:
            if not int(idx) < files.__len__():
                msg = "File_ID Index out of range in ObjectTable. Please check for alterations."
                raise ValueError(msg)

        object_idx = object_keys['objects_idx']
        for idx in object_idx:
            if not int(idx) < objects.__len__():
                msg = "Object Index out of range in ObjectKeyTable. Please check for alterations."
                raise ValueError(msg)

        keys_idx = object_keys['keys_idx']
        for idx in keys_idx:
            if not int(idx) < keys.__len__():
                msg = "Key Index out of range in ObjectKeyTable. Please check for alterations."
                raise ValueError(msg)

        er = ExternalResources(files=files,
                               keys=keys,
                               entities=entities,
                               objects=objects,
                               object_keys=object_keys)
        return er

    @docval({'name': 'path', 'type': str, 'doc': 'path of the tsv file to write'})
    def to_flat_tsv(self, **kwargs):
        """
        Write ExternalResources as a single, flat table to TSV
        Internally, the function uses :py:meth:`pandas.DataFrame.to_csv`. Pandas can
        infer compression based on the filename, i.e., by changing the file extension to
        '.gz', '.bz2', '.zip', '.xz', or '.zst' we can write compressed files.
        The TSV is formatted as follows: 1) line one indicates for each column the name of the table
        the column belongs to, 2) line two is the name of the column within the table, 3) subsequent
        lines are each a row in the flattened ExternalResources table. The first column is the
        row id in the flattened table and does not have a label, i.e., the first and second
        row will start with a tab character, and subsequent rows are numbered sequentially 1,2,3,... .

        See also :py:meth:`~hdmf.common.resources.ExternalResources.from_tsv`
        """  # noqa: E501
        path = popargs('path', kwargs)
        df = self.to_dataframe(use_categories=True)
        df.to_csv(path, sep='\t')

    @classmethod
    @docval({'name': 'path', 'type': str, 'doc': 'path of the tsv file to read'},
            returns="ExternalResources loaded from TSV", rtype="ExternalResources")
    def from_flat_tsv(cls, **kwargs):
        """
        Read ExternalResources from a flat tsv file
        Formatting of the TSV file is assumed to be consistent with the format
        generated by :py:meth:`~hdmf.common.resources.ExternalResources.to_tsv`.
        The function attempts to validate that the data in the TSV is consistent
        and parses the data from the denormalized table in the TSV to the
        normalized linked table structure used by ExternalResources.
        Currently the checks focus on ensuring that row id links between tables are valid.
        Inconsistencies in other (non-index) fields (e.g., when two rows with the same resource_idx
        have different resource_uri values) are not checked and will be ignored. In this case, the value
        from the first row that contains the corresponding entry will be kept.

        .. note::
           Since TSV files may be edited by hand or other applications, it is possible that data
           in the TSV may be inconsistent. E.g., object_idx may be missing if rows were removed
           and ids not updated. Also since the TSV is flattened into a single denormalized table
           (i.e., data are stored with duplication, rather than normalized across several tables),
           it is possible that values may be inconsistent if edited outside. E.g., we may have
           objects with the same index (object_idx) but different object_id, relative_path, or field
           values. While flat TSVs are sometimes preferred for ease of sharing, editing
           the TSV without using the :py:meth:`~hdmf.common.resources.ExternalResources` class
           should be done with great care!
        """
        def check_idx(idx_arr, name):
            """Check that indices are consecutively numbered without missing values"""
            idx_diff = np.diff(idx_arr)
            if np.any(idx_diff != 1):
                missing_idx = [i for i in range(np.max(idx_arr)) if i not in idx_arr]
                msg = "Missing %s entries %s" % (name, str(missing_idx))
                raise ValueError(msg)

        path = popargs('path', kwargs)
        df = pd.read_csv(path, header=[0, 1], sep='\t').replace(np.nan, '')
        # Construct the ExternalResources
        er = ExternalResources()
        # Retrieve all the Files
        files_idx, files_rows = np.unique(df[('objects', 'files_idx')], return_index=True)
        file_order = np.argsort(files_idx)
        files_idx = files_idx[file_order]
        files_rows = files_rows[file_order]
        # Check that files are consecutively numbered
        check_idx(idx_arr=files_idx, name='files_idx')
        files = df[('files', 'file_object_id')].iloc[files_rows]
        for file in zip(files):
            er._add_file(file_object_id=file[0])

        # Retrieve all the objects
        ob_idx, ob_rows = np.unique(df[('objects', 'objects_idx')], return_index=True)
        # Sort objects based on their index
        ob_order = np.argsort(ob_idx)
        ob_idx = ob_idx[ob_order]
        ob_rows = ob_rows[ob_order]
        # Check that objects are consecutively numbered
        check_idx(idx_arr=ob_idx, name='objects_idx')
        # Add the objects to the Object table
        ob_files = df[('objects', 'files_idx')].iloc[ob_rows]
        ob_ids = df[('objects', 'object_id')].iloc[ob_rows]
        ob_types = df[('objects', 'object_type')].iloc[ob_rows]
        ob_relpaths = df[('objects', 'relative_path')].iloc[ob_rows]
        ob_fields = df[('objects', 'field')].iloc[ob_rows]
        for ob in zip(ob_files, ob_ids, ob_types, ob_relpaths, ob_fields):
            er._add_object(files_idx=ob[0], container=ob[1], object_type=ob[2], relative_path=ob[3], field=ob[4])
        # Retrieve all keys
        keys_idx, keys_rows = np.unique(df[('keys', 'keys_idx')], return_index=True)
        # Sort keys based on their index
        keys_order = np.argsort(keys_idx)
        keys_idx = keys_idx[keys_order]
        keys_rows = keys_rows[keys_order]
        # Check that keys are consecutively numbered
        check_idx(idx_arr=keys_idx, name='keys_idx')
        # Add the keys to the Keys table
        keys_key = df[('keys', 'key')].iloc[keys_rows]
        all_added_keys = [er._add_key(k) for k in keys_key]

        # Add all the object keys to the ObjectKeys table. A single key may be assigned to multiple
        # objects. As such it is not sufficient to iterate over the unique ob_rows with the unique
        # objects, but we need to find all unique (objects_idx, keys_idx) combinations.
        ob_keys_idx = np.unique(df[[('objects', 'objects_idx'), ('keys', 'keys_idx')]], axis=0)
        for obk in ob_keys_idx:
            er._add_object_key(obj=obk[0], key=obk[1])

        # Retrieve all entities
        entities_idx, entities_rows = np.unique(df[('entities', 'entities_idx')], return_index=True)
        # Sort entities based on their index
        entities_order = np.argsort(entities_idx)
        entities_idx = entities_idx[entities_order]
        entities_rows = entities_rows[entities_order]
        # Check that entities are consecutively numbered
        check_idx(idx_arr=entities_idx, name='entities_idx')
        # Add the entities to the Resources table
        entities_id = df[('entities', 'entity_id')].iloc[entities_rows]
        entities_uri = df[('entities', 'entity_uri')].iloc[entities_rows]
        entities_keys = np.array(all_added_keys)[df[('keys', 'keys_idx')].iloc[entities_rows]]
        for e in zip(entities_keys, entities_id, entities_uri):
            er._add_entity(key=e[0], entity_id=e[1], entity_uri=e[2])
        # Return the reconstructed ExternalResources
        return er
