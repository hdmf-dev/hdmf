import pandas as pd
import numpy as np
from . import register_class, EXP_NAMESPACE
from . import get_type_map
from ..container import Table, Row, Container, Data, AbstractContainer, HERDManager
from ..term_set import TermSet
from ..data_utils import DataIO
from ..utils import docval, popargs, AllowPositional
from ..build import TypeMap
from ..term_set import TermSetWrapper
from glob import glob
import os
import zipfile
from collections import namedtuple
from warnings import warn


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


class EntityKeyTable(Table):
    """
    A table for identifying which entities are used by which keys for referring to external resources.
    """

    __defaultname__ = 'entity_keys'

    __columns__ = (
        {'name': 'entities_idx', 'type': (int, Entity),
         'doc': 'The index into the EntityTable for the Entity that associated with the Key.'},
        {'name': 'keys_idx', 'type': (int, Key),
         'doc': 'The index into the KeyTable that is used to make an external resource reference.'}
    )


class EntityKey(Row):
    """
    A Row class for representing rows in the EntityKeyTable.
    """

    __table__ = EntityKeyTable


class ObjectKey(Row):
    """
    A Row class for representing rows in the ObjectKeyTable.
    """

    __table__ = ObjectKeyTable


@register_class('HERD', EXP_NAMESPACE)
class HERD(Container):
    """
    HDMF External Resources Data Structure.
    A table for mapping user terms (i.e. keys) to resource entities.
    """

    __fields__ = (
        {'name': 'keys', 'child': True},
        {'name': 'files', 'child': True},
        {'name': 'objects', 'child': True},
        {'name': 'object_keys', 'child': True},
        {'name': 'entity_keys', 'child': True},
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
             'doc': 'The table storing object-key relationships.'},
            {'name': 'entity_keys', 'type': EntityKeyTable, 'default': None,
             'doc': 'The table storing entity-key relationships.'},
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
        self.entity_keys = kwargs['entity_keys'] or EntityKeyTable()
        self.type_map = kwargs['type_map'] or get_type_map()

    @staticmethod
    def assert_external_resources_equal(left, right, check_dtype=True):
        """
        Compare that the keys, resources, entities, objects, and object_keys tables match

        :param left: HERD object to compare with right
        :param right: HERD object to compare with left
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

    @docval({'name': 'entity_id', 'type': str, 'doc': 'The unique entity id.'},
            {'name': 'entity_uri', 'type': str, 'doc': 'The URI for the entity.'})
    def _add_entity(self, **kwargs):
        """
        Add an entity that will be referenced to using keys specified in HERD.entity_keys.
        """
        entity_id = kwargs['entity_id']
        entity_uri = kwargs['entity_uri']
        entity = Entity( entity_id, entity_uri, table=self.entities)
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

    @docval({'name': 'entity', 'type': (int, Entity), 'doc': 'The Entity associated with the Key.'},
            {'name': 'key', 'type': (int, Key), 'doc': 'The Key that the connected to the Entity.'})
    def _add_entity_key(self, **kwargs):
        """
        Add entity-key relationship to the EntityKeyTable.
        """
        entity, key = popargs('entity', 'key', kwargs)
        return EntityKey(entity, key, table=self.entity_keys)

    @docval({'name': 'file',  'type': HERDManager, 'doc': 'The file associated with the container.'},
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

        if len(files_idx) > 1:  # pragma: no cover
            # It isn't possible for len(files_idx) > 1 without the user directly using _add_file
            raise ValueError("Found multiple instances of the same file.")
        elif len(files_idx) == 1:
            files_idx = files_idx[0]
        else:
            files_idx = None

        objecttable_idx = self.objects.which(object_id=container.object_id)

        if len(objecttable_idx) > 0:
            relative_path_idx = self.objects.which(relative_path=relative_path)
            field_idx = self.objects.which(field=field)
            objecttable_idx = list(set(objecttable_idx) & set(relative_path_idx) & set(field_idx))
        if len(objecttable_idx) == 1:
            return self.objects.row[objecttable_idx[0]]
        elif len(objecttable_idx) == 0 and create:
            # Used for add_ref
            return {'file_object_id': file_object_id,
                    'files_idx': files_idx,
                    'container': container,
                    'relative_path': relative_path,
                    'field': field}
        elif len(objecttable_idx) == 0 and not create:
            raise ValueError("Object not in Object Table.")
        else:  # pragma: no cover
            # It isn't possible for this to happen unless the user used _add_object.
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

        if isinstance(container, HERDManager):
            file = container
            return file
        else:
            parent = container.parent
            if parent is not None:
                while parent is not None:
                    if isinstance(parent, HERDManager):
                        file = parent
                        return file
                    else:
                        parent = parent.parent
            else:
                msg = 'Could not find file. Add container to the file.'
                raise ValueError(msg)

    @docval({'name': 'objects', 'type': list,
             'doc': 'List of objects to check for TermSetWrapper within the fields.'})
    def __check_termset_wrapper(self, **kwargs):
        """
        Takes a list of objects and checks the fields for TermSetWrapper.

        wrapped_obj = namedtuple('wrapped_obj', ['object', 'attribute', 'wrapper'])
        :return: [wrapped_obj(object1, attribute_name1, wrapper1), ...]
        """
        objects = kwargs['objects']

        ret = [] # list to be returned with the objects, attributes and corresponding termsets

        for obj in objects:
            # Get all the fields, parse out the methods and internal variables
            obj_fields = [a for a in dir(obj) if not a.startswith('_') and not callable(getattr(obj, a))]
            for attribute in obj_fields:
                attr = getattr(obj, attribute)
                if isinstance(attr, TermSetWrapper):
                    # Search objects that are wrapped
                    wrapped_obj = namedtuple('wrapped_obj', ['object', 'attribute', 'wrapper'])
                    ret.append(wrapped_obj(obj, attribute, attr))

        return ret

    @docval({'name': 'root_container', 'type': HERDManager,
             'doc': 'The root container or file containing objects with a TermSet.'})
    def add_ref_container(self, **kwargs):
        """
        Method to search through the root_container for all instances of TermSet.
        Currently, only datasets are supported. By using a TermSet, the data comes validated
        and can use the permissible values within the set to populate HERD.
        """
        root_container = kwargs['root_container']

        all_objects = root_container.all_children() # list of child objects and the container itself

        add_ref_items = self.__check_termset_wrapper(objects=all_objects)
        for ref in add_ref_items:
            container, attr_name, wrapper = ref
            if isinstance(wrapper.value, (list, np.ndarray, tuple)):
                values = wrapper.value
            else:
                # create list for single values (edge-case) for a simple iteration downstream
                values = [wrapper.value]
            for term in values:
                term_info = wrapper.termset[term]
                entity_id = term_info[0]
                entity_uri = term_info[2]
                self.add_ref(file=root_container,
                             container=container,
                             attribute=attr_name,
                             key=term,
                             entity_id=entity_id,
                             entity_uri=entity_uri)

    @docval({'name': 'file',  'type': HERDManager, 'doc': 'The file associated with the container.',
             'default': None},
            {'name': 'container', 'type': (str, AbstractContainer), 'default': None,
             'doc': ('The Container/Data object that uses the key or '
                     'the object_id for the Container/Data object that uses the key.')},
            {'name': 'attribute', 'type': str,
             'doc': 'The attribute of the container for the external reference.', 'default': None},
            {'name': 'field', 'type': str, 'default': '',
             'doc': ('The field of the compound data type using an external resource.')},
            {'name': 'key', 'type': (str, Key), 'default': None,
             'doc': 'The name of the key or the Key object from the KeyTable for the key to add a resource for.'},
            {'name': 'termset', 'type': TermSet,
             'doc': 'The TermSet to be used if the container/attribute does not have one.'}
            )
    def add_ref_termset(self, **kwargs):
        """
        This method allows users to take advantage of using the TermSet class to provide the entity information
        for add_ref, while also validating the data. This method supports adding a single key or an entire dataset
        to the HERD tables. For both cases, the term, i.e., key, will be validated against the permissible values
        in the TermSet. If valid, it will proceed to call add_ref. Otherwise, the method will return a dict of
        missing terms (terms not found in the TermSet).
        """
        file = kwargs['file']
        container = kwargs['container']
        attribute = kwargs['attribute']
        key = kwargs['key']
        field = kwargs['field']
        termset = kwargs['termset']

        if file is None:
            file = self._get_file_from_container(container=container)
        # if key is provided then add_ref proceeds as normal
        if key is not None:
            data = [key]
        else:
            # if the key is not provided, proceed to "bulk add"
            if attribute is None:
                data_object = container
            else:
                data_object = getattr(container, attribute)
            if isinstance(data_object, (Data, DataIO)):
                data = data_object.data
            elif isinstance(data_object, (list, tuple, np.ndarray)):
                data = data_object
            else:
                msg = ("The data object being used is not supported. "
                       "Please review the documentation for supported types.")
                raise ValueError(msg)
        missing_terms = []
        for term in data:
            # check the data according to the permissible_values
            try:
                term_info = termset[term]
            except ValueError:
                missing_terms.append(term)
                continue
            entity_id = term_info[0]
            entity_uri = term_info[2]
            self.add_ref(file=file,
                         container=container,
                         attribute=attribute,
                         key=term,
                         field=field,
                         entity_id=entity_id,
                         entity_uri=entity_uri)
        if len(missing_terms)>0:
            return {"missing_terms": missing_terms}

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
            {'name': 'entity_uri', 'type': str, 'doc': 'The URI for the identifier at the resource.', 'default': None},
            {'name': 'file',  'type': HERDManager, 'doc': 'The file associated with the container.',
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
        if isinstance(container, Data):
            # Used when using the TermSetWrapper
            if attribute == 'data':
                attribute = None
        key = kwargs['key']
        field = kwargs['field']
        entity_id = kwargs['entity_id']
        entity_uri = kwargs['entity_uri']
        file = kwargs['file']

        ##################
        # Set File if None
        ##################
        if file is None:
            file = self._get_file_from_container(container=container)
        # TODO: Add this once you've created a HDMF_file to rework testing
        # else:
        #     file_from_container = self._get_file_from_container(container=container)
        #     if file.object_id != file_from_container.object_id:
        #         msg = "The file given does not match the file in which the container is stored."
        #         raise ValueError(msg)

        ################
        # Set Key Checks
        ################
        add_key = False
        add_object_key = False
        check_object_key = False
        if not isinstance(key, Key):
            add_key = True
            add_object_key = True
        else:
            # Check to see that the existing key is being used with the object.
            # If true, do nothing. If false, create a new obj/key relationship
            # in the ObjectKeyTable
            check_object_key = True

        ###################
        # Set Entity Checks
        ###################
        add_entity_key = False
        add_entity = False

        entity = self.get_entity(entity_id=entity_id)
        check_entity_key = False
        if entity is None:
            if entity_uri is None:
                msg = 'New entities must have an entity_uri.'
                raise ValueError(msg)

            add_entity = True
            add_entity_key = True
        else:
            # The entity exists and so we need to check if an entity_key exists
            # for this entity and key combination.
            check_entity_key = True
            if entity_uri is not None:
                entity_uri = entity.entity_uri
                msg = 'This entity already exists. Ignoring new entity uri'
                warn(msg, stacklevel=2)

        #################
        # Validate Object
        #################
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

        #######################################
        # Validate Parameters and Populate HERD
        #######################################
        if isinstance(object_field, dict):
            # Create the object and file
            if object_field['files_idx'] is None:
                self._add_file(object_field['file_object_id'])
                object_field['files_idx'] = self.files.which(file_object_id=object_field['file_object_id'])[0]
            object_field = self._add_object(files_idx=object_field['files_idx'],
                                            container=object_field['container'],
                                            relative_path=object_field['relative_path'],
                                            field=object_field['field'])

        if add_key:
            # Now that object_field is set, we need to check if
            # the key has been associated with that object.
            # If so, just reuse the key.
            key_exists = False
            key_idx_matches = self.keys.which(key=key)
            if len(key_idx_matches)!=0:
                for row_idx in self.object_keys.which(objects_idx=object_field.idx):
                    key_idx = self.object_keys['keys_idx', row_idx]
                    if key_idx in key_idx_matches:
                        key_exists = True # Make sure we don't add the key.
                        # Automatically resolve the key for keys associated with
                        # the same object.
                        key = self.keys.row[key_idx]

            if not key_exists:
                key = self._add_key(key)

        if check_object_key:
            # When using a Key Object, we want to still check for whether the key
            # has been used with the Object object. If not, add it to ObjectKeyTable.
            # If so, do nothing and add_object_key remains False.
            obj_key_exists = False
            key_idx = key.idx
            object_key_row_idx = self.object_keys.which(keys_idx=key_idx)
            if len(object_key_row_idx)!=0:
                # this means there exists rows where the key is in the ObjectKeyTable
                for row_idx in object_key_row_idx:
                    obj_idx = self.object_keys['objects_idx', row_idx]
                    if obj_idx == object_field.idx:
                        obj_key_exists = True
                        # this means there is already a object-key relationship recorded
                if not obj_key_exists:
                    # this means that though the key is there, there is no object-key relationship
                    add_object_key = True

        if add_object_key:
            self._add_object_key(object_field, key)

        if check_entity_key:
            # check for entity-key relationship in EntityKeyTable
            entity_key_check = False
            key_idx = key.idx
            entity_key_row_idx = self.entity_keys.which(keys_idx=key_idx)
            if len(entity_key_row_idx)!=0:
                # this means there exists rows where the key is in the EntityKeyTable
                for row_idx in entity_key_row_idx:
                    entity_idx = self.entity_keys['entities_idx', row_idx]
                    if entity_idx == entity.idx:
                        entity_key_check = True
                        # this means there is already a entity-key relationship recorded
                if not entity_key_check:
                    # this means that though the key is there, there is no entity-key relationship
                    add_entity_key = True
            else:
                # this means that specific key is not in the EntityKeyTable, so add it and establish
                # the relationship with the entity
                add_entity_key = True

        if add_entity:
            entity = self._add_entity(entity_id, entity_uri)

        if add_entity_key:
            self._add_entity_key(entity, key)

    @docval({'name': 'key_name', 'type': str, 'doc': 'The name of the Key to get.'},
            {'name': 'file', 'type': HERDManager, 'doc': 'The file associated with the container.',
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

        If there are multiple matches, a list of all matching keys will be returned.
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
                return [self.keys.row[x] for x in key_idx_matches]
            else:
                return self.keys.row[key_idx_matches[0]]

    @docval({'name': 'entity_id', 'type': str, 'doc': 'The ID for the identifier at the resource.'})
    def get_entity(self, **kwargs):
        entity_id = kwargs['entity_id']
        entity = self.entities.which(entity_id=entity_id)
        if len(entity)>0:
            return self.entities.row[entity[0]]
        else:
            return None

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

    @docval({'name': 'file',  'type': HERDManager, 'doc': 'The file.',
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
            entity_key_row_idx = self.entity_keys.which(keys_idx=key_idx)
            for row_idx in entity_key_row_idx:
                entity_idx = self.entity_keys['entities_idx', row_idx]
                entities.append(self.entities.__getitem__(entity_idx))
        df = pd.DataFrame(entities, columns=['entity_id', 'entity_uri'])
        return df

    @docval({'name': 'use_categories', 'type': bool, 'default': False,
             'doc': 'Use a multi-index on the columns to indicate which category each column belongs to.'},
            rtype='pandas.DataFrame', returns='A DataFrame with all data merged into a flat, denormalized table.')
    def to_dataframe(self, **kwargs):
        """
        Convert the data from the keys, resources, entities, objects, and object_keys tables
        to a single joint dataframe. I.e., here data is being denormalized, e.g., keys that
        are used across multiple entities or objects will duplicated across the corresponding
        rows.

        Returns: :py:class:`~pandas.DataFrame` with all data merged into a single, flat, denormalized table.

        """
        use_categories = popargs('use_categories', kwargs)
        # Step 1: Combine the entities, keys, and entity_keys table
        ent_key_df = self.entity_keys.to_dataframe()
        entities_mapped_df = self.entities.to_dataframe().iloc[ent_key_df['entities_idx']].reset_index(drop=True)
        keys_mapped_df = self.keys.to_dataframe().iloc[ent_key_df['keys_idx']].reset_index(drop=True)
        ent_key_df = pd.concat(objs=[ent_key_df, entities_mapped_df, keys_mapped_df],
                                   axis=1,
                                   verify_integrity=False)
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
                    ent_key_df[ent_key_df['keys_idx'] == object_keys_df['keys_idx'].iloc[i]].reset_index(drop=True),
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

    @docval({'name': 'path', 'type': str, 'doc': 'The path to the zip file.'})
    def to_zip(self, **kwargs):
        """
        Write the tables in HERD to zipped tsv files.
        """
        zip_file = kwargs['path']
        directory = os.path.dirname(zip_file)

        files = [os.path.join(directory, child.name)+'.tsv' for child in self.children]
        for i in range(len(self.children)):
            df = self.children[i].to_dataframe()
            df.to_csv(files[i], sep='\t', index=False)

        with zipfile.ZipFile(zip_file, 'w') as zipF:
          for file in files:
              zipF.write(file)

        # remove tsv files
        for file in files:
            os.remove(file)

    @classmethod
    @docval({'name': 'path', 'type': str, 'doc': 'The path to the zip file.'})
    def get_zip_directory(cls, path):
        """
        Return the directory of the file given.
        """
        directory = os.path.dirname(os.path.realpath(path))
        return directory

    @classmethod
    @docval({'name': 'path', 'type': str, 'doc': 'The path to the zip file.'})
    def from_zip(cls, **kwargs):
        """
        Method to read in zipped tsv files to populate HERD.
        """
        zip_file = kwargs['path']
        directory = cls.get_zip_directory(zip_file)

        with zipfile.ZipFile(zip_file, 'r') as zip:
            zip.extractall(directory)
        tsv_paths = glob(directory+'/*')

        for file in tsv_paths:
            file_name = os.path.basename(file)
            if file_name == 'files.tsv':
                files_df = pd.read_csv(file, sep='\t').replace(np.nan, '')
                files = FileTable().from_dataframe(df=files_df, name='files', extra_ok=False)
                os.remove(file)
                continue
            if file_name == 'keys.tsv':
                keys_df = pd.read_csv(file, sep='\t').replace(np.nan, '')
                keys = KeyTable().from_dataframe(df=keys_df, name='keys', extra_ok=False)
                os.remove(file)
                continue
            if file_name == 'entities.tsv':
                entities_df = pd.read_csv(file, sep='\t').replace(np.nan, '')
                entities = EntityTable().from_dataframe(df=entities_df, name='entities', extra_ok=False)
                os.remove(file)
                continue
            if file_name == 'objects.tsv':
                objects_df = pd.read_csv(file, sep='\t').replace(np.nan, '')
                objects = ObjectTable().from_dataframe(df=objects_df, name='objects', extra_ok=False)
                os.remove(file)
                continue
            if file_name == 'object_keys.tsv':
                object_keys_df = pd.read_csv(file, sep='\t').replace(np.nan, '')
                object_keys = ObjectKeyTable().from_dataframe(df=object_keys_df, name='object_keys', extra_ok=False)
                os.remove(file)
                continue
            if file_name == 'entity_keys.tsv':
                ent_key_df = pd.read_csv(file, sep='\t').replace(np.nan, '')
                entity_keys = EntityKeyTable().from_dataframe(df=ent_key_df, name='entity_keys', extra_ok=False)
                os.remove(file)
                continue

        # we need to check the idx columns in entities, objects, and object_keys
        entity_idx = entity_keys['entities_idx']
        for idx in entity_idx:
            if not int(idx) < len(entities):
                msg = "Entity Index out of range in EntityTable. Please check for alterations."
                raise ValueError(msg)

        files_idx = objects['files_idx']
        for idx in files_idx:
            if not int(idx) < len(files):
                msg = "File_ID Index out of range in ObjectTable. Please check for alterations."
                raise ValueError(msg)

        object_idx = object_keys['objects_idx']
        for idx in object_idx:
            if not int(idx) < len(objects):
                msg = "Object Index out of range in ObjectKeyTable. Please check for alterations."
                raise ValueError(msg)

        keys_idx = object_keys['keys_idx']
        for idx in keys_idx:
            if not int(idx) < len(keys):
                msg = "Key Index out of range in ObjectKeyTable. Please check for alterations."
                raise ValueError(msg)

        keys_idx = entity_keys['keys_idx']
        for idx in keys_idx:
            if not int(idx) < len(keys):
                msg = "Key Index out of range in EntityKeyTable. Please check for alterations."
                raise ValueError(msg)


        er = HERD(files=files,
                               keys=keys,
                               entities=entities,
                               entity_keys=entity_keys,
                               objects=objects,
                               object_keys=object_keys)
        return er
