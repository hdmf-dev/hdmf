import pandas as pd
import numpy as np
from . import register_class
from . import get_type_map
from ..container import Table, Row, Container, Data, AbstractContainer, HERDManager
from ..term_set import TermSet
from ..data_utils import DataIO
from ..utils import AllowPositional
from ..build import TypeMap
from ..term_set import TermSetWrapper
from glob import glob
import os
import zipfile
from collections import namedtuple
from warnings import warn
from ..typing import Bool, Int, validated


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
        {'name': 'files_idx', 'type': (int, np.integer),
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
        {'name': 'objects_idx', 'type': (int, np.integer, Object),
         'doc': 'The index into the objects table for the Object that uses the Key.'},
        {'name': 'keys_idx', 'type': (int, np.integer, Key),
         'doc': 'The index into the keys table that is used to make an external resource reference.'}
    )


class EntityKeyTable(Table):
    """
    A table for identifying which entities are used by which keys for referring to external resources.
    """

    __defaultname__ = 'entity_keys'

    __columns__ = (
        {'name': 'entities_idx', 'type': (int, np.integer, Entity),
         'doc': 'The index into the EntityTable for the Entity that associated with the Key.'},
        {'name': 'keys_idx', 'type': (int, np.integer, Key),
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


@register_class('HERD')
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

    @validated(allow_positional=AllowPositional.WARNING)
    def __init__(self,
                 keys: KeyTable | None = None,
                 files: FileTable | None = None,
                 entities: EntityTable | None = None,
                 objects: ObjectTable | None = None,
                 object_keys: ObjectKeyTable | None = None,
                 entity_keys: EntityKeyTable | None = None,
                 type_map: TypeMap | None = None):
        """Initialize the HERD container.

        Args:
            keys: The table storing user keys for referencing resources.
            files: The table for storing file ids used in external resources.
            entities: The table storing entity information.
            objects: The table storing object information.
            object_keys: The table storing object-key relationships.
            entity_keys: The table storing entity-key relationships.
            type_map: The type map. If None is provided, the HDMF-common type map will be used.
        """
        name = 'external_resources'
        super().__init__(name)
        self.keys = keys or KeyTable()
        self.files = files or FileTable()
        self.entities = entities or EntityTable()
        self.objects = objects or ObjectTable()
        self.object_keys = object_keys or ObjectKeyTable()
        self.entity_keys = entity_keys or EntityKeyTable()
        self.type_map = type_map or get_type_map()

    @staticmethod
    def assert_external_resources_equal(left, right, check_dtype=True):
        """
        Compare that the keys, files, entities, objects, object_keys, and entity_keys tables match

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
        for table_name in ('keys', 'files', 'objects', 'entities', 'object_keys', 'entity_keys'):
            try:
                pd.testing.assert_frame_equal(getattr(left, table_name).to_dataframe(),
                                              getattr(right, table_name).to_dataframe(),
                                              check_dtype=check_dtype)
            except AssertionError as e:
                errors.append(e)
        if len(errors) > 0:
            msg = ''.join(str(e)+"\n\n" for e in errors)
            raise AssertionError(msg)
        return True

    @validated
    def _add_key(self, key_name: str):
        """Add a key to be used for making references to external resources.

        It is possible to use the same *key_name* to refer to different resources so long as the *key_name* is not
        used within the same object, relative_path, and field. To do so, this method must be called for the
        two different resources.

        The returned Key objects must be managed by the caller so as to be appropriately passed to subsequent calls
        to methods for storing information about the different resources.

        Args:
            key_name: The name of the key to be added.
        """
        key = key_name
        return Key(key, table=self.keys)

    @validated
    def _add_file(self, file_object_id: str):
        """Add a file to be used for making references to external resources.

        This is optional when working in HDMF.

        Args:
            file_object_id: The id of the file
        """
        return File(file_object_id, table=self.files)

    @validated
    def _add_entity(self, entity_id: str, entity_uri: str):
        """Add an entity that will be referenced to using keys specified in HERD.entity_keys.

        Args:
            entity_id: The unique entity id.
            entity_uri: The URI for the entity.
        """
        entity = Entity( entity_id, entity_uri, table=self.entities)
        return entity

    @validated
    def _add_object(self,
                    container: str | AbstractContainer,
                    files_idx: Int | np.integer,
                    relative_path: str,
                    object_type: str | None = None,
                    field: str = ''):
        """Add an object that references an external resource.

        Args:
            container: The Container/Data object to add or the object id of the Container/Data object to add.
            files_idx: The file_object_id row idx.
            object_type: The type of the object. This is also the parent in relative_path. If omitted, the name of the
                container class is used.
            relative_path: The relative_path of the attribute of the object that uses an external resource reference
                key. Use an empty string if not applicable.
            field: The field of the compound data type using an external resource.
        """

        if object_type is None:
            object_type = container.__class__.__name__

        if isinstance(container, AbstractContainer):
            container = container.object_id
        obj = Object(files_idx, container, object_type, relative_path, field, table=self.objects)
        return obj

    @validated
    def _add_object_key(self, obj: Int | np.integer | Object, key: Int | np.integer | Key):
        """Specify that an object (i.e. container and relative_path) uses a key to reference
        an external resource.

        Args:
            obj: The Object that uses the Key.
            key: The Key that the Object uses.
        """
        return ObjectKey(obj, key, table=self.object_keys)

    @validated
    def _add_entity_key(self, entity: Int | np.integer | Entity, key: Int | np.integer | Key):
        """Add entity-key relationship to the EntityKeyTable.

        Args:
            entity: The Entity associated with the Key.
            key: The Key that the connected to the Entity.
        """
        return EntityKey(entity, key, table=self.entity_keys)

    def _find_object(self, file, container, relative_path, field):
        """
        Return the Object row matching ``file``, ``container``, ``relative_path``, and ``field``, or None.

        An object is identified by its file together with its object_id, relative_path, and field. The file
        is part of the identity because an object_id is not unique across files: a file can be copied and
        modified while keeping the same object_ids, so the same object_id may appear under different files.
        Matches are therefore restricted to objects belonging to ``file``.

        :returns: The matching Object row, or None if no object matches.
        :raises ValueError: If multiple matching objects exist (only possible via direct ``_add_object`` use).
        """
        matches = set(self.objects.which(object_id=container.object_id))
        if matches:
            matches &= set(self.objects.which(relative_path=relative_path))
            matches &= set(self.objects.which(field=field))
            # restrict to objects belonging to this file to disambiguate a shared object_id across files
            objects_in_file = set()
            for file_idx in self.files.which(file_object_id=file.object_id):
                objects_in_file |= set(self.objects.which(files_idx=file_idx))
            matches &= objects_in_file
        matches = list(matches)

        if len(matches) == 1:
            return self.objects.row[matches[0]]
        elif len(matches) > 1:  # pragma: no cover
            # It isn't possible for this to happen unless the user used _add_object directly.
            raise ValueError("Found multiple instances of the same object id, relative path, "
                             "and field in objects table.")
        return None

    def _find_or_add_object(self, file, container, relative_path, field):
        """
        Return the Object row for ``file``, ``container``, ``relative_path``, and ``field``, adding it
        (along with its file entry, if needed) when it is not already present.
        """
        object_field = self._find_object(file, container, relative_path, field)
        if object_field is not None:
            return object_field

        files_idx = self.files.which(file_object_id=file.object_id)
        if len(files_idx) > 1:  # pragma: no cover
            # It isn't possible for len(files_idx) > 1 without the user directly using _add_file
            raise ValueError("Found multiple instances of the same file.")
        elif len(files_idx) == 1:
            files_idx = files_idx[0]
        else:
            self._add_file(file.object_id)
            files_idx = self.files.which(file_object_id=file.object_id)[0]
        return self._add_object(files_idx=files_idx, container=container,
                                relative_path=relative_path, field=field)

    @validated
    def _get_file_from_container(self, container: str | AbstractContainer):
        """Method to retrieve a file associated with the container in the case a file is not provided.

        Args:
            container: The Container/Data object that uses the key or the object id for the Container/Data object that
                uses the key.
        """

        if isinstance(container, HERDManager):
            return container

        # Walk up the parent chain looking for the parent HERDManager.
        # In most practical cases, this will be the root file, however, it is
        # possible to construct a file that stores multiple Container objects
        # that each act as separate HERDManager for their child objects
        parent = container.parent
        while parent is not None:
            if isinstance(parent, HERDManager):
                return parent
            parent = parent.parent

        # No HERDManager was found in the container's ancestry. This happens when the
        # container has no parent (e.g., if it is has not been added to a file yet)  or because
        # none of its ancestors are a HERDManager
        msg = ("Could not find the file associated with container '%s'. Please add the container "
               "to the file before adding an external reference." % getattr(container, 'name', container))
        raise ValueError(msg)

    @validated
    def __check_termset_wrapper(self, objects: list):
        """Takes a list of objects and checks the fields for TermSetWrapper.

        wrapped_obj = namedtuple('wrapped_obj', ['object', 'attribute', 'wrapper'])
        :return: [wrapped_obj(object1, attribute_name1, wrapper1), ...]

        Args:
            objects: List of objects to check for TermSetWrapper within the fields.
        """

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

    @validated
    def add_ref_container(self, root_container: HERDManager):
        """Method to search through the root_container for all instances of TermSet.
        Currently, only datasets are supported. By using a TermSet, the data comes validated
        and can use the permissible values within the set to populate HERD.

        Args:
            root_container: The root container or file containing objects with a TermSet.
        """

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
                self.add_ref(container=container,
                             attribute=attr_name,
                             key=term,
                             entity_id=entity_id,
                             entity_uri=entity_uri)

    @validated
    def add_ref_termset(self,
                        termset: TermSet,
                        container: AbstractContainer | None = None,
                        attribute: str | None = None,
                        field: str = '',
                        key: str | Key | None = None):
        """This method allows users to take advantage of using the TermSet class to provide the entity information
        for add_ref, while also validating the data. This method supports adding a single key or an entire dataset
        to the HERD tables. For both cases, the term, i.e., key, will be validated against the permissible values
        in the TermSet. If valid, it will proceed to call add_ref. Otherwise, the method will return a dict of
        missing terms (terms not found in the TermSet).

        Args:
            container: The Container/Data object that uses the key.
            attribute: The attribute of the container for the external reference.
            field: The field of the compound data type using an external resource.
            key: The name of the key or the Key object from the KeyTable for the key to add a resource for.
            termset: The TermSet to be used if the container/attribute does not have one.
        """

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
            self.add_ref(container=container,
                         attribute=attribute,
                         key=term,
                         field=field,
                         entity_id=entity_id,
                         entity_uri=entity_uri)
        if len(missing_terms)>0:
            return {"missing_terms": missing_terms}

    def _resolve_object_target(self, container, attribute):
        """
        Resolve ``(container, attribute)`` to the ``(container, relative_path)`` that identify the object an
        external reference is attached to.

          - ``attribute`` is None: the reference is on the container itself (relative_path '').
          - ``attribute`` names a DataType (an AbstractContainer): the reference is on that sub-container.
          - ``attribute`` names a non-DataType attribute (e.g. ``DynamicTable.description``): the reference is on the
            nearest container ancestor of the attribute spec, with the relative_path computed from the spec path.

        :param container: The Container/Data object that the reference is attached to.
        :param attribute: The name of the attribute on the container, or None for the container itself.
        :returns: A ``(container, relative_path)`` tuple identifying the referenced object.
        :raises ValueError: If the container is not the nearest data_type to the attribute.
        """
        if attribute is None:  # Trivial Case
            return container, ''

        attribute_object = getattr(container, attribute)
        if isinstance(attribute_object, AbstractContainer):  # DataType Attribute Case
            return attribute_object, ''

        # Non-DataType Attribute Case: the reference is on the nearest container ancestor of the attribute spec
        obj_mapper = self.type_map.get_map(container)
        spec = obj_mapper.get_attr_spec(attr_name=attribute)
        parent_spec = spec.parent  # the parent spec of the attribute
        if parent_spec.data_type is None:
            while parent_spec.data_type is None:
                parent_spec = parent_spec.parent  # find the closest parent with a data_type
            parent_cls = self.type_map.get_dt_container_cls(data_type=parent_spec.data_type, autogen=False)
            if not isinstance(container, parent_cls):
                raise ValueError('Container not the nearest data_type')
        # strip everything prior to the container from the absolute spec path
        relative_path = spec.path[spec.path.find('/')+1:]
        return container, relative_path


    @validated
    def add_ref(self,  # noqa: C901
                entity_id: str,
                container: AbstractContainer | None = None,
                attribute: str | None = None,
                field: str = '',
                key: str | Key | None = None,
                entity_uri: str | None = None):
        """Add information about an external reference used in this file.

        It is possible to use the same name of the key to refer to different resources
        so long as the name of the key is not used within the same object, relative_path, and
        field combination. This method does not support such functionality by default.

        Args:
            container: The Container/Data object that uses the key.
            attribute: The attribute of the container for the external reference.
            field: The field of the compound data type using an external resource.
            key: The name of the key or the Key object from the KeyTable for the key to add a resource for. If not
                provided and ``attribute`` names a scalar string attribute, the value of that attribute is used as the
                key.
            entity_id: The identifier for the entity at the resource.
            entity_uri: The URI for the identifier at the resource.
        """
        ###############################################################
        if isinstance(container, Data):
            # Used when using the TermSetWrapper
            if attribute == 'data':
                attribute = None

        ##########################################
        # Default the key from a scalar attribute
        ##########################################
        if key is None and attribute is not None:
            if not isinstance(container, AbstractContainer):
                msg = ("Cannot default 'key' from attribute '%s' because 'container' is not a "
                       "Container/Data object. Provide 'key' explicitly." % attribute)
                raise ValueError(msg)
            attribute_value = getattr(container, attribute)
            if not isinstance(attribute_value, str):
                msg = ("Cannot default 'key' from attribute '%s' because its value is not a single "
                       "string. Provide 'key' explicitly." % attribute)
                raise ValueError(msg)
            key = attribute_value

        ##################
        # Resolve the file
        ##################
        # The file is always resolved from the container so that a reference can only be
        # added to a container that has already been added to a file. This raises a clear
        # error when the container is not in a file.
        file = self._get_file_from_container(container=container)

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
            # The existing entity_uri is always kept. Re-passing the same entity_uri is
            # harmless and common when annotating many objects/files with the same entity,
            # so only warn when a *different* entity_uri is provided.
            if entity_uri is not None and entity_uri != entity.entity_uri:
                msg = ("The provided entity_uri '%s' does not match the existing entity_uri '%s' "
                       "for entity_id '%s'. The existing entity_uri is kept."
                       % (entity_uri, entity.entity_uri, entity_id))
                warn(msg, stacklevel=3)

        # Resolve the object, adding it (and its file entry) to the tables if it is not already present.
        target_container, relative_path = self._resolve_object_target(container, attribute)
        object_field = self._find_or_add_object(file, target_container, relative_path, field)

        #######################################
        # Validate Parameters and Populate HERD
        #######################################
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

    @validated
    def get_key(self,
                key_name: str,
                file: HERDManager | None = None,
                container: AbstractContainer | None = None,
                relative_path: str = '',
                field: str = ''):
        """Return a Key.

        If container, relative_path, and field are provided, the Key that corresponds to the given name of the key
        for the given container, relative_path, and field is returned.

        If there are multiple matches, a list of all matching keys will be returned.

        Args:
            key_name: The name of the Key to get.
            file: The file associated with the container.
            container: The Container/Data object that uses the key.
            relative_path: The relative_path of the attribute of the object that uses an external resource reference
                key. Use an empty string if not applicable.
            field: The field of the compound data type using an external resource.
        """
        key_idx_matches = self.keys.which(key=key_name)


        if container is not None:
            if file is None:
                file = self._get_file_from_container(container=container)
            # if same key is used multiple times, determine
            # which instance based on the Container
            object_field = self._find_object(file, container, relative_path, field)
            if object_field is None:
                raise ValueError("Object not in Object Table.")
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

    @validated
    def get_entity(self, entity_id: str):
        """get_entity

        Args:
            entity_id: The ID for the identifier at the resource.
        """
        entity = self.entities.which(entity_id=entity_id)
        if len(entity)>0:
            return self.entities.row[entity[0]]
        else:
            return None

    @validated
    def get_object_type(self,
                        object_type: str,
                        relative_path: str = '',
                        field: str = '',
                        all_instances: Bool = False):
        """Get all entities/resources associated with an object_type.

        Args:
            object_type: The type of the object. This is also the parent in relative_path.
            relative_path: The relative_path of the attribute of the object that uses an external resource reference
                key. Use an empty string if not applicable.
            field: The field of the compound data type using an external resource.
            all_instances: The bool to return a dataframe with all instances of the object_type. If True,
                relative_path and field inputs will be ignored.
        """

        df = self.to_dataframe()

        if all_instances:
            df = df.loc[df['object_type'] == object_type]
        else:
            df = df.loc[(df['object_type'] == object_type)
                        & (df['relative_path'] == relative_path)
                        & (df['field'] == field)]
        return df

    @validated
    def get_object_entities(self,
                            container: AbstractContainer,
                            file: HERDManager | None = None,
                            attribute: str | None = None,
                            relative_path: str = '',
                            field: str = ''):
        """Get all entities/resources associated with an object.

        Args:
            file: The file.
            container: The Container/data object that is linked to resources/entities.
            attribute: The attribute of the container for the external reference.
            relative_path: The relative_path of the attribute of the object that uses an external resource reference
                key. Use an empty string if not applicable.
            field: The field of the compound data type using an external resource.
        """

        if file is None:
            file = self._get_file_from_container(container=container)

        keys = []
        entities = []
        if attribute is None:
            target_container, target_relative_path = container, relative_path
        else:
            # resolve the attribute the same way add_ref does so that a reference added with an
            # attribute can be retrieved with the same attribute
            target_container, target_relative_path = self._resolve_object_target(container, attribute)
        object_field = self._find_object(file, target_container, target_relative_path, field)
        if object_field is None:
            raise ValueError("Object not in Object Table.")
        # Find all keys associated with the object
        for row_idx in self.object_keys.which(objects_idx=object_field.idx):
            keys.append(self.object_keys['keys_idx', row_idx])
        # Find all the entities/resources for each key.
        for key_idx in keys:
            entity_key_row_idx = self.entity_keys.which(keys_idx=key_idx)
            for row_idx in entity_key_row_idx:
                entity_idx = self.entity_keys['entities_idx', row_idx]
                # coerce the row to a tuple so a read-back numpy structured-array row
                # (numpy.void) expands into columns the same as an in-memory list row
                entities.append(tuple(self.entities[entity_idx]))
        df = pd.DataFrame(entities, columns=['entity_id', 'entity_uri'])
        return df

    @validated
    def to_dataframe(self, use_categories: Bool = False) -> pd.DataFrame:
        """Convert the data from the keys, resources, entities, objects, and object_keys tables
        to a single joint dataframe. I.e., here data is being denormalized, e.g., keys that
        are used across multiple entities or objects will duplicated across the corresponding
        rows.

        Returns: :py:class:`~pandas.DataFrame` with all data merged into a single, flat, denormalized table.

        Args:
            use_categories: Use a multi-index on the columns to indicate which category each column belongs to.

        Returns:
            A DataFrame with all data merged into a flat, denormalized table.
        """
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
        files_df = self.files.to_dataframe()
        for idx in result_df['files_idx']:
            file_id_val = files_df.iloc[int(idx)]['file_object_id']
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

    def __flattened_dataframe_or_none(self):
        """Return the flattened ``to_dataframe()`` view, or None when there are no references.

        ``to_dataframe`` raises when the HERD holds no object-key relationships and may fail if the
        backing file is closed. The repr methods use this helper so they never raise on display.
        """
        if len(self.object_keys) == 0:
            return None
        try:
            return self.to_dataframe()
        except Exception:
            return None

    def __summary_line(self):
        """Return a one-line summary of the table sizes."""
        return ("%d key(s), %d entity(ies), %d object(s), %d file(s)"
                % (len(self.keys), len(self.entities), len(self.objects), len(self.files)))

    def __repr__(self):
        cls = self.__class__
        template = "%s %s.%s at 0x%d" % (self.name, cls.__module__, cls.__name__, id(self))
        template += "\n  " + self.__summary_line()
        df = self.__flattened_dataframe_or_none()
        if df is not None and len(df) > 0:
            template += "\n" + repr(df)
        return template

    def _repr_html_(self):
        """Generate an HTML representation that surfaces the references as a flattened table."""
        header_text = self.name if self.name == self.__class__.__name__ else \
            f"{self.name} ({self.__class__.__name__})"
        html_repr = self.css_style + self.js_script
        html_repr += "<div class='container-wrap'>"
        html_repr += f"<div class='container-header'><div class='xr-obj-type'><h3>{header_text}</h3></div></div>"
        html_repr += self._closed_file_warning_html()
        html_repr += f"<p class='container-fields'>{self.__summary_line()}</p>"
        df = self.__flattened_dataframe_or_none()
        if df is None or len(df) == 0:
            html_repr += "<p class='container-fields'>No external resource references.</p>"
        else:
            html_repr += df.to_html()
        html_repr += "</div>"
        return html_repr

    @validated
    def to_zip(self, path: str):
        """Write the tables in HERD to zipped tsv files.

        Args:
            path: The path to the zip file.
        """
        zip_file = path
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
    @classmethod
    @validated
    def get_zip_directory(cls, path: str):
        """Return the directory of the file given.

        Args:
            path: The path to the zip file.
        """
        directory = os.path.dirname(os.path.realpath(path))
        return directory

    @classmethod
    @classmethod
    @validated
    def from_zip(cls, path: str, type_map: TypeMap | None = None):
        """Method to read in zipped tsv files to populate HERD.

        Args:
            path: The path to the zip file.
            type_map: The TypeMap to use for the returned HERD. If None, the default TypeMap is used.
        """
        zip_file, type_map = path, type_map
        directory = cls.get_zip_directory(zip_file)

        with zipfile.ZipFile(zip_file, 'r') as zip:
            zip.extractall(directory)
        tsv_paths = glob(directory+'/*')

        # the tsv file name (without extension) matches both the table attribute name and the table class
        table_classes = {'files': FileTable,
                         'keys': KeyTable,
                         'entities': EntityTable,
                         'objects': ObjectTable,
                         'object_keys': ObjectKeyTable,
                         'entity_keys': EntityKeyTable}
        tables = {}
        for file in tsv_paths:
            name, ext = os.path.splitext(os.path.basename(file))
            table_cls = table_classes.get(name) if ext == '.tsv' else None
            if table_cls is None:
                continue
            df = pd.read_csv(file, sep='\t').replace(np.nan, '')
            tables[name] = table_cls().from_dataframe(df=df, name=name, extra_ok=False)
            os.remove(file)

        # check that the idx columns reference rows that exist in the target tables
        cls._assert_idx_in_range(tables['entity_keys']['entities_idx'], len(tables['entities']),
                                 "Entity Index out of range in EntityTable. Please check for alterations.")
        cls._assert_idx_in_range(tables['objects']['files_idx'], len(tables['files']),
                                 "File_ID Index out of range in ObjectTable. Please check for alterations.")
        cls._assert_idx_in_range(tables['object_keys']['objects_idx'], len(tables['objects']),
                                 "Object Index out of range in ObjectKeyTable. Please check for alterations.")
        cls._assert_idx_in_range(tables['object_keys']['keys_idx'], len(tables['keys']),
                                 "Key Index out of range in ObjectKeyTable. Please check for alterations.")
        cls._assert_idx_in_range(tables['entity_keys']['keys_idx'], len(tables['keys']),
                                 "Key Index out of range in EntityKeyTable. Please check for alterations.")

        er = cls(
            files=tables['files'],
            keys=tables['keys'],
            entities=tables['entities'],
            entity_keys=tables['entity_keys'],
            objects=tables['objects'],
            object_keys=tables['object_keys'],
            type_map=type_map,
        )
        return er

    @staticmethod
    def _assert_idx_in_range(indices, limit, msg):
        """Raise ``ValueError(msg)`` if any value in ``indices`` is not less than ``limit``."""
        for idx in indices:
            if not int(idx) < limit:
                raise ValueError(msg)
