import numpy as np
import pandas as pd

from . import register_class
from ..container import Table, Row, Container, AbstractContainer
from ..utils import docval, popargs


class KeyTable(Table):
    """
    A table for storing keys used to reference external resources
    """

    __defaultname__ = 'keys'

    __columns__ = (
        {'name': 'key_name', 'type': str,
         'doc': 'The user key that maps to the resource term / registry symbol.'},
    )


class Key(Row):
    """
    A Row class for representing rows in the KeyTable
    """

    __table__ = KeyTable


class ResourceTable(Table):
    """
    A table for storing the external resources a key refers to
    """

    __defaultname__ = 'resources'

    __columns__ = (
        {'name': 'keytable_idx', 'type': (int, Key),
         'doc': ('The index into the keys table for the user key that '
                 'maps to the resource term / registry symbol.')},
        {'name': 'resource_name', 'type': str,
         'doc': 'The resource/registry that the term/symbol comes from.'},
        {'name': 'resource_entity_id', 'type': str,
         'doc': 'The unique ID for the resource term / registry symbol.'},
        {'name': 'resource_entity_uri', 'type': str,
         'doc': 'The URI for the resource term / registry symbol.'},
    )


class ResourceEntity(Row):
    """
    A Row class for representing rows in the ResourceTable
    """

    __table__ = ResourceTable


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
        {'name': 'objecttable_idx', 'type': (int, Object),
         'doc': 'the index into the objects table for the object that uses the key'},
        {'name': 'keytable_idx', 'type': (int, Key),
         'doc': 'the index into the key table that is used to make an external resource reference'}
    )


class ObjectKey(Row):
    """
    A Row class for representing rows in the ObjectKeyTable
    """

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
        """
        Add a key to be used for making references to external resources

        It is possible to use the same *key_name* to refer to different resources so long as the *key_name* is not
        used within the same object and field. To do so, this method must be called for the two different resources.
        The returned Key objects must be managed by the caller so as to be appropriately passed to subsequent calls
        to methods for storing information about the different resources.
        """
        return Key(key, table=self.keys)

    @docval({'name': 'key', 'type': (str, Key), 'doc': 'the key to associate the resource with'},
            {'name': 'resource_name', 'type': str, 'doc': 'the name of the resource'},
            {'name': 'resource_entity_id', 'type': str, 'doc': 'the entity at the resource to associate'},
            {'name': 'resource_entity_uri', 'type': str, 'doc': 'the URL for the entity at the resource'})
    def add_resource(self, **kwargs):
        """
        Add an external resource that will be referenced to using the given key
        """
        key = kwargs['key']
        resource_name = kwargs['resource_name']
        resource_entity_id = kwargs['resource_entity_id']
        resource_entity_uri = kwargs['resource_entity_uri']
        if not isinstance(key, Key):
            key = self.add_key(key)
        resource_entity = ResourceEntity(key, resource_name, resource_entity_id, resource_entity_uri,
                                         table=self.resources)
        return resource_entity

    @docval({'name': 'container', 'type': (str, AbstractContainer),
             'doc': 'the Container/Data object to add or the object_id for the Container/Data object to add'},
            {'name': 'field', 'type': str, 'doc': 'the field on the Container to add'})
    def add_object(self, **kwargs):
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
    def add_external_reference(self, **kwargs):
        """
        Specify that an object (i.e. container and field) uses a key to reference
        an external resource
        """
        obj, key = popargs('obj', 'key', kwargs)
        return ObjectKey(obj, key, table=self.object_keys)

    def _check_object_field(self, container, field):
        """
        A helper function for checking if a container and field have been added.

        If the container and field have not been added, add the pair and return
        the corresponding Object. Otherwise, just return the Object.
        """
        objecttable_idx = self.objects.which(object_id=container)
        if len(objecttable_idx) > 0:
            field_idx = self.objects.which(field=field)
            objecttable_idx = list(set(objecttable_idx) & set(field_idx))

        if len(objecttable_idx) == 1:
            return self.objects.row[objecttable_idx[0]]
        elif len(objecttable_idx) == 0:
            return self.add_object(container, field)
        else:
            raise ValueError("Found multiple instances of the same object_id and field in object table")

    @docval({'name': 'key_name', 'type': str, 'doc': 'the name of the key to get'},
            {'name': 'container', 'type': (str, AbstractContainer), 'default': None,
             'doc': ('the Container/Data object that uses the key or '
                     'the object_id for the Container/Data object that uses the key')},
            {'name': 'field', 'type': str, 'doc': 'the field of the Container that uses the key', 'default': None})
    def get_key(self, **kwargs):
        """
        Return a Key or a list of Key objects that correspond to the given key_name.

        If container and field are provided, the Key that corresponds to the given key_name
        for the given container and field is returned.
        """
        key_name, container, field = popargs('key_name', 'container', 'field', kwargs)
        key_id = self.keys.which(key_name=key_name)
        if container is not None and field is not None:
            # if same key is used multiple times, determine
            # which instance based on the Container
            object_field = self._check_object_field(container, field)
            key_tmp = self.object_keys['keytable_idx', object_field.idx]
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

    @docval({'name': 'container', 'type': (str, AbstractContainer), 'default': None,
             'doc': ('the Container/Data object that uses the key or '
                     'the object_id for the Container/Data object that uses the key')},
            {'name': 'field', 'type': str, 'doc': 'the field of the Container/Data that uses the key', 'default': None},
            {'name': 'key', 'type': (str, Key), 'default': None,
             'doc': 'the name of the key or the Row object from the KeyTable for the key to add a resource for'},
            {'name': 'resource_name', 'type': str, 'doc': 'the online resource (i.e. database) name', 'default': None},
            {'name': 'entity_id', 'type': str, 'doc': 'the identifier for the entity at the resource', 'default': None},
            {'name': 'entity_uri', 'type': str, 'doc': 'the URI for the identifier at the resource', 'default': None})
    def add_ref(self, **kwargs):
        """
        Add information about an external reference used in this file.

        It is possible to use the same *key_name* to refer to different resources so long as the *key_name* is not
        used within the same object and field. This method does not support such functionality by default. The different
        keys must be added separately using *add_key* and passed to the *key* argument in separate calls of this method.
        """
        container = kwargs['container']
        field = kwargs['field']
        key = kwargs['key']
        resource_name = kwargs['resource_name']
        resource_id = kwargs['entity_id']
        resource_uri = kwargs['entity_uri']
        add_rsc = False

        if resource_name is not None and resource_id is not None and resource_uri is not None:
            add_rsc = True
        elif not (resource_name is None and resource_id is None and resource_uri is None):
            msg = ("Specify all or none of resource_name, entity_id, and entity_uri arguments. "
                   "All three are required to create a reference")
            raise ValueError(msg)

        if isinstance(container, Container):
            container = container.object_id

        object_field = self._check_object_field(container, field)

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

                key_tmp = self.object_keys['keytable_idx', object_field.idx]
                if key_tmp in key_id:
                    key = self.keys.row[key_tmp]
                else:
                    key = self.add_key(key)
            else:
                key = self.keys.row[key_id[0]]

        if add_rsc:
            resource_entity = self.add_resource(key, resource_name, resource_id, resource_uri)
            self.add_external_reference(object_field, key)

        return resource_entity

    @docval({'name': 'res_df', 'type': pd.DataFrame, 'doc': 'the DataFrame with all the keys and their resources'},
            rtype=dict, returns='a dict with the Key objects that were added')
    def add_keys(self, **kwargs):
        """
        Add key to be used for making references to external resources. This must be a DataFrame with the
        following columns:
            - *key_name*:              the key that will be used for referencing an external resource
            - *resource_name*:         the name of the external resource
            - *resource_entity_jd*:    the identifier for the entity at the external resource
            - *resource_entity_uri*:   the URI for the entity at the external resource

        It is possible to use the same *key_name* to refer to different resources so long as the *key_name* is not
        used within the same object and field. This method does not support such functionality. See *add_key* and
        *add_resource*.
        """
        res_df = popargs('res_df', kwargs)
        keys = res_df['key_name'].values
        ret = dict()
        for key in np.unique(keys):
            mask = keys == key
            ret[key] = self.add_key(key)
            for row in res_df[mask][[d['name'] for d in self.resources.__columns__[1:]]].to_dict('records'):
                self.add_resource(ret[key], *row.values())
        return ret

    @docval({'name': 'keys', 'type': (list, Key), 'default': None,
             'doc': 'the Key(s) to get external resource data for'},
            rtype=pd.DataFrame, returns='a DataFrame with keys and external resource data')
    def get_keys(self, **kwargs):
        """
        Return a DataFrame with information about keys used to make references to external resources.
        The DataFrame will contain the following columns:
            - *key_name*:              the key that will be used for referencing an external resource
            - *resource_name*:         the name of the external resource
            - *resource_entity_jd*:    the identifier for the entity at the external resource
            - *resource_entity_uri*:   the URI for the entity at the external resource

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
            rsc_ids = self.resources.which(keytable_idx=key.idx)
            for rsc_id in rsc_ids:
                rsc_row = self.resources.row[rsc_id].todict()
                rsc_row.pop('keytable_idx')
                rsc_row['key_name'] = key.key_name
                data.append(rsc_row)
        return pd.DataFrame(data=data, columns=['key_name', 'resource_name',
                                                'resource_entity_id', 'resource_entity_uri'])
