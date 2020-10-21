import numpy as np

from . import register_class
from ..container import Table, Container

from ..utils import docval, call_docval_func, popargs


def _check_id(table, id):
    if id >= 0 and len(table.which(id=id)) == 0:
        return np.uint64(id)
    else:
        raise ValueError('id must be a non-negative integer that is not already in the table: %d' % id)


@register_class('ResourceIdentiferMap')
class ResourceIdentiferMap(Table):

    __defaultname__ = 'resource_map'

    __columns__ = (
        {'name': 'id', 'type': (int, np.uint64), 'doc': 'The unique identifier in this table.'},
        {'name': 'key', 'type': str, 'doc': 'The user key that maps to the resource term / registry symbol.'},
        {'name': 'resource', 'type': str, 'doc': 'The resource/registry that the term/symbol comes from.'},
        {'name': 'uri', 'type': str, 'doc': 'The unique resource identifier for the resource term / registry symbol.'},
    )

    @docval(*__columns__)
    def add_row(self, **kwargs):
        id = popargs('id', kwargs)
        kwargs['id'] = _check_id(self, id)
        return super().add_row(kwargs)


@register_class('ResourceReferences')
class ResourceReferences(Table):

    __defaultname__ = 'references'

    __columns__ = (
        {'name': 'id', 'type': (int, np.uint64), 'doc': 'The unique identifier in this table.'},
        {'name': 'object_id', 'type': str, 'doc': 'The UUID for the object that uses this ontology term.'},
        {'name': 'field', 'type': str,
         'doc': 'The field from the object (specified by object_id) that uses this ontological term.'},
        {'name': 'item', 'type': (int, np.uint64),
         'doc': 'An index into the ResourceIdentiferMap that contains the term.'},
    )

    @docval(*__columns__)
    def add_row(self, **kwargs):
        id, item = popargs('id', 'item', kwargs)
        kwargs['id'] = _check_id(self, id)
        if item >= 0:
            kwargs['item'] = np.uint64(item)
        else:
            raise ValueError('item must be a non-negative integer: %d' % id)
        return super().add_row(kwargs)


@register_class('ExternalResources')
class ExternalResources(Container):

    __defaultname__ = 'external_resources'

    __fields__ = (
        {'name': 'resource_map', 'child': True},
        {'name': 'references', 'child': True},
    )

    @docval({'name': 'resource_map', 'type': ResourceIdentiferMap,
             'doc': 'the resource reference map for external resources', 'default': None},
            {'name': 'references', 'type': ResourceReferences,
             'doc': 'the references used in this file', 'default': None},
            {'name': 'name', 'type': str, 'doc': 'the name of this ExternalResources object', 'default': None})
    def __init__(self, **kwargs):
        resource_map, references = popargs('resource_map', 'references', kwargs)
        kwargs['name'] = kwargs['name'] or self.__defaultname__
        call_docval_func(super().__init__, kwargs)
        if resource_map is None:
            self.resource_map = ResourceIdentiferMap()
            if references is not None:
                raise ValueError('Cannot specify references without specifying the accompanying resource map')
        else:
            self.resource_map = resource_map
            self.references = references or ResourceReferences()

    def get_crid(self, object_id, field, key):
        """Return the CRIDs (tuple of (resource, URI) tuples) associated with the given object_id, field, and key.
        """

        # get the values in the item column where the values in the object_id and field columns match the arguments
        oid_idx_matches = self.references.which(object_id=object_id)
        field_col_idx = self.references.__colidx__.get('field')
        item_col_idx = self.references.__colidx__.get('item')
        terms_indices = list()
        for i in oid_idx_matches:
            row = self.references.data[i]
            field_val = row[field_col_idx]
            if field_val == field:
                item_val = row[item_col_idx]
                terms_indices.append(item_val)

        key_col_idx = ResourceIdentiferMap.__colidx__.get('key')
        resource_col_idx = ResourceIdentiferMap.__colidx__.get('resource')
        uri_col_idx = ResourceIdentiferMap.__colidx__.get('uri')

        ret = list()
        for i in terms_indices:
            terms_row = self.resource_map.data[i]
            key_val = terms_row[key_col_idx]
            if key_val == key:
                resource_val = terms_row[resource_col_idx]
                uri_val = terms_row[uri_col_idx]
                ret.append((resource_val, uri_val))

        return tuple(ret)
