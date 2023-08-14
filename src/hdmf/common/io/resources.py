from .. import register_map
from ..resources import HERD, KeyTable, FileTable, ObjectTable, ObjectKeyTable, EntityTable, EntityKeyTable
from ...build import ObjectMapper


@register_map(HERD)
class HERDMap(ObjectMapper):

    def construct_helper(self, name, parent_builder, table_cls, manager):
        """Create a new instance of table_cls with data from parent_builder[name].

           The DatasetBuilder for name is associated with data_type Data and container class Data,
           but users should use the more specific table_cls for these datasets.
        """
        parent = manager._get_proxy_builder(parent_builder)
        builder = parent_builder[name]
        src = builder.source
        oid = builder.attributes.get(self.spec.id_key())
        kwargs = dict(name=builder.name, data=builder.data)
        return self.__new_container__(table_cls, src, parent, oid, **kwargs)

    @ObjectMapper.constructor_arg('keys')
    def keys(self, builder, manager):
        return self.construct_helper('keys', builder, KeyTable, manager)

    @ObjectMapper.constructor_arg('files')
    def files(self, builder, manager):
        return self.construct_helper('files', builder, FileTable, manager)

    @ObjectMapper.constructor_arg('entities')
    def entities(self, builder, manager):
        return self.construct_helper('entities', builder, EntityTable, manager)

    @ObjectMapper.constructor_arg('objects')
    def objects(self, builder, manager):
        return self.construct_helper('objects', builder, ObjectTable, manager)

    @ObjectMapper.constructor_arg('object_keys')
    def object_keys(self, builder, manager):
        return self.construct_helper('object_keys', builder, ObjectKeyTable, manager)

    @ObjectMapper.constructor_arg('entity_keys')
    def entity_keys(self, builder, manager):
        return self.construct_helper('entity_keys', builder, EntityKeyTable, manager)
