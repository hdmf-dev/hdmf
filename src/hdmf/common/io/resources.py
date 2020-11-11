from ...build import ObjectMapper
from ..resources import ExternalResources, KeyTable, ResourceTable, ObjectTable, ObjectKeyTable
from .. import register_map


@register_map(ExternalResources)
class ExternalResourcesMap(ObjectMapper):

    @classmethod
    def construct_helper(cls, name, builder, table_cls):
        builder = builder[name]
        return table_cls(name=name, data=builder.data)

    @ObjectMapper.constructor_arg('keys')
    def keys(self, builder, manager):
        return self.construct_helper('keys', builder, KeyTable)

    @ObjectMapper.constructor_arg('resources')
    def resources(self, builder, manager):
        return self.construct_helper('resources', builder, ResourceTable)

    @ObjectMapper.constructor_arg('objects')
    def objects(self, builder, manager):
        return self.construct_helper('objects', builder, ObjectTable)

    @ObjectMapper.constructor_arg('object_keys')
    def object_keys(self, builder, manager):
        return self.construct_helper('object_keys', builder, ObjectKeyTable)
