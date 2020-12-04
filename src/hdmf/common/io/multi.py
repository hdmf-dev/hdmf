from .. import register_map
from ..multi import SimpleMultiContainer
from ...build import ObjectMapper
from ...container import Container, Data


@register_map(SimpleMultiContainer)
class SimpleMultiContainerMap(ObjectMapper):

    @ObjectMapper.object_attr('containers')
    def containers_attr(self, container, manager):
        return [c for c in container.containers.values() if isinstance(c, Container)]

    @ObjectMapper.constructor_arg('containers')
    def containers_carg(self, builder, manager):
        return [manager.construct(sub) for sub in builder.datasets.values()
                if manager.is_sub_data_type(sub, 'Data')] + \
               [manager.construct(sub) for sub in builder.groups.values()
                if manager.is_sub_data_type(sub, 'Container')]

    @ObjectMapper.object_attr('datas')
    def datas_attr(self, container, manager):
        return [c for c in container.containers.values() if isinstance(c, Data)]
