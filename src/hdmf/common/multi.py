from . import register_class
from ..container import Container, Data, MultiContainerInterface
from ..utils import docval, call_docval_func, popargs


@register_class('SimpleMultiContainer')
class SimpleMultiContainer(MultiContainerInterface):

    __clsconf__ = {
        'attr': 'containers',
        'type': (Container, Data),
        'add': 'add_container',
        'get': 'get_container',
    }

    @docval({'name': 'name', 'type': str, 'doc': 'the name of this container'},
            {'name': 'containers', 'type': (list, tuple), 'default': None,
             'doc': 'the Container or Data objects in this file'})
    def __init__(self, **kwargs):
        containers = popargs('containers', kwargs)
        call_docval_func(super().__init__, kwargs)
        self.containers = containers
