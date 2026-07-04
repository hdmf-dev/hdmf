from . import register_class
from ..container import Container, Data, MultiContainerInterface
from ..typing import validated
from ..utils import AllowPositional


@register_class('SimpleMultiContainer')
class SimpleMultiContainer(MultiContainerInterface):

    __clsconf__ = {
        'attr': 'containers',
        'type': (Container, Data),
        'add': 'add_container',
        'get': 'get_container',
    }

    @validated(allow_positional=AllowPositional.WARNING)
    def __init__(self, name: str, containers: list | tuple | None = None):
        """Initialize the SimpleMultiContainer.

        Args:
            name: the name of this container
            containers: the Container or Data objects in this file
        """
        super().__init__(name=name)
        self.containers = containers
