from .builders import Builder
from ..container import AbstractContainer
from ..utils import docval, getargs


class BuildError(Exception):
    """Error raised when building a container into a builder."""

    @docval({'name': 'builder', 'type': Builder, 'doc': 'the builder that cannot be built'},
            {'name': 'reason', 'type': str, 'doc': 'the reason for the error'})
    def __init__(self, **kwargs):
        self.__builder = getargs('builder', kwargs)
        self.__reason = getargs('reason', kwargs)
        self.__message = "%s (%s): %s" % (self.__builder.name, self.__builder.path, self.__reason)
        super().__init__(self.__message)


class OrphanContainerBuildError(BuildError):

    @docval({'name': 'builder', 'type': Builder, 'doc': 'the builder containing the broken link'},
            {'name': 'container', 'type': AbstractContainer, 'doc': 'the container that has no parent'})
    def __init__(self, **kwargs):
        builder = getargs('builder', kwargs)
        self.__container = getargs('container', kwargs)
        reason = ("Linked %s '%s' has no parent. Remove the link or ensure the linked container is added properly."
                  % (self.__container.__class__.__name__, self.__container.name))
        super().__init__(builder=builder, reason=reason)


class ReferenceTargetNotBuiltError(BuildError):

    @docval({'name': 'builder', 'type': Builder, 'doc': 'the builder containing the reference that cannot be found'},
            {'name': 'container', 'type': AbstractContainer, 'doc': 'the container that is not built yet'})
    def __init__(self, **kwargs):
        builder = getargs('builder', kwargs)
        self.__container = getargs('container', kwargs)
        reason = ("Could not find already-built Builder for %s '%s' in BuildManager"
                  % (self.__container.__class__.__name__, self.__container.name))
        super().__init__(builder=builder, reason=reason)


class ContainerConfigurationError(Exception):
    """Error raised when the container class is improperly configured."""
    pass


class ConstructError(Exception):
    """Error raised when constructing a container from a builder."""
