"""Module for build error definitions"""
from .builders import Builder
from ..container import AbstractContainer
from ..typing import validated


class BuildError(Exception):
    """Error raised when building a container into a builder."""

    @validated
    def __init__(self, builder: Builder, reason: str):
        """Initialize this object.

        Args:
            builder: the builder that cannot be built
            reason: the reason for the error
        """
        self.__builder = builder
        self.__reason = reason
        self.__message = "%s (%s): %s" % (self.__builder.name, self.__builder.path, self.__reason)
        super().__init__(self.__message)


class OrphanContainerBuildError(BuildError):

    @validated
    def __init__(self, builder: Builder, container: AbstractContainer):
        """Initialize this object.

        Args:
            builder: the builder containing the broken link
            container: the container that has no parent
        """
        self.__container = container
        reason = ("Linked %s '%s' has no parent. Remove the link or ensure the linked container is added properly."
                  % (self.__container.__class__.__name__, self.__container.name))
        super().__init__(builder=builder, reason=reason)


class ReferenceTargetNotBuiltError(BuildError):

    @validated
    def __init__(self, builder: Builder, container: AbstractContainer):
        """Initialize this object.

        Args:
            builder: the builder containing the reference that cannot be found
            container: the container that is not built yet
        """
        self.__container = container
        reason = ("Could not find already-built Builder for %s '%s' in BuildManager"
                  % (self.__container.__class__.__name__, self.__container.name))
        super().__init__(builder=builder, reason=reason)


class ContainerConfigurationError(Exception):
    """Error raised when the container class is improperly configured."""
    pass


class ConstructError(Exception):
    """Error raised when constructing a container from a builder."""
