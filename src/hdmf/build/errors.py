class BuildError(Exception):
    """
    Raised when an error occurs while creating a builder from a container given a spec.
    """


class MismatchedTypeBuildError(BuildError):
    """
    Raised when creating a builder from a container given a spec and the data type of a container does not match
    the type specified by the corresponding group/dataset/attribute in the spec.
    """
