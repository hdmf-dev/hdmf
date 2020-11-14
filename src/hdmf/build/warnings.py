class BuildWarning(UserWarning):
    """
    Base class for warnings that are raised during the building of a container.
    """
    pass


class IncorrectQuantityBuildWarning(BuildWarning):
    """
    Raised when a container field contains a number of groups/datasets/links that is not allowed by the spec.
    """
    pass


class MissingRequiredBuildWarning(BuildWarning):
    """
    Raised when a required field is missing.
    """
    pass


class MissingRequiredWarning(MissingRequiredBuildWarning):
    """
    Alias for MissingRequiredBuildWarning.
    """
    pass


class DtypeConversionWarning(UserWarning):
    """
    Raised when a value is converted to a different data type in order to match the specification.
    """
    pass
