class OrphanContainerWarning(UserWarning):
    """
    Raised when a container does not have a parent.

    Only the top level container (e.g. file) should be
    without a parent
    """
    pass


class MissingRequiredWarning(UserWarning):
    """
    Raised when a required field is missing.
    """
    pass


class DtypeConversionWarning(UserWarning):
    """
    Raised when a value is converted to a different data type in order to match the specification.
    """
    pass
