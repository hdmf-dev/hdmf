from numpy import dtype

from ..spec.spec import DtypeHelper
from ..typing import Int, validated

__all__ = [
    "Error",
    "DtypeError",
    "MissingError",
    "ExpectedArrayError",
    "ShapeError",
    "MissingDataType",
    "IllegalLinkError",
    "IncorrectDataType",
    "IncorrectQuantityError"
]


class Error:

    @validated
    def __init__(self, name: str, reason: str, location: str | None = None):
        """Initialize this object.

        Args:
            name: the name of the component that is erroneous
            reason: the reason for the error
            location: the location of the error
        """
        self.__name = name
        self.__reason = reason
        self.__location = location

    @property
    def name(self):
        return self.__name

    @property
    def reason(self):
        return self.__reason

    @property
    def location(self):
        return self.__location

    @location.setter
    def location(self, loc):
        self.__location = loc

    def __str__(self):
        return self.__format_str(self.name, self.location, self.reason)

    @staticmethod
    def __format_str(name, location, reason):
        if location is not None:
            return "%s (%s): %s" % (name, location, reason)
        else:
            return "%s: %s" % (name, reason)

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        """Returns the hash value of this Error

        Note: if the location property is set after creation, the hash value will
        change. Therefore, it is important to finalize the value of location
        before getting the hash value.
        """
        return hash(self.__equatable_str())

    def __equatable_str(self):
        """A string representation of the error which can be used to check for equality

        For a single error, name can end up being different depending on whether it is
        generated from a base data type spec or from an inner type definition. These errors
        should still be considered equal because they are caused by the same problem.

        When a location is provided, we only consider the name of the field and drop the
        rest of the spec name. However, when a location is not available, then we need to
        use the fully-provided name.
        """
        if self.location is not None:
            equatable_name = self.name.split('/')[-1]
        else:
            equatable_name = self.name
        return self.__format_str(equatable_name, self.location, self.reason)

    def __eq__(self, other):
        return hash(self) == hash(other)


class DtypeError(Error):

    @validated
    def __init__(self,
                 name: str,
                 expected: dtype | type | str | list,
                 received: dtype | type | str | list,
                 location: str | None = None):
        """Initialize this object.

        Args:
            name: the name of the component that is erroneous
            expected: the expected dtype
            received: the received dtype
            location: the location of the error
        """
        if isinstance(expected, list):
            expected = DtypeHelper.simplify_cpd_type(expected)
        reason = "incorrect type - expected '%s', got '%s'" % (expected, received)
        loc = location
        super().__init__(name, reason, location=loc)


class MissingError(Error):
    @validated
    def __init__(self, name: str, location: str | None = None):
        """Initialize this object.

        Args:
            name: the name of the component that is erroneous
            location: the location of the error
        """
        reason = "argument missing"
        loc = location
        super().__init__(name, reason, location=loc)


class MissingDataType(Error):
    @validated
    def __init__(self, name: str, data_type: str, location: str | None = None, missing_dt_name: str | None = None):
        """Initialize this object.

        Args:
            name: the name of the component that is erroneous
            data_type: the missing data type
            location: the location of the error
            missing_dt_name: the name of the missing data type
        """
        self.__data_type = data_type
        if missing_dt_name is not None:
            reason = "missing data type %s (%s)" % (self.__data_type, missing_dt_name)
        else:
            reason = "missing data type %s" % self.__data_type
        loc = location
        super().__init__(name, reason, location=loc)

    @property
    def data_type(self):
        return self.__data_type


class IncorrectQuantityError(Error):
    """A validation error indicating that a child group/dataset/link has the incorrect quantity of matching elements"""
    @validated
    def __init__(self,
                 name: str,
                 data_type: str,
                 expected: str | Int,
                 received: str | Int,
                 location: str | None = None):
        """Initialize this object.

        Args:
            name: the name of the component that is erroneous
            data_type: the data type which has the incorrect quantity
            expected: the expected quantity
            received: the received quantity
            location: the location of the error
        """
        reason = "expected a quantity of %s for data type %s, received %s" % (str(expected), data_type, str(received))
        loc = location
        super().__init__(name, reason, location=loc)


class ExpectedArrayError(Error):

    @validated
    def __init__(self, name: str, expected: tuple | list, received: str, location: str | None = None):
        """Initialize this object.

        Args:
            name: the name of the component that is erroneous
            expected: the expected shape
            received: the received data
            location: the location of the error
        """
        reason = "incorrect shape - expected an array of shape '%s', got non-array data '%s'" % (expected, received)
        loc = location
        super().__init__(name, reason, location=loc)


class ShapeError(Error):

    @validated
    def __init__(self, name: str, expected: tuple | list, received: tuple | list, location: str | None = None):
        """Initialize this object.

        Args:
            name: the name of the component that is erroneous
            expected: the expected shape
            received: the received shape
            location: the location of the error
        """
        if isinstance(expected, (list, tuple)) and all(isinstance(e, (list, tuple)) for e in expected):
            allowable_shapes_str = " or ".join(map(str, expected))
        else:
            allowable_shapes_str = str(expected)
        allowable_shapes_str = allowable_shapes_str.replace("None", "*")
        reason = "incorrect shape - expected '%s', got '%s'" % (allowable_shapes_str, received)
        loc = location
        super().__init__(name, reason, location=loc)


class IllegalLinkError(Error):
    """
    A validation error for indicating that a link was used where an actual object
    (i.e. a dataset or a group) must be used
    """

    @validated
    def __init__(self, name: str, location: str | None = None):
        """Initialize this object.

        Args:
            name: the name of the component that is erroneous
            location: the location of the error
        """
        reason = "illegal use of link (linked object will not be validated)"
        loc = location
        super().__init__(name, reason, location=loc)


class IncorrectDataType(Error):
    """
    A validation error for indicating that the incorrect data_type (not dtype) was used.
    """

    @validated
    def __init__(self, name: str, expected: str, received: str, location: str | None = None):
        """Initialize this object.

        Args:
            name: the name of the component that is erroneous
            expected: the expected data_type
            received: the received data_type
            location: the location of the error
        """
        reason = "incorrect data_type - expected '%s', got '%s'" % (expected, received)
        loc = location
        super().__init__(name, reason, location=loc)
