from numpy import dtype

from ..spec.spec import DtypeHelper
from ..utils import docval, getargs

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

    @docval({'name': 'name', 'type': str, 'doc': 'the name of the component that is erroneous'},
            {'name': 'reason', 'type': str, 'doc': 'the reason for the error'},
            {'name': 'location', 'type': str, 'doc': 'the location of the error', 'default': None})
    def __init__(self, **kwargs):
        self.__name = getargs('name', kwargs)
        self.__reason = getargs('reason', kwargs)
        self.__location = getargs('location', kwargs)
        if self.__location is not None:
            self.__str = "%s (%s): %s" % (self.__name, self.__location, self.__reason)
        else:
            self.__str = "%s: %s" % (self.name, self.reason)

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
        self.__str = "%s (%s): %s" % (self.__name, self.__location, self.__reason)

    def __str__(self):
        return self.__str

    def __repr__(self):
        return self.__str__()


class DtypeError(Error):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of the component that is erroneous'},
            {'name': 'expected', 'type': (dtype, type, str, list), 'doc': 'the expected dtype'},
            {'name': 'received', 'type': (dtype, type, str, list), 'doc': 'the received dtype'},
            {'name': 'location', 'type': str, 'doc': 'the location of the error', 'default': None})
    def __init__(self, **kwargs):
        name = getargs('name', kwargs)
        expected = getargs('expected', kwargs)
        received = getargs('received', kwargs)
        if isinstance(expected, list):
            expected = DtypeHelper.simplify_cpd_type(expected)
        reason = "incorrect type - expected '%s', got '%s'" % (expected, received)
        loc = getargs('location', kwargs)
        super().__init__(name, reason, location=loc)


class MissingError(Error):
    @docval({'name': 'name', 'type': str, 'doc': 'the name of the component that is erroneous'},
            {'name': 'location', 'type': str, 'doc': 'the location of the error', 'default': None})
    def __init__(self, **kwargs):
        name = getargs('name', kwargs)
        reason = "argument missing"
        loc = getargs('location', kwargs)
        super().__init__(name, reason, location=loc)


class MissingDataType(Error):
    @docval({'name': 'name', 'type': str, 'doc': 'the name of the component that is erroneous'},
            {'name': 'data_type', 'type': str, 'doc': 'the missing data type'},
            {'name': 'location', 'type': str, 'doc': 'the location of the error', 'default': None},
            {'name': 'missing_dt_name', 'type': str, 'doc': 'the name of the missing data type', 'default': None})
    def __init__(self, **kwargs):
        name, data_type, missing_dt_name = getargs('name', 'data_type', 'missing_dt_name', kwargs)
        self.__data_type = data_type
        if missing_dt_name is not None:
            reason = "missing data type %s (%s)" % (self.__data_type, missing_dt_name)
        else:
            reason = "missing data type %s" % self.__data_type
        loc = getargs('location', kwargs)
        super().__init__(name, reason, location=loc)

    @property
    def data_type(self):
        return self.__data_type


class IncorrectQuantityError(Error):
    """A validation error indicating that a child group/dataset/link has the incorrect quantity of matching elements"""
    @docval({'name': 'name', 'type': str, 'doc': 'the name of the component that is erroneous'},
            {'name': 'data_type', 'type': str, 'doc': 'the data type which has the incorrect quantity'},
            {'name': 'expected', 'type': (str, int), 'doc': 'the expected quantity'},
            {'name': 'received', 'type': (str, int), 'doc': 'the received quantity'},
            {'name': 'location', 'type': str, 'doc': 'the location of the error', 'default': None})
    def __init__(self, **kwargs):
        name, data_type, expected, received = getargs('name', 'data_type', 'expected', 'received', kwargs)
        reason = "expected a quantity of %s for data type %s, received %s" % (str(expected), data_type, str(received))
        loc = getargs('location', kwargs)
        super().__init__(name, reason, location=loc)


class ExpectedArrayError(Error):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of the component that is erroneous'},
            {'name': 'expected', 'type': (tuple, list), 'doc': 'the expected shape'},
            {'name': 'received', 'type': str, 'doc': 'the received data'},
            {'name': 'location', 'type': str, 'doc': 'the location of the error', 'default': None})
    def __init__(self, **kwargs):
        name = getargs('name', kwargs)
        expected = getargs('expected', kwargs)
        received = getargs('received', kwargs)
        reason = "incorrect shape - expected an array of shape '%s', got non-array data '%s'" % (expected, received)
        loc = getargs('location', kwargs)
        super().__init__(name, reason, location=loc)


class ShapeError(Error):

    @docval({'name': 'name', 'type': str, 'doc': 'the name of the component that is erroneous'},
            {'name': 'expected', 'type': (tuple, list), 'doc': 'the expected shape'},
            {'name': 'received', 'type': (tuple, list), 'doc': 'the received shape'},
            {'name': 'location', 'type': str, 'doc': 'the location of the error', 'default': None})
    def __init__(self, **kwargs):
        name = getargs('name', kwargs)
        expected = getargs('expected', kwargs)
        received = getargs('received', kwargs)
        reason = "incorrect shape - expected '%s', got '%s'" % (expected, received)
        loc = getargs('location', kwargs)
        super().__init__(name, reason, location=loc)


class IllegalLinkError(Error):
    """
    A validation error for indicating that a link was used where an actual object
    (i.e. a dataset or a group) must be used
    """

    @docval({'name': 'name', 'type': str, 'doc': 'the name of the component that is erroneous'},
            {'name': 'location', 'type': str, 'doc': 'the location of the error', 'default': None})
    def __init__(self, **kwargs):
        name = getargs('name', kwargs)
        reason = "illegal use of link (linked object will not be validated)"
        loc = getargs('location', kwargs)
        super().__init__(name, reason, location=loc)


class IncorrectDataType(Error):
    """
    A validation error for indicating that the incorrect data_type (not dtype) was used.
    """

    @docval({'name': 'name', 'type': str, 'doc': 'the name of the component that is erroneous'},
            {'name': 'expected', 'type': str, 'doc': 'the expected data_type'},
            {'name': 'received', 'type': str, 'doc': 'the received data_type'},
            {'name': 'location', 'type': str, 'doc': 'the location of the error', 'default': None})
    def __init__(self, **kwargs):
        name = getargs('name', kwargs)
        expected = getargs('expected', kwargs)
        received = getargs('received', kwargs)
        reason = "incorrect data_type - expected '%s', got '%s'" % (expected, received)
        loc = getargs('location', kwargs)
        super().__init__(name, reason, location=loc)
