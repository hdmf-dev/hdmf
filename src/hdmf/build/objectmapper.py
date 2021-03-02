import logging
import re
import warnings
from collections import OrderedDict
from copy import copy
from datetime import datetime

import numpy as np

from .builders import DatasetBuilder, GroupBuilder, LinkBuilder, Builder, ReferenceBuilder, RegionBuilder, BaseBuilder
from .errors import (BuildError, OrphanContainerBuildError, ReferenceTargetNotBuiltError, ContainerConfigurationError,
                     ConstructError)
from .manager import Proxy, BuildManager
from .warnings import MissingRequiredBuildWarning, DtypeConversionWarning, IncorrectQuantityBuildWarning
from ..container import AbstractContainer, Data, DataRegion
from ..data_utils import DataIO, AbstractDataChunkIterator
from ..query import ReferenceResolver
from ..spec import Spec, AttributeSpec, DatasetSpec, GroupSpec, LinkSpec, NAME_WILDCARD, RefSpec
from ..spec.spec import BaseStorageSpec
from ..utils import docval, getargs, ExtenderMeta, get_docval

_const_arg = '__constructor_arg'


@docval({'name': 'name', 'type': str, 'doc': 'the name of the constructor argument'},
        is_method=False)
def _constructor_arg(**kwargs):
    '''Decorator to override the default mapping scheme for a given constructor argument.

    Decorate ObjectMapper methods with this function when extending ObjectMapper to override the default
    scheme for mapping between AbstractContainer and Builder objects. The decorated method should accept as its
    first argument the Builder object that is being mapped. The method should return the value to be passed
    to the target AbstractContainer class constructor argument given by *name*.
    '''
    name = getargs('name', kwargs)

    def _dec(func):
        setattr(func, _const_arg, name)
        return func

    return _dec


_obj_attr = '__object_attr'


@docval({'name': 'name', 'type': str, 'doc': 'the name of the constructor argument'},
        is_method=False)
def _object_attr(**kwargs):
    '''Decorator to override the default mapping scheme for a given object attribute.

    Decorate ObjectMapper methods with this function when extending ObjectMapper to override the default
    scheme for mapping between AbstractContainer and Builder objects. The decorated method should accept as its
    first argument the AbstractContainer object that is being mapped. The method should return the child Builder
    object (or scalar if the object attribute corresponds to an AttributeSpec) that represents the
    attribute given by *name*.
    '''
    name = getargs('name', kwargs)

    def _dec(func):
        setattr(func, _obj_attr, name)
        return func

    return _dec


def _unicode(s):
    """
    A helper function for converting to Unicode
    """
    if isinstance(s, str):
        return s
    elif isinstance(s, bytes):
        return s.decode('utf-8')
    else:
        raise ValueError("Expected unicode or ascii string, got %s" % type(s))


def _ascii(s):
    """
    A helper function for converting to ASCII
    """
    if isinstance(s, str):
        return s.encode('ascii', 'backslashreplace')
    elif isinstance(s, bytes):
        return s
    else:
        raise ValueError("Expected unicode or ascii string, got %s" % type(s))


class ObjectMapper(metaclass=ExtenderMeta):
    '''A class for mapping between Spec objects and AbstractContainer attributes

    '''

    # mapping from spec dtypes to numpy dtypes or functions for conversion of values to spec dtypes
    # make sure keys are consistent between hdmf.spec.spec.DtypeHelper.primary_dtype_synonyms,
    # hdmf.build.objectmapper.ObjectMapper.__dtypes, hdmf.build.manager.TypeMap._spec_dtype_map,
    # hdmf.validate.validator.__allowable, and backend dtype maps
    __dtypes = {
        "float": np.float32,
        "float32": np.float32,
        "double": np.float64,
        "float64": np.float64,
        "long": np.int64,
        "int64": np.int64,
        "int": np.int32,
        "int32": np.int32,
        "short": np.int16,
        "int16": np.int16,
        "int8": np.int8,
        "uint": np.uint32,
        "uint64": np.uint64,
        "uint32": np.uint32,
        "uint16": np.uint16,
        "uint8": np.uint8,
        "bool": np.bool_,
        "text": _unicode,
        "utf": _unicode,
        "utf8": _unicode,
        "utf-8": _unicode,
        "ascii": _ascii,
        "bytes": _ascii,
        "isodatetime": _ascii,
        "datetime": _ascii,
    }

    __no_convert = set()

    @classmethod
    def __resolve_numeric_dtype(cls, given, specified):
        """
        Determine the dtype to use from the dtype of the given value and the specified dtype.
        This amounts to determining the greater precision of the two arguments, but also
        checks to make sure the same base dtype is being used. A warning is raised if the
        base type of the specified dtype differs from the base type of the given dtype and
        a conversion will result (e.g., float32 -> uint32).
        """
        g = np.dtype(given)
        s = np.dtype(specified)
        if g == s:
            return s.type, None
        if g.itemsize <= s.itemsize:  # given type has precision < precision of specified type
            # note: this allows float32 -> int32, bool -> int8, int16 -> uint16 which may involve buffer overflows,
            # truncated values, and other unexpected consequences.
            warning_msg = ('Value with data type %s is being converted to data type %s as specified.'
                           % (g.name, s.name))
            return s.type, warning_msg
        elif g.name[:3] == s.name[:3]:
            return g.type, None  # same base type, use higher-precision given type
        else:
            if np.issubdtype(s, np.unsignedinteger):
                # e.g.: given int64 and spec uint32, return uint64. given float32 and spec uint8, return uint32.
                ret_type = np.dtype('uint' + str(int(g.itemsize * 8)))
                warning_msg = ('Value with data type %s is being converted to data type %s (min specification: %s).'
                               % (g.name, ret_type.name, s.name))
                return ret_type.type, warning_msg
            if np.issubdtype(s, np.floating):
                # e.g.: given int64 and spec float32, return float64. given uint64 and spec float32, return float32.
                ret_type = np.dtype('float' + str(max(int(g.itemsize * 8), 32)))
                warning_msg = ('Value with data type %s is being converted to data type %s (min specification: %s).'
                               % (g.name, ret_type.name, s.name))
                return ret_type.type, warning_msg
            if np.issubdtype(s, np.integer):
                # e.g.: given float64 and spec int8, return int64. given uint32 and spec int8, return int32.
                ret_type = np.dtype('int' + str(int(g.itemsize * 8)))
                warning_msg = ('Value with data type %s is being converted to data type %s (min specification: %s).'
                               % (g.name, ret_type.name, s.name))
                return ret_type.type, warning_msg
            if s.type is np.bool_:
                msg = "expected %s, received %s - must supply %s" % (s.name, g.name, s.name)
                raise ValueError(msg)
            # all numeric types in __dtypes should be caught by the above
            raise ValueError('Unsupported conversion to specification data type: %s' % s.name)

    @classmethod
    def no_convert(cls, obj_type):
        """
        Specify an object type that ObjectMappers should not convert.
        """
        cls.__no_convert.add(obj_type)

    @classmethod  # noqa: C901
    def convert_dtype(cls, spec, value, spec_dtype=None):  # noqa: C901
        """
        Convert values to the specified dtype. For example, if a literal int
        is passed in to a field that is specified as a unsigned integer, this function
        will convert the Python int to a numpy unsigned int.

        :param spec: The DatasetSpec or AttributeSpec to which this value is being applied
        :param value: The value being converted to the spec dtype
        :param spec_dtype: Optional override of the dtype in spec.dtype. Used to specify the parent dtype when the given
                           extended spec lacks a dtype.

        :return: The function returns a tuple consisting of 1) the value, and 2) the data type.
                 The value is returned as the function may convert the input value to comply
                 with the dtype specified in the schema.
        """
        if spec_dtype is None:
            spec_dtype = spec.dtype
        ret, ret_dtype = cls.__check_edgecases(spec, value, spec_dtype)
        if ret is not None or ret_dtype is not None:
            return ret, ret_dtype
        # spec_dtype is a string, spec_dtype_type is a type or the conversion helper functions _unicode or _ascii
        spec_dtype_type = cls.__dtypes[spec_dtype]
        warning_msg = None
        if isinstance(value, np.ndarray):
            if spec_dtype_type is _unicode:
                ret = value.astype('U')
                ret_dtype = "utf8"
            elif spec_dtype_type is _ascii:
                ret = value.astype('S')
                ret_dtype = "ascii"
            else:
                dtype_func, warning_msg = cls.__resolve_numeric_dtype(value.dtype, spec_dtype_type)
                if value.dtype == dtype_func:
                    ret = value
                else:
                    ret = value.astype(dtype_func)
                ret_dtype = ret.dtype.type
        elif isinstance(value, (tuple, list)):
            if len(value) == 0:
                if spec_dtype_type is _unicode:
                    ret_dtype = 'utf8'
                elif spec_dtype_type is _ascii:
                    ret_dtype = 'ascii'
                else:
                    ret_dtype = spec_dtype_type
                return value, ret_dtype
            ret = list()
            for elem in value:
                tmp, tmp_dtype = cls.convert_dtype(spec, elem, spec_dtype)
                ret.append(tmp)
            ret = type(value)(ret)
            ret_dtype = tmp_dtype
        elif isinstance(value, AbstractDataChunkIterator):
            ret = value
            if spec_dtype_type is _unicode:
                ret_dtype = "utf8"
            elif spec_dtype_type is _ascii:
                ret_dtype = "ascii"
            else:
                ret_dtype, warning_msg = cls.__resolve_numeric_dtype(value.dtype, spec_dtype_type)
        else:
            if spec_dtype_type in (_unicode, _ascii):
                ret_dtype = 'ascii'
                if spec_dtype_type is _unicode:
                    ret_dtype = 'utf8'
                ret = spec_dtype_type(value)
            else:
                dtype_func, warning_msg = cls.__resolve_numeric_dtype(type(value), spec_dtype_type)
                ret = dtype_func(value)
                ret_dtype = type(ret)
        if warning_msg:
            full_warning_msg = "Spec '%s': %s" % (spec.path, warning_msg)
            warnings.warn(full_warning_msg, DtypeConversionWarning)
        return ret, ret_dtype

    @classmethod
    def __check_convert_numeric(cls, value_type):
        # dtype 'numeric' allows only ints, floats, and uints
        value_dtype = np.dtype(value_type)
        if not (np.issubdtype(value_dtype, np.unsignedinteger) or
                np.issubdtype(value_dtype, np.floating) or
                np.issubdtype(value_dtype, np.integer)):
            raise ValueError("Cannot convert from %s to 'numeric' specification dtype." % value_type)

    @classmethod  # noqa: C901
    def __check_edgecases(cls, spec, value, spec_dtype):  # noqa: C901
        """
        Check edge cases in converting data to a dtype
        """
        if value is None:
            dt = spec_dtype
            if isinstance(dt, RefSpec):
                dt = dt.reftype
            return None, dt
        if isinstance(spec_dtype, list):
            # compound dtype - Since the I/O layer needs to determine how to handle these,
            # return the list of DtypeSpecs
            return value, spec_dtype
        if isinstance(value, DataIO):
            return value, cls.convert_dtype(spec, value.data, spec_dtype)[1]
        if spec_dtype is None or spec_dtype == 'numeric' or type(value) in cls.__no_convert:
            # infer type from value
            if hasattr(value, 'dtype'):  # covers numpy types, AbstractDataChunkIterator
                if spec_dtype == 'numeric':
                    cls.__check_convert_numeric(value.dtype.type)
                if np.issubdtype(value.dtype, np.str_):
                    ret_dtype = 'utf8'
                elif np.issubdtype(value.dtype, np.string_):
                    ret_dtype = 'ascii'
                else:
                    ret_dtype = value.dtype.type
                return value, ret_dtype
            if isinstance(value, (list, tuple)):
                if len(value) == 0:
                    msg = "Cannot infer dtype of empty list or tuple. Please use numpy array with specified dtype."
                    raise ValueError(msg)
                return value, cls.__check_edgecases(spec, value[0], spec_dtype)[1]  # infer dtype from first element
            ret_dtype = type(value)
            if spec_dtype == 'numeric':
                cls.__check_convert_numeric(ret_dtype)
            if ret_dtype is str:
                ret_dtype = 'utf8'
            elif ret_dtype is bytes:
                ret_dtype = 'ascii'
            return value, ret_dtype
        if isinstance(spec_dtype, RefSpec):
            if not isinstance(value, ReferenceBuilder):
                msg = "got RefSpec for value of type %s" % type(value)
                raise ValueError(msg)
            return value, spec_dtype
        if spec_dtype is not None and spec_dtype not in cls.__dtypes:  # pragma: no cover
            msg = "unrecognized dtype: %s -- cannot convert value" % spec_dtype
            raise ValueError(msg)
        return None, None

    _const_arg = '__constructor_arg'

    @staticmethod
    @docval({'name': 'name', 'type': str, 'doc': 'the name of the constructor argument'},
            is_method=False)
    def constructor_arg(**kwargs):
        '''Decorator to override the default mapping scheme for a given constructor argument.

        Decorate ObjectMapper methods with this function when extending ObjectMapper to override the default
        scheme for mapping between AbstractContainer and Builder objects. The decorated method should accept as its
        first argument the Builder object that is being mapped. The method should return the value to be passed
        to the target AbstractContainer class constructor argument given by *name*.
        '''
        name = getargs('name', kwargs)
        return _constructor_arg(name)

    _obj_attr = '__object_attr'

    @staticmethod
    @docval({'name': 'name', 'type': str, 'doc': 'the name of the constructor argument'},
            is_method=False)
    def object_attr(**kwargs):
        '''Decorator to override the default mapping scheme for a given object attribute.

        Decorate ObjectMapper methods with this function when extending ObjectMapper to override the default
        scheme for mapping between AbstractContainer and Builder objects. The decorated method should accept as its
        first argument the AbstractContainer object that is being mapped. The method should return the child Builder
        object (or scalar if the object attribute corresponds to an AttributeSpec) that represents the
        attribute given by *name*.
        '''
        name = getargs('name', kwargs)
        return _object_attr(name)

    @staticmethod
    def __is_attr(attr_val):
        return hasattr(attr_val, _obj_attr)

    @staticmethod
    def __get_obj_attr(attr_val):
        return getattr(attr_val, _obj_attr)

    @staticmethod
    def __is_constructor_arg(attr_val):
        return hasattr(attr_val, _const_arg)

    @staticmethod
    def __get_cargname(attr_val):
        return getattr(attr_val, _const_arg)

    @ExtenderMeta.post_init
    def __gather_procedures(cls, name, bases, classdict):
        if hasattr(cls, 'constructor_args'):
            cls.constructor_args = copy(cls.constructor_args)
        else:
            cls.constructor_args = dict()
        if hasattr(cls, 'obj_attrs'):
            cls.obj_attrs = copy(cls.obj_attrs)
        else:
            cls.obj_attrs = dict()
        for name, func in cls.__dict__.items():
            if cls.__is_constructor_arg(func):
                cls.constructor_args[cls.__get_cargname(func)] = getattr(cls, name)
            elif cls.__is_attr(func):
                cls.obj_attrs[cls.__get_obj_attr(func)] = getattr(cls, name)

    @docval({'name': 'spec', 'type': (DatasetSpec, GroupSpec),
             'doc': 'The specification for mapping objects to builders'})
    def __init__(self, **kwargs):
        """ Create a map from AbstractContainer attributes to specifications """
        self.logger = logging.getLogger('%s.%s' % (self.__class__.__module__, self.__class__.__qualname__))
        spec = getargs('spec', kwargs)
        self.__spec = spec
        self.__data_type_key = spec.type_key()
        self.__spec2attr = dict()
        self.__attr2spec = dict()
        self.__spec2carg = dict()
        self.__carg2spec = dict()
        self.__map_spec(spec)

    @property
    def spec(self):
        ''' the Spec used in this ObjectMapper '''
        return self.__spec

    @_constructor_arg('name')
    def get_container_name(self, *args):
        builder = args[0]
        return builder.name

    @classmethod
    @docval({'name': 'spec', 'type': Spec, 'doc': 'the specification to get the name for'})
    def convert_dt_name(cls, **kwargs):
        '''Construct the attribute name corresponding to a specification'''
        spec = getargs('spec', kwargs)
        name = cls.__get_data_type(spec)
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
        name = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()
        if name[-1] != 's' and spec.is_many():
            name += 's'
        return name

    @classmethod
    def __get_fields(cls, name_stack, all_names, spec):
        name = spec.name
        if spec.name is None:
            name = cls.convert_dt_name(spec)
        name_stack.append(name)
        name = '__'.join(name_stack)
        # TODO address potential name clashes, e.g., quantity '*' subgroups and links of same data_type_inc will
        # have the same name
        all_names[name] = spec
        if isinstance(spec, BaseStorageSpec):
            if not (spec.data_type_def is None and spec.data_type_inc is None):
                # don't get names for components in data_types
                name_stack.pop()
                return
            for subspec in spec.attributes:
                cls.__get_fields(name_stack, all_names, subspec)
            if isinstance(spec, GroupSpec):
                for subspec in spec.datasets:
                    cls.__get_fields(name_stack, all_names, subspec)
                for subspec in spec.groups:
                    cls.__get_fields(name_stack, all_names, subspec)
                for subspec in spec.links:
                    cls.__get_fields(name_stack, all_names, subspec)
        name_stack.pop()

    @classmethod
    @docval({'name': 'spec', 'type': Spec, 'doc': 'the specification to get the object attribute names for'})
    def get_attr_names(cls, **kwargs):
        '''Get the attribute names for each subspecification in a Spec'''
        spec = getargs('spec', kwargs)
        names = OrderedDict()
        for subspec in spec.attributes:
            cls.__get_fields(list(), names, subspec)
        if isinstance(spec, GroupSpec):
            for subspec in spec.groups:
                cls.__get_fields(list(), names, subspec)
            for subspec in spec.datasets:
                cls.__get_fields(list(), names, subspec)
            for subspec in spec.links:
                cls.__get_fields(list(), names, subspec)
        return names

    def __map_spec(self, spec):
        attr_names = self.get_attr_names(spec)
        for k, v in attr_names.items():
            self.map_spec(k, v)

    @docval({"name": "attr_name", "type": str, "doc": "the name of the object to map"},
            {"name": "spec", "type": Spec, "doc": "the spec to map the attribute to"})
    def map_attr(self, **kwargs):
        """ Map an attribute to spec. Use this to override default behavior """
        attr_name, spec = getargs('attr_name', 'spec', kwargs)
        self.__spec2attr[spec] = attr_name
        self.__attr2spec[attr_name] = spec

    @docval({"name": "attr_name", "type": str, "doc": "the name of the attribute"})
    def get_attr_spec(self, **kwargs):
        """ Return the Spec for a given attribute """
        attr_name = getargs('attr_name', kwargs)
        return self.__attr2spec.get(attr_name)

    @docval({"name": "carg_name", "type": str, "doc": "the name of the constructor argument"})
    def get_carg_spec(self, **kwargs):
        """ Return the Spec for a given constructor argument """
        carg_name = getargs('carg_name', kwargs)
        return self.__carg2spec.get(carg_name)

    @docval({"name": "const_arg", "type": str, "doc": "the name of the constructor argument to map"},
            {"name": "spec", "type": Spec, "doc": "the spec to map the attribute to"})
    def map_const_arg(self, **kwargs):
        """ Map an attribute to spec. Use this to override default behavior """
        const_arg, spec = getargs('const_arg', 'spec', kwargs)
        self.__spec2carg[spec] = const_arg
        self.__carg2spec[const_arg] = spec

    @docval({"name": "spec", "type": Spec, "doc": "the spec to map the attribute to"})
    def unmap(self, **kwargs):
        """ Removing any mapping for a specification. Use this to override default mapping """
        spec = getargs('spec', kwargs)
        self.__spec2attr.pop(spec, None)
        self.__spec2carg.pop(spec, None)

    @docval({"name": "attr_carg", "type": str, "doc": "the constructor argument/object attribute to map this spec to"},
            {"name": "spec", "type": Spec, "doc": "the spec to map the attribute to"})
    def map_spec(self, **kwargs):
        """ Map the given specification to the construct argument and object attribute """
        spec, attr_carg = getargs('spec', 'attr_carg', kwargs)
        self.map_const_arg(attr_carg, spec)
        self.map_attr(attr_carg, spec)

    def __get_override_carg(self, *args):
        name = args[0]
        remaining_args = tuple(args[1:])
        if name in self.constructor_args:
            self.logger.debug("        Calling override function for constructor argument '%s'" % name)
            func = self.constructor_args[name]
            return func(self, *remaining_args)
        return None

    def __get_override_attr(self, name, container, manager):
        if name in self.obj_attrs:
            self.logger.debug("        Calling override function for attribute '%s'" % name)
            func = self.obj_attrs[name]
            return func(self, container, manager)
        return None

    @docval({"name": "spec", "type": Spec, "doc": "the spec to get the attribute for"},
            returns='the attribute name', rtype=str)
    def get_attribute(self, **kwargs):
        ''' Get the object attribute name for the given Spec '''
        spec = getargs('spec', kwargs)
        val = self.__spec2attr.get(spec, None)
        return val

    @docval({"name": "spec", "type": Spec, "doc": "the spec to get the attribute value for"},
            {"name": "container", "type": AbstractContainer, "doc": "the container to get the attribute value from"},
            {"name": "manager", "type": BuildManager, "doc": "the BuildManager used for managing this build"},
            returns='the value of the attribute')
    def get_attr_value(self, **kwargs):
        ''' Get the value of the attribute corresponding to this spec from the given container '''
        spec, container, manager = getargs('spec', 'container', 'manager', kwargs)
        attr_name = self.get_attribute(spec)
        if attr_name is None:
            return None
        attr_val = self.__get_override_attr(attr_name, container, manager)
        if attr_val is None:
            try:
                attr_val = getattr(container, attr_name)
            except AttributeError:
                msg = ("%s '%s' does not have attribute '%s' for mapping to spec: %s"
                       % (container.__class__.__name__, container.name, attr_name, spec))
                raise ContainerConfigurationError(msg)
            if attr_val is not None:
                attr_val = self.__convert_string(attr_val, spec)
                spec_dt = self.__get_data_type(spec)
                if spec_dt is not None:
                    try:
                        attr_val = self.__filter_by_spec_dt(attr_val, spec_dt, manager)
                    except ValueError as e:
                        msg = ("%s '%s' attribute '%s' has unexpected type."
                               % (container.__class__.__name__, container.name, attr_name))
                        raise ContainerConfigurationError(msg) from e
            # else: attr_val is an attribute on the Container and its value is None
        # attr_val can be None, an AbstractContainer, or a list of AbstractContainers
        return attr_val

    @classmethod
    def __get_data_type(cls, spec):
        ret = None
        if isinstance(spec, LinkSpec):
            ret = spec.target_type
        elif isinstance(spec, BaseStorageSpec):
            if spec.data_type_def is not None:
                ret = spec.data_type_def
            elif spec.data_type_inc is not None:
                ret = spec.data_type_inc
            # else, untyped group/dataset spec
        # else, attribute spec
        return ret

    def __convert_string(self, value, spec):
        """Convert string types to the specified dtype."""
        ret = value
        if isinstance(spec, AttributeSpec):
            if 'text' in spec.dtype:
                if spec.shape is not None or spec.dims is not None:
                    ret = list(map(str, value))
                else:
                    ret = str(value)
        elif isinstance(spec, DatasetSpec):
            # TODO: make sure we can handle specs with data_type_inc set
            if spec.data_type_inc is None and spec.dtype is not None:
                string_type = None
                if 'text' in spec.dtype:
                    string_type = str
                elif 'ascii' in spec.dtype:
                    string_type = bytes
                elif 'isodatetime' in spec.dtype:
                    string_type = datetime.isoformat
                if string_type is not None:
                    if spec.shape is not None or spec.dims is not None:
                        ret = list(map(string_type, value))
                    else:
                        ret = string_type(value)
                    # copy over any I/O parameters if they were specified
                    if isinstance(value, DataIO):
                        params = value.get_io_params()
                        params['data'] = ret
                        ret = value.__class__(**params)
        return ret

    def __filter_by_spec_dt(self, attr_value, spec_dt, build_manager):
        """Return a list of containers that match the spec data type.

        If attr_value is a container that does not match the spec data type, then None is returned.
        If attr_value is a collection, then a list of only the containers in the collection that match the
        spec data type are returned.
        Otherwise, attr_value is returned unchanged.

        spec_dt is a string representing a spec data type.

        Return None, an AbstractContainer, or a list of AbstractContainers
        """
        if isinstance(attr_value, AbstractContainer):
            if build_manager.is_sub_data_type(attr_value, spec_dt):
                return attr_value
            else:
                return None

        ret = attr_value
        if isinstance(attr_value, (list, tuple, set, dict)):
            if isinstance(attr_value, dict):
                attr_values = attr_value.values()
            else:
                attr_values = attr_value
            ret = []
            # NOTE: this will test collections of non-containers element-wise (e.g. lists of lists of ints)
            for c in attr_values:
                if self.__filter_by_spec_dt(c, spec_dt, build_manager) is not None:
                    ret.append(c)
            if len(ret) == 0:
                ret = None
        else:
            raise ValueError("Unexpected type for attr_value: %s. Only AbstractContainer, list, tuple, set, dict, are "
                             "allowed." % type(attr_value))
        return ret

    def __check_quantity(self, attr_value, spec, container):
        if attr_value is None and spec.required:
            attr_name = self.get_attribute(spec)
            msg = ("%s '%s' is missing required value for attribute '%s'."
                   % (container.__class__.__name__, container.name, attr_name))
            warnings.warn(msg, MissingRequiredBuildWarning)
            self.logger.debug('MissingRequiredBuildWarning: ' + msg)
        elif attr_value is not None and self.__get_data_type(spec) is not None:
            # quantity is valid only for specs with a data type or target type
            if isinstance(attr_value, AbstractContainer):
                attr_value = [attr_value]
            n = len(attr_value)
            if (n and isinstance(attr_value[0], AbstractContainer) and
                    ((n > 1 and not spec.is_many()) or (isinstance(spec.quantity, int) and n != spec.quantity))):
                attr_name = self.get_attribute(spec)
                msg = ("%s '%s' has %d values for attribute '%s' but spec allows %s."
                       % (container.__class__.__name__, container.name, n, attr_name, repr(spec.quantity)))
                warnings.warn(msg, IncorrectQuantityBuildWarning)
                self.logger.debug('IncorrectQuantityBuildWarning: ' + msg)

    @docval({"name": "spec", "type": Spec, "doc": "the spec to get the constructor argument for"},
            returns="the name of the constructor argument", rtype=str)
    def get_const_arg(self, **kwargs):
        ''' Get the constructor argument for the given Spec '''
        spec = getargs('spec', kwargs)
        return self.__spec2carg.get(spec, None)

    @docval({"name": "container", "type": AbstractContainer, "doc": "the container to convert to a Builder"},
            {"name": "manager", "type": BuildManager, "doc": "the BuildManager to use for managing this build"},
            {"name": "parent", "type": GroupBuilder, "doc": "the parent of the resulting Builder", 'default': None},
            {"name": "source", "type": str,
             "doc": "the source of container being built i.e. file path", 'default': None},
            {"name": "builder", "type": BaseBuilder, "doc": "the Builder to build on", 'default': None},
            {"name": "spec_ext", "type": BaseStorageSpec, "doc": "a spec extension", 'default': None},
            {"name": "export", "type": bool, "doc": "whether this build is for exporting",
             'default': False},
            returns="the Builder representing the given AbstractContainer", rtype=Builder)
    def build(self, **kwargs):
        '''Convert an AbstractContainer to a Builder representation.

        References are not added but are queued to be added in the BuildManager.
        '''
        container, manager, parent, source = getargs('container', 'manager', 'parent', 'source', kwargs)
        builder, spec_ext, export = getargs('builder', 'spec_ext', 'export', kwargs)
        name = manager.get_builder_name(container)
        if isinstance(self.__spec, GroupSpec):
            self.logger.debug("Building %s '%s' as a group (source: %s)"
                              % (container.__class__.__name__, container.name, repr(source)))
            if builder is None:
                builder = GroupBuilder(name, parent=parent, source=source)
            self.__add_datasets(builder, self.__spec.datasets, container, manager, source, export)
            self.__add_groups(builder, self.__spec.groups, container, manager, source, export)
            self.__add_links(builder, self.__spec.links, container, manager, source, export)
        else:
            if builder is None:
                if not isinstance(container, Data):
                    msg = "'container' must be of type Data with DatasetSpec"
                    raise ValueError(msg)
                spec_dtype, spec_shape, spec = self.__check_dset_spec(self.spec, spec_ext)
                if isinstance(spec_dtype, RefSpec):
                    self.logger.debug("Building %s '%s' as a dataset of references (source: %s)"
                                      % (container.__class__.__name__, container.name, repr(source)))
                    # create dataset builder with data=None as a placeholder. fill in with refs later
                    builder = DatasetBuilder(name, data=None, parent=parent, source=source, dtype=spec_dtype.reftype)
                    manager.queue_ref(self.__set_dataset_to_refs(builder, spec_dtype, spec_shape, container, manager))
                elif isinstance(spec_dtype, list):
                    # a compound dataset
                    self.logger.debug("Building %s '%s' as a dataset of compound dtypes (source: %s)"
                                      % (container.__class__.__name__, container.name, repr(source)))
                    # create dataset builder with data=None, dtype=None as a placeholder. fill in with refs later
                    builder = DatasetBuilder(name, data=None, parent=parent, source=source, dtype=spec_dtype)
                    manager.queue_ref(self.__set_compound_dataset_to_refs(builder, spec, spec_dtype, container,
                                                                          manager))
                else:
                    # a regular dtype
                    if spec_dtype is None and self.__is_reftype(container.data):
                        self.logger.debug("Building %s '%s' containing references as a dataset of unspecified dtype "
                                          "(source: %s)"
                                          % (container.__class__.__name__, container.name, repr(source)))
                        # an unspecified dtype and we were given references
                        # create dataset builder with data=None as a placeholder. fill in with refs later
                        builder = DatasetBuilder(name, data=None, parent=parent, source=source, dtype='object')
                        manager.queue_ref(self.__set_untyped_dataset_to_refs(builder, container, manager))
                    else:
                        # a dataset that has no references, pass the conversion off to the convert_dtype method
                        self.logger.debug("Building %s '%s' as a dataset (source: %s)"
                                          % (container.__class__.__name__, container.name, repr(source)))
                        try:
                            # use spec_dtype from self.spec when spec_ext does not specify dtype
                            bldr_data, dtype = self.convert_dtype(spec, container.data, spec_dtype=spec_dtype)
                        except Exception as ex:
                            msg = 'could not resolve dtype for %s \'%s\'' % (type(container).__name__, container.name)
                            raise Exception(msg) from ex
                        builder = DatasetBuilder(name, bldr_data, parent=parent, source=source, dtype=dtype)

        # Add attributes from the specification extension to the list of attributes
        all_attrs = self.__spec.attributes + getattr(spec_ext, 'attributes', tuple())
        # If the spec_ext refines an existing attribute it will now appear twice in the list. The
        # refinement should only be relevant for validation (not for write). To avoid problems with the
        # write we here remove duplicates and keep the original spec of the two to make write work.
        # TODO: We should add validation in the AttributeSpec to make sure refinements are valid
        # TODO: Check the BuildManager as refinements should probably be resolved rather than be passed in via spec_ext
        all_attrs = list({a.name: a for a in all_attrs[::-1]}.values())
        self.__add_attributes(builder, all_attrs, container, manager, source, export)
        return builder

    def __check_dset_spec(self, orig, ext):
        """
        Check a dataset spec against a refining spec to see which dtype and shape should be used
        """
        dtype = orig.dtype
        shape = orig.shape
        spec = orig
        if ext is not None:
            if ext.dtype is not None:
                dtype = ext.dtype
            if ext.shape is not None:
                shape = ext.shape
            spec = ext
        return dtype, shape, spec

    def __is_reftype(self, data):
        if (isinstance(data, AbstractDataChunkIterator) or
                (isinstance(data, DataIO) and isinstance(data.data, AbstractDataChunkIterator))):
            return False

        tmp = data
        while hasattr(tmp, '__len__') and not isinstance(tmp, (AbstractContainer, str, bytes)):
            tmptmp = None
            for t in tmp:
                # In case of a numeric array stop the iteration at the first element to avoid long-running loop
                if isinstance(t, (int, float, complex, bool)):
                    break
                if hasattr(t, '__len__') and len(t) > 0 and not isinstance(t, (AbstractContainer, str, bytes)):
                    tmptmp = tmp[0]
                    break
            if tmptmp is not None:
                break
            else:
                if len(tmp) == 0:
                    tmp = None
                else:
                    tmp = tmp[0]
        if isinstance(tmp, AbstractContainer):
            return True
        else:
            return False

    def __set_dataset_to_refs(self, builder, dtype, shape, container, build_manager):
        self.logger.debug("Queueing set dataset of references %s '%s' to reference builder(s)"
                          % (builder.__class__.__name__, builder.name))

        def _filler():
            builder.data = self.__get_ref_builder(builder, dtype, shape, container, build_manager)

        return _filler

    def __set_compound_dataset_to_refs(self, builder, spec, spec_dtype, container, build_manager):
        self.logger.debug("Queueing convert compound dataset %s '%s' and set any references to reference builders"
                          % (builder.__class__.__name__, builder.name))

        def _filler():
            self.logger.debug("Converting compound dataset %s '%s' and setting any references to reference builders"
                              % (builder.__class__.__name__, builder.name))
            # convert the reference part(s) of a compound dataset to ReferenceBuilders, row by row
            refs = [(i, subt) for i, subt in enumerate(spec_dtype) if isinstance(subt.dtype, RefSpec)]
            bldr_data = list()
            for i, row in enumerate(container.data):
                tmp = list(row)
                for j, subt in refs:
                    tmp[j] = self.__get_ref_builder(builder, subt.dtype, None, row[j], build_manager)
                bldr_data.append(tuple(tmp))
            builder.data = bldr_data

        return _filler

    def __set_untyped_dataset_to_refs(self, builder, container, build_manager):
        self.logger.debug("Queueing set untyped dataset %s '%s' to reference builders"
                          % (builder.__class__.__name__, builder.name))

        def _filler():
            self.logger.debug("Setting untyped dataset %s '%s' to list of reference builders"
                              % (builder.__class__.__name__, builder.name))
            bldr_data = list()
            for d in container.data:
                if d is None:
                    bldr_data.append(None)
                else:
                    target_builder = self.__get_target_builder(d, build_manager, builder)
                    bldr_data.append(ReferenceBuilder(target_builder))
            builder.data = bldr_data

        return _filler

    def __get_ref_builder(self, builder, dtype, shape, container, build_manager):
        bldr_data = None
        if dtype.is_region():
            if shape is None:
                if not isinstance(container, DataRegion):
                    msg = "'container' must be of type DataRegion if spec represents region reference"
                    raise ValueError(msg)
                self.logger.debug("Setting %s '%s' data to region reference builder"
                                  % (builder.__class__.__name__, builder.name))
                target_builder = self.__get_target_builder(container.data, build_manager, builder)
                bldr_data = RegionBuilder(container.region, target_builder)
            else:
                self.logger.debug("Setting %s '%s' data to list of region reference builders"
                                  % (builder.__class__.__name__, builder.name))
                bldr_data = list()
                for d in container.data:
                    target_builder = self.__get_target_builder(d.target, build_manager, builder)
                    bldr_data.append(RegionBuilder(d.slice, target_builder))
        else:
            self.logger.debug("Setting object reference dataset on %s '%s' data"
                              % (builder.__class__.__name__, builder.name))
            if isinstance(container, Data):
                self.logger.debug("Setting %s '%s' data to list of reference builders"
                                  % (builder.__class__.__name__, builder.name))
                bldr_data = list()
                for d in container.data:
                    target_builder = self.__get_target_builder(d, build_manager, builder)
                    bldr_data.append(ReferenceBuilder(target_builder))
            else:
                self.logger.debug("Setting %s '%s' data to reference builder"
                                  % (builder.__class__.__name__, builder.name))
                target_builder = self.__get_target_builder(container, build_manager, builder)
                bldr_data = ReferenceBuilder(target_builder)
        return bldr_data

    def __get_target_builder(self, container, build_manager, builder):
        target_builder = build_manager.get_builder(container)
        if target_builder is None:
            raise ReferenceTargetNotBuiltError(builder, container)
        return target_builder

    def __add_attributes(self, builder, attributes, container, build_manager, source, export):
        if attributes:
            self.logger.debug("Adding attributes from %s '%s' to %s '%s'"
                              % (container.__class__.__name__, container.name,
                                 builder.__class__.__name__, builder.name))
        for spec in attributes:
            self.logger.debug("    Adding attribute for spec name: %s (dtype: %s)"
                              % (repr(spec.name), spec.dtype.__class__.__name__))
            if spec.value is not None:
                attr_value = spec.value
            else:
                attr_value = self.get_attr_value(spec, container, build_manager)
                if attr_value is None:
                    attr_value = spec.default_value

            attr_value = self.__check_ref_resolver(attr_value)

            self.__check_quantity(attr_value, spec, container)
            if attr_value is None:
                self.logger.debug("        Skipping empty attribute")
                continue

            if isinstance(spec.dtype, RefSpec):
                if not self.__is_reftype(attr_value):
                    msg = ("invalid type for reference '%s' (%s) - must be AbstractContainer"
                           % (spec.name, type(attr_value)))
                    raise ValueError(msg)

                build_manager.queue_ref(self.__set_attr_to_ref(builder, attr_value, build_manager, spec))
                continue
            else:
                try:
                    attr_value, attr_dtype = self.convert_dtype(spec, attr_value)
                except Exception as ex:
                    msg = 'could not convert %s for %s %s' % (spec.name, type(container).__name__, container.name)
                    raise BuildError(builder, msg) from ex

                # do not write empty or null valued objects
                self.__check_quantity(attr_value, spec, container)
                if attr_value is None:
                    self.logger.debug("        Skipping empty attribute")
                    continue

            builder.set_attribute(spec.name, attr_value)

    def __set_attr_to_ref(self, builder, attr_value, build_manager, spec):
        self.logger.debug("Queueing set reference attribute on %s '%s' attribute '%s' to %s"
                          % (builder.__class__.__name__, builder.name, spec.name,
                             attr_value.__class__.__name__))

        def _filler():
            self.logger.debug("Setting reference attribute on %s '%s' attribute '%s' to %s"
                              % (builder.__class__.__name__, builder.name, spec.name,
                                 attr_value.__class__.__name__))
            target_builder = self.__get_target_builder(attr_value, build_manager, builder)
            ref_attr_value = ReferenceBuilder(target_builder)
            builder.set_attribute(spec.name, ref_attr_value)

        return _filler

    def __add_links(self, builder, links, container, build_manager, source, export):
        if links:
            self.logger.debug("Adding links from %s '%s' to %s '%s'"
                              % (container.__class__.__name__, container.name,
                                 builder.__class__.__name__, builder.name))
        for spec in links:
            self.logger.debug("    Adding link for spec name: %s, target_type: %s"
                              % (repr(spec.name), repr(spec.target_type)))
            attr_value = self.get_attr_value(spec, container, build_manager)
            self.__check_quantity(attr_value, spec, container)
            if attr_value is None:
                self.logger.debug("        Skipping link - no attribute value")
                continue
            self.__add_containers(builder, spec, attr_value, build_manager, source, container, export)

    def __add_datasets(self, builder, datasets, container, build_manager, source, export):
        if datasets:
            self.logger.debug("Adding datasets from %s '%s' to %s '%s'"
                              % (container.__class__.__name__, container.name,
                                 builder.__class__.__name__, builder.name))
        for spec in datasets:
            self.logger.debug("    Adding dataset for spec name: %s (dtype: %s)"
                              % (repr(spec.name), spec.dtype.__class__.__name__))
            attr_value = self.get_attr_value(spec, container, build_manager)
            self.__check_quantity(attr_value, spec, container)
            if attr_value is None:
                self.logger.debug("        Skipping dataset - no attribute value")
                continue
            attr_value = self.__check_ref_resolver(attr_value)
            if isinstance(attr_value, DataIO) and attr_value.data is None:
                self.logger.debug("        Skipping dataset - attribute is dataio or has no data")
                continue
            if isinstance(attr_value, LinkBuilder):
                self.logger.debug("        Adding %s '%s' for spec name: %s, %s: %s, %s: %s"
                                  % (attr_value.name, attr_value.__class__.__name__,
                                     repr(spec.name),
                                     spec.def_key(), repr(spec.data_type_def),
                                     spec.inc_key(), repr(spec.data_type_inc)))
                builder.set_link(attr_value)  # add the existing builder
            elif spec.data_type_def is None and spec.data_type_inc is None:  # untyped, named dataset
                if spec.name in builder.datasets:
                    sub_builder = builder.datasets[spec.name]
                    self.logger.debug("        Retrieving existing DatasetBuilder '%s' for spec name %s and adding "
                                      "attributes" % (sub_builder.name, repr(spec.name)))
                else:
                    self.logger.debug("        Converting untyped dataset for spec name %s to spec dtype %s"
                                      % (repr(spec.name), repr(spec.dtype)))
                    try:
                        data, dtype = self.convert_dtype(spec, attr_value)
                    except Exception as ex:
                        msg = 'could not convert \'%s\' for %s \'%s\''
                        msg = msg % (spec.name, type(container).__name__, container.name)
                        raise BuildError(builder, msg) from ex
                    self.logger.debug("        Adding untyped dataset for spec name %s and adding attributes"
                                      % repr(spec.name))
                    sub_builder = DatasetBuilder(spec.name, data, parent=builder, source=source, dtype=dtype)
                    builder.set_dataset(sub_builder)
                self.__add_attributes(sub_builder, spec.attributes, container, build_manager, source, export)
            else:
                self.logger.debug("        Adding typed dataset for spec name: %s, %s: %s, %s: %s"
                                  % (repr(spec.name),
                                     spec.def_key(), repr(spec.data_type_def),
                                     spec.inc_key(), repr(spec.data_type_inc)))
                self.__add_containers(builder, spec, attr_value, build_manager, source, container, export)

    def __add_groups(self, builder, groups, container, build_manager, source, export):
        if groups:
            self.logger.debug("Adding groups from %s '%s' to %s '%s'"
                              % (container.__class__.__name__, container.name,
                                 builder.__class__.__name__, builder.name))
        for spec in groups:
            if spec.data_type_def is None and spec.data_type_inc is None:
                self.logger.debug("    Adding untyped group for spec name: %s" % repr(spec.name))
                # we don't need to get attr_name since any named group does not have the concept of value
                sub_builder = builder.groups.get(spec.name)
                if sub_builder is None:
                    sub_builder = GroupBuilder(spec.name, source=source)
                self.__add_attributes(sub_builder, spec.attributes, container, build_manager, source, export)
                self.__add_datasets(sub_builder, spec.datasets, container, build_manager, source, export)
                self.__add_links(sub_builder, spec.links, container, build_manager, source, export)
                self.__add_groups(sub_builder, spec.groups, container, build_manager, source, export)
                empty = sub_builder.is_empty()
                if not empty or (empty and spec.required):
                    if sub_builder.name not in builder.groups:
                        builder.set_group(sub_builder)
            else:
                self.logger.debug("    Adding group for spec name: %s, %s: %s, %s: %s"
                                  % (repr(spec.name),
                                     spec.def_key(), repr(spec.data_type_def),
                                     spec.inc_key(), repr(spec.data_type_inc)))
                attr_value = self.get_attr_value(spec, container, build_manager)
                self.__check_quantity(attr_value, spec, container)
                if attr_value is not None:
                    self.__add_containers(builder, spec, attr_value, build_manager, source, container, export)

    def __add_containers(self, builder, spec, value, build_manager, source, parent_container, export):
        if isinstance(value, AbstractContainer):
            self.logger.debug("    Adding container %s '%s' with parent %s '%s' to %s '%s'"
                              % (value.__class__.__name__, value.name,
                                 parent_container.__class__.__name__, parent_container.name,
                                 builder.__class__.__name__, builder.name))
            if value.parent is None:
                if (value.container_source == parent_container.container_source or
                        build_manager.get_builder(value) is None):
                    # value was removed (or parent not set) and there is a link to it in same file
                    # or value was read from an external link
                    raise OrphanContainerBuildError(builder, value)

            if value.modified or export:
                # writing a newly instantiated container (modified is False only after read) or as if it is newly
                # instantianted (export=True)
                self.logger.debug("    Building newly instantiated %s '%s'" % (value.__class__.__name__, value.name))
                if isinstance(spec, BaseStorageSpec):
                    new_builder = build_manager.build(value, source=source, spec_ext=spec, export=export)
                else:
                    new_builder = build_manager.build(value, source=source, export=export)
                # use spec to determine what kind of HDF5 object this AbstractContainer corresponds to
                if isinstance(spec, LinkSpec) or value.parent is not parent_container:
                    self.logger.debug("    Adding link to %s '%s' in %s '%s'"
                                      % (new_builder.__class__.__name__, new_builder.name,
                                         builder.__class__.__name__, builder.name))
                    builder.set_link(LinkBuilder(new_builder, name=spec.name, parent=builder))
                elif isinstance(spec, DatasetSpec):
                    self.logger.debug("    Adding dataset %s '%s' to %s '%s'"
                                      % (new_builder.__class__.__name__, new_builder.name,
                                         builder.__class__.__name__, builder.name))
                    builder.set_dataset(new_builder)
                else:
                    self.logger.debug("    Adding subgroup %s '%s' to %s '%s'"
                                      % (new_builder.__class__.__name__, new_builder.name,
                                         builder.__class__.__name__, builder.name))
                    builder.set_group(new_builder)
            elif value.container_source:  # make a link to an existing container
                if (value.container_source != parent_container.container_source
                        or value.parent is not parent_container):
                    self.logger.debug("    Building %s '%s' (container source: %s) and adding a link to it"
                                      % (value.__class__.__name__, value.name, value.container_source))
                    if isinstance(spec, BaseStorageSpec):
                        new_builder = build_manager.build(value, source=source, spec_ext=spec, export=export)
                    else:
                        new_builder = build_manager.build(value, source=source, export=export)
                    builder.set_link(LinkBuilder(new_builder, name=spec.name, parent=builder))
                else:
                    self.logger.debug("    Skipping build for %s '%s' because both it and its parents were read "
                                      "from the same source."
                                      % (value.__class__.__name__, value.name))
            else:
                raise ValueError("Found unmodified AbstractContainer with no source - '%s' with parent '%s'" %
                                 (value.name, parent_container.name))
        elif isinstance(value, list):
            for container in value:
                self.__add_containers(builder, spec, container, build_manager, source, parent_container, export)
        else:  # pragma: no cover
            msg = ("Received %s, expected AbstractContainer or a list of AbstractContainers."
                   % value.__class__.__name__)
            raise ValueError(msg)

    def __get_subspec_values(self, builder, spec, manager):
        ret = dict()
        # First get attributes
        attributes = builder.attributes
        for attr_spec in spec.attributes:
            attr_val = attributes.get(attr_spec.name)
            if attr_val is None:
                continue
            if isinstance(attr_val, (GroupBuilder, DatasetBuilder)):
                ret[attr_spec] = manager.construct(attr_val)
            elif isinstance(attr_val, RegionBuilder):  # pragma: no cover
                raise ValueError("RegionReferences as attributes is not yet supported")
            elif isinstance(attr_val, ReferenceBuilder):
                ret[attr_spec] = manager.construct(attr_val.builder)
            else:
                ret[attr_spec] = attr_val
        if isinstance(spec, GroupSpec):
            if not isinstance(builder, GroupBuilder):  # pragma: no cover
                raise ValueError("__get_subspec_values - must pass GroupBuilder with GroupSpec")
            # first aggregate links by data type and separate them
            # by group and dataset
            groups = dict(builder.groups)  # make a copy so we can separate links
            datasets = dict(builder.datasets)  # make a copy so we can separate links
            links = builder.links
            link_dt = dict()
            for link_builder in links.values():
                target = link_builder.builder
                if isinstance(target, DatasetBuilder):
                    datasets[link_builder.name] = target
                else:
                    groups[link_builder.name] = target
                dt = manager.get_builder_dt(target)
                if dt is not None:
                    link_dt.setdefault(dt, list()).append(target)
            # now assign links to their respective specification
            for subspec in spec.links:
                if subspec.name is not None and subspec.name in links:
                    ret[subspec] = manager.construct(links[subspec.name].builder)
                else:
                    sub_builder = link_dt.get(subspec.target_type)
                    if sub_builder is not None:
                        ret[subspec] = self.__flatten(sub_builder, subspec, manager)
            # now process groups and datasets
            self.__get_sub_builders(groups, spec.groups, manager, ret)
            self.__get_sub_builders(datasets, spec.datasets, manager, ret)
        elif isinstance(spec, DatasetSpec):
            if not isinstance(builder, DatasetBuilder):  # pragma: no cover
                raise ValueError("__get_subspec_values - must pass DatasetBuilder with DatasetSpec")
            if (spec.shape is None and getattr(builder.data, 'shape', None) == (1,) and
                    type(builder.data[0]) != np.void):
                # if a scalar dataset is expected and a 1-element non-compound dataset is given, then read the dataset
                builder['data'] = builder.data[0]  # use dictionary reference instead of .data to bypass error
            ret[spec] = self.__check_ref_resolver(builder.data)
        return ret

    @staticmethod
    def __check_ref_resolver(data):
        """
        Check if this dataset is a reference resolver, and invert it if so.
        """
        if isinstance(data, ReferenceResolver):
            return data.invert()
        return data

    def __get_sub_builders(self, sub_builders, subspecs, manager, ret):
        # index builders by data_type
        builder_dt = dict()
        for g in sub_builders.values():
            dt = manager.get_builder_dt(g)
            ns = manager.get_builder_ns(g)
            if dt is None or ns is None:
                continue
            for parent_dt in manager.namespace_catalog.get_hierarchy(ns, dt):
                builder_dt.setdefault(parent_dt, list()).append(g)
        for subspec in subspecs:
            # first get data type for the spec
            if subspec.data_type_def is not None:
                dt = subspec.data_type_def
            elif subspec.data_type_inc is not None:
                dt = subspec.data_type_inc
            else:
                dt = None
            # use name if we can, otherwise use data_data
            if subspec.name is None:
                sub_builder = builder_dt.get(dt)
                if sub_builder is not None:
                    sub_builder = self.__flatten(sub_builder, subspec, manager)
                    ret[subspec] = sub_builder
            else:
                sub_builder = sub_builders.get(subspec.name)
                if sub_builder is None:
                    continue
                if dt is None:
                    # recurse
                    ret.update(self.__get_subspec_values(sub_builder, subspec, manager))
                else:
                    ret[subspec] = manager.construct(sub_builder)

    def __flatten(self, sub_builder, subspec, manager):
        tmp = [manager.construct(b) for b in sub_builder]
        if len(tmp) == 1 and not subspec.is_many():
            tmp = tmp[0]
        return tmp

    @docval({'name': 'builder', 'type': (DatasetBuilder, GroupBuilder),
             'doc': 'the builder to construct the AbstractContainer from'},
            {'name': 'manager', 'type': BuildManager, 'doc': 'the BuildManager for this build'},
            {'name': 'parent', 'type': (Proxy, AbstractContainer),
             'doc': 'the parent AbstractContainer/Proxy for the AbstractContainer being built', 'default': None})
    def construct(self, **kwargs):
        ''' Construct an AbstractContainer from the given Builder '''
        builder, manager, parent = getargs('builder', 'manager', 'parent', kwargs)
        cls = manager.get_cls(builder)
        # gather all subspecs
        subspecs = self.__get_subspec_values(builder, self.spec, manager)
        # get the constructor argument that each specification corresponds to
        const_args = dict()
        # For Data container classes, we need to populate the data constructor argument since
        # there is no sub-specification that maps to that argument under the default logic
        if issubclass(cls, Data):
            if not isinstance(builder, DatasetBuilder):  # pragma: no cover
                raise ValueError('Can only construct a Data object from a DatasetBuilder - got %s' % type(builder))
            const_args['data'] = self.__check_ref_resolver(builder.data)
        for subspec, value in subspecs.items():
            const_arg = self.get_const_arg(subspec)
            if const_arg is not None:
                if isinstance(subspec, BaseStorageSpec) and subspec.is_many():
                    existing_value = const_args.get(const_arg)
                    if isinstance(existing_value, list):
                        value = existing_value + value
                const_args[const_arg] = value
        # build kwargs for the constructor
        kwargs = dict()
        for const_arg in get_docval(cls.__init__):
            argname = const_arg['name']
            override = self.__get_override_carg(argname, builder, manager)
            if override is not None:
                val = override
            elif argname in const_args:
                val = const_args[argname]
            else:
                continue
            kwargs[argname] = val
        try:
            obj = self.__new_container__(cls, builder.source, parent, builder.attributes.get(self.__spec.id_key()),
                                         **kwargs)
        except Exception as ex:
            msg = 'Could not construct %s object due to: %s' % (cls.__name__, ex)
            raise ConstructError(builder, msg) from ex
        return obj

    def __new_container__(self, cls, container_source, parent, object_id, **kwargs):
        """A wrapper function for ensuring a container gets everything set appropriately"""
        obj = cls.__new__(cls, container_source=container_source, parent=parent, object_id=object_id)
        obj.__init__(**kwargs)
        return obj

    @docval({'name': 'container', 'type': AbstractContainer,
             'doc': 'the AbstractContainer to get the Builder name for'})
    def get_builder_name(self, **kwargs):
        '''Get the name of a Builder that represents a AbstractContainer'''
        container = getargs('container', kwargs)
        if self.__spec.name not in (NAME_WILDCARD, None):
            ret = self.__spec.name
        else:
            ret = container.name
        return ret
