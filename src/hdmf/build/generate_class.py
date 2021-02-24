from copy import copy, deepcopy
from datetime import datetime

import numpy as np

from ..container import Container, Data, DataRegion, MultiContainerInterface
from ..spec import AttributeSpec, DatasetSpec, GroupSpec, LinkSpec, RefSpec
from ..spec.spec import BaseStorageSpec, ZERO_OR_MANY, ONE_OR_MANY
from ..utils import docval, getargs, ExtenderMeta, get_docval, fmt_docval_args


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


class ClassGenerator(metaclass=ExtenderMeta):

    def __init__(self, container_types):
        self.__container_types = container_types
        self.__generator_cls = dict()  # the ObjectMapper class to use for each container type

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

    @ExtenderMeta.post_init
    def __gather_procedures(cls, name, bases, classdict):
        if hasattr(cls, 'constructor_args'):
            cls.constructor_args = copy(cls.constructor_args)
        else:
            cls.constructor_args = dict()
        for name, func in cls.__dict__.items():
            if cls.__is_constructor_arg(func):
                cls.constructor_args[cls.__get_cargname(func)] = getattr(cls, name)

    @_constructor_arg('name')
    def get_container_name(self, *args):
        builder = args[0]
        return builder.name

    def __get_override_carg(self, *args):
        name = args[0]
        remaining_args = tuple(args[1:])
        if name in self.constructor_args:
            self.logger.debug("        Calling override function for constructor argument '%s'" % name)
            func = self.constructor_args[name]
            return func(self, *remaining_args)
        return None

    @docval({"name": "container_cls", "type": type,
             "doc": "the AbstractContainer class for which the given ObjectMapper class gets used for"},
            {"name": "mapper_cls", "type": type, "doc": "the ObjectMapper class to use to map"})
    def register_generator(self, **kwargs):
        ''' Map a container class to an ObjectMapper class '''
        container_cls, mapper_cls = getargs('container_cls', 'mapper_cls', kwargs)
        if self.get_container_cls_dt(container_cls) == (None, None):
            raise ValueError('cannot register map for type %s - no data_type found' % container_cls)
        self.__generator_cls[container_cls] = mapper_cls

    # mapping from spec types to allowable python types for docval for fields during dynamic class generation
    # e.g., if a dataset/attribute spec has dtype int32, then get_class should generate a docval for the class'
    # __init__ method that allows the types (int, np.int32, np.int64) for the corresponding field.
    # passing an np.int16 would raise a docval error.
    # passing an int64 to __init__ would result in the field storing the value as an int64 (and subsequently written
    # as an int64). no upconversion or downconversion happens as a result of this map
    _spec_dtype_map = {
        'float32': (float, np.float32, np.float64),
        'float': (float, np.float32, np.float64),
        'float64': (float, np.float64),
        'double': (float, np.float64),
        'int8': (np.int8, np.int16, np.int32, np.int64, int),
        'int16': (np.int16, np.int32, np.int64, int),
        'short': (np.int16, np.int32, np.int64, int),
        'int32': (int, np.int32, np.int64),
        'int': (int, np.int32, np.int64),
        'int64': np.int64,
        'long': np.int64,
        'uint8': (np.uint8, np.uint16, np.uint32, np.uint64),
        'uint16': (np.uint16, np.uint32, np.uint64),
        'uint32': (np.uint32, np.uint64),
        'uint64': np.uint64,
        'numeric': (float, np.float32, np.float64, np.int8, np.int16, np.int32, np.int64, int, np.uint8, np.uint16,
                    np.uint32, np.uint64),
        'text': str,
        'utf': str,
        'utf8': str,
        'utf-8': str,
        'ascii': bytes,
        'bytes': bytes,
        'bool': (bool, np.bool_),
        'isodatetime': datetime,
        'datetime': datetime
    }

    def __get_container_type(self, container_name):
        container_type = None
        for val in self.__container_types.values():
            container_type = val.get(container_name)
            if container_type is not None:
                return container_type
        if container_type is None:  # pragma: no cover
            # this code should never happen after hdmf#322
            raise TypeDoesNotExistError("Type '%s' does not exist." % container_name)

    def __get_scalar_type_map(self, spec_dtype):
        dtype = self._spec_dtype_map.get(spec_dtype)
        if dtype is None:  # pragma: no cover
            # this should not happen as long as _spec_dtype_map is kept up to date with
            # hdmf.spec.spec.DtypeHelper.valid_primary_dtypes
            raise ValueError("Spec dtype '%s' cannot be mapped to a Python type." % spec_dtype)
        return dtype

    def __get_type(self, spec):
        if isinstance(spec, AttributeSpec):
            if isinstance(spec.dtype, RefSpec):
                tgttype = spec.dtype.target_type
                for val in self.__container_types.values():
                    container_type = val.get(tgttype)
                    if container_type is not None:
                        return container_type
                return Data, Container
            elif spec.shape is None and spec.dims is None:
                return self.__get_scalar_type_map(spec.dtype)
            else:
                return 'array_data', 'data'
        if isinstance(spec, LinkSpec):
            return self.__get_container_type(spec.target_type)
        if spec.data_type_def is not None:
            return self.__get_container_type(spec.data_type_def)
        if spec.data_type_inc is not None:
            return self.__get_container_type(spec.data_type_inc)
        if spec.shape is None and spec.dims is None:
            return self.__get_scalar_type_map(spec.dtype)
        return 'array_data', 'data'

    def __ischild(self, dtype):
        """
        Check if dtype represents a type that is a child
        """
        ret = False
        if isinstance(dtype, tuple):
            for sub in dtype:
                ret = ret or self.__ischild(sub)
        else:
            if isinstance(dtype, type) and issubclass(dtype, (Container, Data, DataRegion)):
                ret = True
        return ret

    @staticmethod
    def __set_default_name(docval_args, default_name):
        if default_name is not None:
            for x in docval_args:
                if x['name'] == 'name':
                    x['default'] = default_name

    def _build_docval(self, base, addl_fields, name=None, default_name=None):
        """Build docval for auto-generated class

        :param base: The base class of the new class
        :param addl_fields: Dict of additional fields that are not in the base class
        :param name: Fixed name of instances of this class, or None if name is not fixed to a particular value
        :param default_name: Default name of instances of this class, or None if not specified
        :return:
        """
        docval_args = list(deepcopy(get_docval(base.__init__)))
        for f, field_spec in addl_fields.items():
            docval_arg = dict(name=f, doc=field_spec.doc)
            if getattr(field_spec, 'quantity', None) in (ZERO_OR_MANY, ONE_OR_MANY):
                docval_arg.update(type=(list, tuple, dict, self.__get_type(field_spec)))
            else:
                dtype = self.__get_type(field_spec)
                docval_arg.update(type=dtype)
                if getattr(field_spec, 'shape', None) is not None:
                    docval_arg.update(shape=field_spec.shape)
            if not field_spec.required:
                docval_arg['default'] = getattr(field_spec, 'default_value', None)

            # if argument already exists, overwrite it. If not, append it to list.
            inserted = False
            for i, x in enumerate(docval_args):
                if x['name'] == f:
                    docval_args[i] = docval_arg
                    inserted = True
            if not inserted:
                docval_args.append(docval_arg)

        # if spec provides a fixed name for this type, remove the 'name' arg from docval_args so that values cannot
        # be passed for a name positional or keyword arg
        if name is not None:  # fixed name is specified in spec, remove it from docval args
            docval_args = filter(lambda x: x['name'] != 'name', docval_args)

        # set default name if provided
        self.__set_default_name(docval_args, default_name)

        return docval_args

    def get_cls_dict(self, base, addl_fields, name=None, default_name=None):
        """
        Get __init__ and fields of new class.
        :param base: The base class of the new class
        :param addl_fields: Dict of additional fields that are not in the base class
        :param name: Fixed name of instances of this class, or None if name is not fixed to a particular value
        :param default_name: Default name of instances of this class, or None if not specified
        """
        # TODO: fix this to be more maintainable and smarter
        if base is None:
            raise ValueError('cannot generate class without base class')
        new_args = list()
        fields = list()

        # copy docval args from superclass
        existing_args = set(arg['name'] for arg in get_docval(base.__init__))

        # add new fields to docval and class fields
        for f, field_spec in addl_fields.items():
            if f == 'help':  # pragma: no cover
                # (legacy) do not add field named 'help' to any part of class object
                continue

            dtype = self.__get_type(field_spec)
            fields_conf = {'name': f,
                           'doc': field_spec['doc']}
            if self.__ischild(dtype) and issubclass(base, Container):
                fields_conf['child'] = True
            # if getattr(field_spec, 'value', None) is not None:  # TODO set the fixed value on the class?
            #     fields_conf['settable'] = False
            fields.append(fields_conf)

            # auto-initialize arguments not found in superclass
            if f not in existing_args:
                new_args.append(f)

        classdict = dict()

        if len(fields):
            classdict.update({base._fieldsname: tuple(fields)})

        docval_args = self._build_docval(base, addl_fields, name, default_name)

        if len(fields) or name is not None:
            @docval(*docval_args)
            def __init__(self, **kwargs):
                if name is not None:
                    kwargs.update(name=name)
                pargs, pkwargs = fmt_docval_args(base.__init__, kwargs)
                base.__init__(self, *pargs, **pkwargs)  # special case: need to pass self to __init__
                if len(clsconf):
                    MultiContainerInterface.__init__(self, *pargs, **pkwargs)

                for f in new_args:
                    arg_val = kwargs.get(f, None)
                    setattr(self, f, arg_val)

            classdict.update(__init__=__init__)

        return classdict

    def get_cls_bases(self, parent_cls):
        return (parent_cls, )

    @docval({"name": "namespace", "type": str, "doc": "the namespace containing the data_type"},
            {"name": "data_type", "type": str, "doc": "the data type to create a AbstractContainer class for"},
            {"name": "spec", "type": BaseStorageSpec, "doc": ""},
            {"name": "parent_cls", "type": type, "doc": ""},
            {"name": "attr_names", "type": dict, "doc": ""},
            returns='the class for the given namespace and data_type', rtype=type)
    def generate_class(self, **kwargs):
        '''Get the container class from data type specification.
        If no class has been associated with the ``data_type`` from ``namespace``, a class will be dynamically
        created and returned.
        '''
        namespace, data_type, spec, parent_cls, attr_names = getargs('namespace', 'data_type', 'spec', 'parent_cls',
                                                                     'attr_names', kwargs)

        fields = dict()
        for k, field_spec in attr_names.items():
            if not spec.is_inherited_spec(field_spec):
                fields[k] = field_spec
        try:
            d = self.get_cls_dict(parent_cls, fields, spec.name, spec.default_name)
            bases = self.get_cls_bases(parent_cls)
        except TypeDoesNotExistError as e:  # pragma: no cover
            # this error should never happen after hdmf#322
            name = spec.data_type_def
            if name is None:
                name = 'Unknown'
            raise ValueError("Cannot dynamically generate class for type '%s'. " % name
                             + str(e)
                             + " Please define that type before defining '%s'." % name)
        cls = ExtenderMeta(str(data_type), bases, d)
        return cls


class TypeDoesNotExistError(Exception):  # pragma: no cover
    pass


class MCIClassGenerator(ClassGenerator):

    def update_cls_dict(self, base, addl_fields):
        """Get __init__ and fields of new class.
        :param base: The base class of the new class
        :param addl_fields: Dict of additional fields that are not in the base class
        """
        new_args = list()
        clsconf = list()
        classdict = dict()
        # add new fields to docval and class fields
        for f, field_spec in addl_fields.items():
            if getattr(field_spec, 'quantity', None) in (ZERO_OR_MANY, ONE_OR_MANY):
                # if its a MultiContainerInterface, also build clsconf
                clsconf.append(dict(
                    attr=f,
                    type=self.__get_type(field_spec),
                    add='add_{}'.format(f),
                    get='get_{}'.format(f),
                    create='create_{}'.format(f)
                ))

        if len(clsconf):
            classdict.update(__clsconf__=clsconf)

        if len(fields) or name is not None:
            @docval(*docval_args)
            def __init__(self, **kwargs):
                if name is not None:
                    kwargs.update(name=name)
                pargs, pkwargs = fmt_docval_args(base.__init__, kwargs)
                base.__init__(self, *pargs, **pkwargs)  # special case: need to pass self to __init__
                if len(clsconf):
                    MultiContainerInterface.__init__(self, *pargs, **pkwargs)

                for f in new_args:
                    arg_val = kwargs.get(f, None)
                    setattr(self, f, arg_val)

            classdict.update(__init__=__init__)

        return classdict

    def get_cls_bases(self, parent_cls, bases):
        if not isinstance(parent_cls, MultiContainerInterface):
            bases = tuple([MultiContainerInterface] + list(bases))
