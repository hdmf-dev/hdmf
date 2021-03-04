from copy import deepcopy
from datetime import datetime

import numpy as np

from ..container import Container, Data, DataRegion, MultiContainerInterface
from ..spec import AttributeSpec, LinkSpec, RefSpec
from ..spec.spec import BaseStorageSpec, ZERO_OR_MANY, ONE_OR_MANY
from ..utils import docval, getargs, ExtenderMeta, get_docval, fmt_docval_args


class ClassGenerator:

    def __init__(self):
        self.__custom_generators = []

    @property
    def custom_generators(self):
        return self.__custom_generators

    @docval({"name": "generator", "type": type, "doc": "the CustomClassGenerator class to register"})
    def register_generator(self, **kwargs):
        """Add a custom class generator to this ClassGenerator."""
        generator = getargs('generator', kwargs)
        if not issubclass(generator, CustomClassGenerator):
            raise ValueError('Generator %s must be a subclass of CustomClassGenerator.' % generator.__name__)
        self.__custom_generators.append(generator)

    @docval({"name": "data_type", "type": str, "doc": "the data type to create a AbstractContainer class for"},
            {"name": "spec", "type": BaseStorageSpec, "doc": ""},
            {"name": "parent_cls", "type": type, "doc": ""},
            {"name": "attr_names", "type": dict, "doc": ""},
            {"name": "type_map", "type": 'TypeMap', "doc": ""},
            returns='the class for the given namespace and data_type', rtype=type)
    def generate_class(self, **kwargs):
        '''Get the container class from data type specification.
        If no class has been associated with the ``data_type`` from ``namespace``, a class will be dynamically
        created and returned.
        '''
        data_type, spec, parent_cls, attr_names, type_map = getargs('data_type', 'spec', 'parent_cls', 'attr_names',
                                                                    'type_map', kwargs)

        not_inherited_fields = dict()
        for k, field_spec in attr_names.items():
            if k == 'help':  # pragma: no cover
                # (legacy) do not add field named 'help' to any part of class object
                continue
            if not spec.is_inherited_spec(field_spec):
                not_inherited_fields[k] = field_spec
        try:
            classdict = dict()
            bases = [parent_cls]
            for class_generator in self.__custom_generators:
                # each generator can update classdict and bases
                class_generator.update_cls_args(classdict, bases, not_inherited_fields, spec, type_map)
        except TypeDoesNotExistError as e:  # pragma: no cover
            # this error should never happen after hdmf#322
            name = spec.data_type_def
            if name is None:
                name = 'Unknown'
            raise ValueError("Cannot dynamically generate class for type '%s'. " % name
                             + str(e)
                             + " Please define that type before defining '%s'." % name)
        cls = ExtenderMeta(data_type, tuple(bases), classdict)
        return cls


class TypeDoesNotExistError(Exception):  # pragma: no cover
    pass


class CustomClassGenerator():
    """Subclass this class and register an instance to alter how classes are auto-generated."""

    def __new__(cls, *args, **kwargs):  # pragma: no cover
        raise TypeError('Cannot instantiate class %s' % cls.__name__)

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

    @classmethod
    def _get_container_type(cls, container_name, type_map):
        """Search all namespaces for the container associated with the given data type.

        :raises TypeDoesNotExistError: if type is not found in any namespace"""
        container_type = None
        for val in type_map.container_types.values():
            container_type = val.get(container_name)
            if container_type is not None:
                return container_type
        if container_type is None:  # pragma: no cover
            # this should never happen after hdmf#322
            raise TypeDoesNotExistError("Type '%s' does not exist." % container_name)

    @classmethod
    def _get_scalar_type_map(cls, spec_dtype):
        dtype = cls._spec_dtype_map.get(spec_dtype)
        if dtype is None:  # pragma: no cover
            # this should not happen as long as _spec_dtype_map is kept up to date with
            # hdmf.spec.spec.DtypeHelper.valid_primary_dtypes
            raise ValueError("Spec dtype '%s' cannot be mapped to a Python type." % spec_dtype)
        return dtype

    @classmethod
    def _get_type(cls, spec, type_map):
        """Get the type of a spec for use in docval."""
        if isinstance(spec, AttributeSpec):
            if isinstance(spec.dtype, RefSpec):
                try:
                    container_type = cls._get_container_type(spec.dtype.target_type, type_map)
                    return container_type
                except TypeDoesNotExistError:
                    # TODO what happens when the attribute ref target is not (or not yet) mapped to a container class?
                    # returning Data, Container works as a generic fallback for now but should be more specific
                    return Data, Container
            elif spec.shape is None and spec.dims is None:
                return cls._get_scalar_type_map(spec.dtype)
            else:
                return 'array_data', 'data'
        if isinstance(spec, LinkSpec):
            return cls._get_container_type(spec.target_type, type_map)
        if spec.data_type is not None:
            return cls._get_container_type(spec.data_type, type_map)
        if spec.shape is None and spec.dims is None:
            return cls._get_scalar_type_map(spec.dtype)
        return 'array_data', 'data'

    @classmethod
    def _ischild(cls, dtype):
        """Check if dtype represents a type that is a child."""
        ret = False
        if isinstance(dtype, tuple):
            for sub in dtype:
                ret = ret or cls._ischild(sub)
        else:
            if isinstance(dtype, type) and issubclass(dtype, (Container, Data, DataRegion)):
                ret = True
        return ret

    @staticmethod
    def _set_default_name(docval_args, default_name):
        """Set the default value for the name docval argument."""
        if default_name is not None:
            for x in docval_args:
                if x['name'] == 'name':
                    x['default'] = default_name

    @classmethod
    def _build_docval(cls, base, not_inherited_fields, type_map, name, default_name):
        """Build docval for __init__ of auto-generated class
        :param base: The base class of the new class
        :param not_inherited_fields: Dict of additional fields that are not in the base class
        :param type_map: TypeMap to use when determining type for a spec
        :param name: Fixed name of instances of this class, or None if name is not fixed to a particular value
        :param default_name: Default name of instances of this class, or None if not specified
        :return: List of docval arguments
        """
        docval_args = list(deepcopy(get_docval(base.__init__)))
        for f, field_spec in not_inherited_fields.items():
            docval_arg = dict(name=f, doc=field_spec.doc)
            if getattr(field_spec, 'quantity', None) in (ZERO_OR_MANY, ONE_OR_MANY):
                docval_arg.update(type=(list, tuple, dict, cls._get_type(field_spec, type_map)))
            else:
                dtype = cls._get_type(field_spec, type_map)
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
        cls._set_default_name(docval_args, default_name)

        return docval_args

    @classmethod
    def _build_fields(cls, base, not_inherited_fields, type_map):
        """Create fields spec of new class.
        :param base: The base class of the new class
        :param not_inherited_fields: Dict of additional fields that are not in the base class
        :param type_map: TypeMap to use when determining type for a spec
        """

        # add new fields to docval and class fields
        fields = list()
        for f, field_spec in not_inherited_fields.items():
            dtype = cls._get_type(field_spec, type_map)
            fields_conf = {'name': f,
                           'doc': field_spec['doc']}
            if cls._ischild(dtype) and issubclass(base, Container) and not isinstance(field_spec, LinkSpec):
                fields_conf['child'] = True
            # if getattr(field_spec, 'value', None) is not None:  # TODO set the fixed value on the class?
            #     fields_conf['settable'] = False
            fields.append(fields_conf)

        classdict = dict()
        if len(fields):
            classdict.update({base._fieldsname: tuple(fields)})
        return classdict

    @classmethod
    def _build_init(cls, base, not_inherited_fields, type_map, name, default_name):
        """
        Get __init__ and fields of new class.
        :param base: The base class of the new class
        :param not_inherited_fields: Dict of additional fields that are not in the base class
        :param type_map: TypeMap to use when determining type for a spec
        :param name: Fixed name of instances of this class, or None if name is not fixed to a particular value
        :param default_name: Default name of instances of this class, or None if not specified
        """
        # copy docval args from superclass
        existing_args = set(arg['name'] for arg in get_docval(base.__init__))

        # add new fields to docval and class fields
        new_args = list()
        for f, field_spec in not_inherited_fields.items():
            # auto-initialize arguments not found in superclass
            if f not in existing_args:
                new_args.append(f)

        classdict = dict()

        if len(not_inherited_fields) or name is not None:  # TODO why
            docval_args = cls._build_docval(base, not_inherited_fields, type_map, name, default_name)

            @docval(*docval_args)
            def __init__(self, **kwargs):
                if name is not None:
                    kwargs.update(name=name)
                pargs, pkwargs = fmt_docval_args(base.__init__, kwargs)
                base.__init__(self, *pargs, **pkwargs)  # special case: need to pass self to __init__

                for f in new_args:
                    arg_val = kwargs.get(f, None)
                    setattr(self, f, arg_val)

            classdict.update(__init__=__init__)

        return classdict

    @classmethod
    def update_cls_args(cls, classdict, bases, not_inherited_fields, spec, type_map):
        """Add __fields__ and __init__ to the classdict
        :param classdict: The dict to update with __clsconf__ if applicable
        :param bases: The list of base classes to update if applicable
        :param not_inherited_fields: Dict of additional fields that are not in the base class
        :param spec: The spec for the container class to generate
        :param type_map: TypeMap to use when determining type for a spec
        """
        parent_cls = bases[0]
        d = cls._build_fields(parent_cls, not_inherited_fields, type_map)
        classdict.update(d)
        d = cls._build_init(parent_cls, not_inherited_fields, type_map, spec.name, spec.default_name)
        classdict.update(d)
        # TODO add test that fields is built correctly


class MCIClassGenerator(CustomClassGenerator):

    @classmethod
    def update_cls_args(cls, classdict, bases, not_inherited_fields, spec, type_map):
        """Update the given class dict and base classes if there is a field spec with quantity * or +
        :param classdict: The dict to update with __clsconf__ if applicable
        :param bases: The list of base classes to update if applicable
        :param not_inherited_fields: Dict of additional fields that are not in the base class
        :param spec: The spec for the container class to generate
        :param type_map: The type map to use
        """
        # NOTE: spec is not used here

        # create MultiContainerInterface __clsconf__
        clsconf = list()
        for f, field_spec in not_inherited_fields.items():
            if getattr(field_spec, 'quantity', None) in (ZERO_OR_MANY, ONE_OR_MANY):
                clsconf.append(dict(
                    attr=f,
                    type=cls._get_type(field_spec, type_map),  # <-- from ClassGenerator
                    add='add_{}'.format(f),
                    get='get_{}'.format(f),
                    create='create_{}'.format(f)
                ))
                # remove the attribute from __fields__ -- needed so that MCI initializes the attribute to LabelledDict
                classdict['__fields__'] = tuple(filter(lambda x: x['name'] != f, classdict['__fields__']))

        if len(clsconf):
            classdict.update(__clsconf__=clsconf)

            # add MultiContainerInterface to the type hierarchy if not already there
            if not isinstance(bases[0], MultiContainerInterface):
                bases.insert(0, MultiContainerInterface)
